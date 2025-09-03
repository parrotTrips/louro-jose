# select_important_emails_llm.py
from __future__ import annotations

import os
import sys
import glob
import json
import re
import time
from typing import Dict, List, Tuple
from datetime import datetime

from dotenv import load_dotenv
import google.generativeai as genai

# permitir imports do pacote "modules" e carregar variáveis do .env da raiz
sys.path.append("..")
load_dotenv("../.env")

from modules.io_email import load_email_json  # noqa: E402

RAW_DIR = "raw_messages"
IMPORTANT_DIR = "important_emails"

# ---------------- util ----------------

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def sanitize_fname(s: str) -> str:
    s = (s or "").strip().replace(" ", "_")
    s = re.sub(r"[^a-zA-Z0-9_\-\.@]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s[:160].strip("_")

def trim(s: str, maxlen: int = 4000) -> str:
    s = s or ""
    return s if len(s) <= maxlen else s[:maxlen]

# ---------------- LLM helpers ----------------

def get_response_text(resp) -> str:
    text = (getattr(resp, "text", None) or "").strip()
    if text:
        return text
    try:
        for cand in getattr(resp, "candidates", []) or []:
            content = getattr(cand, "content", None)
            if content and getattr(content, "parts", None):
                for part in content.parts:
                    ptxt = getattr(part, "text", None)
                    if ptxt:
                        ptxt = ptxt.strip()
                        if ptxt:
                            return ptxt
    except Exception:
        pass
    return ""

def force_json_list(text: str) -> List[Dict]:
    # aceita lista/obj único; falha se não for JSON válido
    block = text.strip()
    # tenta detectar bloco markdown ```json ... ```
    m = re.search(r"```json\s*([\s\S]*?)\s*```", block, flags=re.IGNORECASE)
    if m:
        block = m.group(1).strip()
    data = json.loads(block)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    raise ValueError("Resposta não é um JSON de objeto/lista.")

# ---------------- Prompt ----------------

SYSTEM_INSTR = (
    "Você classifica e-mails dentro de uma thread para identificar quais CONTÊM "
    "INFORMAÇÕES ÚTEIS DE COTAÇÃO (preços/diárias, impostos/ISS, categorias/quartos, "
    "confirmação de disponibilidade com datas, políticas/regras de pagamento e cancelamento, "
    "condições de reserva). Não marque pedidos de cotação, agradecimentos, ou mensagens sem dados concretos."
)

def build_selector_prompt(thread_id: str, emails: List[Dict]) -> str:
    """
    Retorne SOMENTE JSON, no formato:
    [
      {"email_index": 2, "is_useful": true, "reasons": "tem preços e ISS", "confidence": 0.95},
      ...
    ]

    Critérios (resumo):
      - is_useful = true quando houver VALOR monetário explícito, ou regras/políticas concretas,
        ou confirmação clara de disponibilidade com datas/categorias/tarifas.
      - Não marque como útil mensagens que apenas pedem cotação, prometem retorno, ou encaminham sem números/regras.
      - Se mais de um e-mail for útil, inclua todos (cada um com seu email_index).
      - Se nenhum for útil, retorne [].
    """
    lines = []
    lines.append(f"THREAD_ID: {thread_id or '(sem id)'}")
    lines.append("TAREFA: Para cada e-mail enumerado, decida se ele contém informação útil de COTAÇÃO.")
    lines.append("RETORNE SOMENTE JSON (sem markdown).")
    lines.append("")
    for i, e in enumerate(emails):
        lines.append(f"--- EMAIL #{i} ---")
        lines.append(f"timestamp: {e.get('timestamp')}")
        lines.append(f"sender: {e.get('sender')}")
        lines.append(f"subject: {e.get('subject')}")
        body = trim(e.get("body") or "", 3500)
        lines.append("body:")
        lines.append(body)
        lines.append("")
    return "\n".join(lines)

# ---------------- LLM call (com retry 429, ainda é LLM) ----------------

def call_gemini_with_retry(model: genai.GenerativeModel, prompt: str, attempts: int = 3):
    last_err = None
    for a in range(attempts):
        try:
            return model.generate_content(prompt)
        except Exception as e:
            msg = str(e)
            # trata 429 (quota/limite) com backoff simples; continua sendo LLM
            if "429" in msg:
                m = re.search(r"retry_delay\s*{\s*seconds:\s*(\d+)", msg)
                delay = int(m.group(1)) if m else (5 * (a + 1))
                time.sleep(delay)
                last_err = e
                continue
            raise
    # se ainda falhar, propaga o último erro
    raise last_err if last_err else RuntimeError("LLM error")

def select_useful_indices_with_llm(thread_id: str, emails: List[Dict]) -> Tuple[List[int], str]:
    model_name = os.getenv("GEMINI_MODEL_NAME", "gemini-1.5-flash").strip()
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("⛔ GEMINI_API_KEY não definido no .env")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        model_name,
        system_instruction=SYSTEM_INSTR,
        generation_config={"temperature": 0, "response_mime_type": "application/json"},
    )

    prompt = build_selector_prompt(thread_id, emails)
    resp = call_gemini_with_retry(model, prompt)
    raw = get_response_text(resp)

    items = force_json_list(raw)
    useful = []
    for it in items:
        if isinstance(it, dict) and it.get("is_useful") is True:
            idx = it.get("email_index")
            if isinstance(idx, int) and 0 <= idx < len(emails):
                useful.append(idx)
    useful = sorted(set(useful))
    return useful, raw

# ---------------- salvar ----------------

def save_important_email(thread_id: str, idx: int, e: Dict, src_path: str, llm_raw: str) -> str:
    ensure_dir(IMPORTANT_DIR)
    ts = (e.get("timestamp") or "na")
    subject = sanitize_fname(e.get("subject") or "")
    fname = f"{sanitize_fname(thread_id or 'single')}__{sanitize_fname(ts)}__{idx:02d}__{subject}.json"
    outp = os.path.join(IMPORTANT_DIR, fname)
    payload = {
        "_source_raw": src_path,
        "_thread_id": thread_id,
        "_email_index": idx,
        "_llm_decision": {"raw": llm_raw},
        "timestamp": e.get("timestamp"),
        "sender": e.get("sender"),
        "recipient": e.get("recipient"),
        "subject": e.get("subject"),
        "body": e.get("body"),
    }
    with open(outp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return outp

# ---------------- main ----------------

def main():
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.json")))
    if not files:
        print(f"⛔ Nenhum arquivo .json encontrado em {RAW_DIR}/")
        return

    print(f"🧠 Seleção via LLM em {len(files)} arquivo(s) de {RAW_DIR}/")
    saved = 0

    for path in files:
        meta = load_email_json(path)

        # normaliza para lista de e-mails
        if isinstance(meta, dict) and isinstance(meta.get("emails"), list):
            emails = sorted(meta["emails"], key=lambda x: x.get("timestamp") or "")
            thread_id = meta.get("thread_id") or ""
        else:
            emails = [{
                "timestamp": meta.get("timestamp"),
                "sender": meta.get("from") or meta.get("sender"),
                "recipient": meta.get("to") or meta.get("recipient"),
                "subject": meta.get("subject"),
                "body": meta.get("body"),
            }]
            thread_id = meta.get("thread_id") or ""

        try:
            useful_idx, raw_decision = select_useful_indices_with_llm(thread_id, emails)
        except Exception as e:
            print(f"⚠️ LLM falhou em {os.path.basename(path)}: {e}")
            useful_idx, raw_decision = [], ""

        if not useful_idx:
            print(f"➖ Nenhum e-mail útil (LLM) em {os.path.basename(path)}")
            continue

        for idx in useful_idx:
            outp = save_important_email(thread_id, idx, emails[idx], path, raw_decision)
            saved += 1
            print(f"✅ IMPORTANTE (LLM) → {os.path.basename(outp)}")

    print(f"📦 Salvos {saved} e-mail(s) úteis em {IMPORTANT_DIR}/")

if __name__ == "__main__":
    main()
