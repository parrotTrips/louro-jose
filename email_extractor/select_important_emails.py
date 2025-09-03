from __future__ import annotations

import os
import sys
import glob
import json
import re
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

# -------------- utils I/O --------------

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

# -------------- currency regex p/ fallback --------------

CURRENCY_RE = re.compile(
    r"(?:R\$\s?|\bBRL\b|\$|\bUSD\b)\s*\d{1,3}(?:[\.\,]\d{3})*(?:[\.\,]\d{2})?",
    re.IGNORECASE
)

# -------------- LLM helpers --------------

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

def extract_json_block(text: str) -> str:
    t = (text or "").strip()
    m = re.search(r"```json\s*([\s\S]*?)\s*```", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    a0, a1 = t.find("["), t.rfind("]")
    if a0 != -1 and a1 != -1 and a1 > a0:
        return t[a0:a1+1].strip()
    o0, o1 = t.find("{"), t.rfind("}")
    if o0 != -1 and o1 != -1 and o1 > o0:
        return t[o0:o1+1].strip()
    return t

def force_json_list(text: str) -> List[Dict]:
    block = extract_json_block(text)
    data = json.loads(block)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    return []

# -------------- Prompting --------------

SYSTEM_INSTR = (
    "Você é um assistente que classifica e-mails dentro de uma mesma thread "
    "para identificar quais CONTÊM INFORMAÇÕES ÚTEIS DE COTAÇÃO (como preço/diária, "
    "impostos/ISS, disponibilidade confirmada, regras de pagamento/cancelamento, "
    "condições de reserva). "
    "Você NÃO deve marcar pedidos, confirmações genéricas, agradecimentos ou respostas sem dados concretos."
)

def build_selector_prompt(thread_id: str, emails: List[Dict]) -> str:
    """
    Esperamos que o modelo retorne JSON estrito:
    [
      {"email_index": 2, "is_useful": true, "reasons": "tem preços e ISS", "confidence": 0.95},
      ...
    ]
    Critérios:
      - is_useful = true SE (a) há VALOR monetário explícito ou (b) regras/políticas detalhadas de pagamento/
        cancelamento/condições ou (c) confirmação clara de disponibilidade acompanhada de categoria/tarifa.
      - NÃO marcar como útil e-mails de solicitação de cotação, 'em breve retornamos', ou encaminhamentos sem números.
      - Em caso de dúvida, prefira o mais RECENTE que contenha dados concretos.
    """
    lines = []
    lines.append(f"THREAD_ID: {thread_id or '(sem id)'}")
    lines.append("TAREFA: Para cada e-mail enumerado abaixo, decida se ele contém informação útil de COTAÇÃO.")
    lines.append("RETORNE SOMENTE JSON (sem comentários, sem markdown). Formato:")
    lines.append('[{"email_index": <int>, "is_useful": true|false, "reasons": "<curto>", "confidence": <0..1>}, ...]')
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
    # reforço final de comportamento
    lines.append(
        "REGRAS:\n"
        "- Marque como útil quando houver ao menos UM valor ou regra concreta (ex.: 'SGL/DBL R$ 975,00 + 5% ISS', "
        "'Pré-pagamento 50%', 'Tarifa não reembolsável', 'Confirmamos disponibilidade para 01-04/01/2026').\n"
        "- Não marque e-mails que apenas pedem cotação, agradecem, ou prometem retorno.\n"
        "- Se mais de um e-mail for útil, retorne todos (cada um com seu 'email_index').\n"
        "- Se nenhum for útil, retorne lista vazia []."
    )
    return "\n".join(lines)

# -------------- Core LLM selection --------------

def select_useful_indices_with_llm(thread_id: str, emails: List[Dict]) -> List[int]:
    model_name = os.getenv("GEMINI_MODEL_NAME", "gemini-1.5-flash").strip()
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("⛔ GEMINI_API_KEY não definido no .env")
    genai.configure(api_key=api_key)

    model = genai.GenerativeModel(model_name, system_instruction=SYSTEM_INSTR)
    prompt = build_selector_prompt(thread_id, emails)
    resp = model.generate_content(prompt)
    raw = get_response_text(resp)
    items = force_json_list(raw)

    useful = []
    for it in items:
        if isinstance(it, dict) and it.get("is_useful") is True:
            idx = it.get("email_index")
            if isinstance(idx, int) and 0 <= idx < len(emails):
                useful.append(idx)

    # ordena e dedup
    useful = sorted(set(useful))
    return useful

# -------------- Fallback simples --------------

def fallback_useful_indices(emails: List[Dict]) -> List[int]:
    """
    Se o LLM falhar/bloquear/retornar vazio:
      - pega o MAIS RECENTE que contenha padrão de moeda.
    """
    for i in range(len(emails) - 1, -1, -1):
        body = emails[i].get("body") or ""
        subj = emails[i].get("subject") or ""
        if CURRENCY_RE.search(body) or CURRENCY_RE.search(subj):
            return [i]
    return []

# -------------- Save important e-mails --------------

def save_important_email(thread_id: str, idx: int, e: Dict, src_path: str, llm_meta: Dict) -> str:
    ensure_dir(IMPORTANT_DIR)
    ts = (e.get("timestamp") or "na")
    subject = sanitize_fname(e.get("subject") or "")
    fname = f"{sanitize_fname(thread_id or 'single')}__{sanitize_fname(ts)}__{idx:02d}__{subject}.json"
    outp = os.path.join(IMPORTANT_DIR, fname)
    payload = {
        "_source_raw": src_path,
        "_thread_id": thread_id,
        "_email_index": idx,
        "_llm_decision": llm_meta,  # guardamos a decisão/resposta bruta do LLM
        "timestamp": e.get("timestamp"),
        "sender": e.get("sender"),
        "recipient": e.get("recipient"),
        "subject": e.get("subject"),
        "body": e.get("body"),
    }
    with open(outp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return outp

# -------------- Main --------------

def main():
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.json")))
    if not files:
        print(f"⛔ Nenhum arquivo .json encontrado em {RAW_DIR}/")
        return

    print(f"🧠 Seleção via LLM em {len(files)} arquivo(s) de {RAW_DIR}/")
    saved = 0

    for path in files:
        meta = load_email_json(path)
        # Normaliza para lista de e-mails (ordenados por timestamp)
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

        # 1) LLM decide índices úteis
        try:
            useful_idx = select_useful_indices_with_llm(thread_id, emails)
        except Exception as e:
            print(f"⚠️ LLM falhou em {os.path.basename(path)}: {e}")
            useful_idx = []

        # 2) Fallback: mais recente com preço
        if not useful_idx:
            useful_idx = fallback_useful_indices(emails)

        if not useful_idx:
            print(f"➖ Nenhum e-mail útil em {os.path.basename(path)}")
            continue

        # 3) Salva cada e-mail útil como JSON achatado
        #    Também armazenamos a resposta do LLM para auditoria
        llm_meta = {}
        try:
            # reexecuta para capturar a resposta em bruto (para auditoria)
            model = genai.GenerativeModel(os.getenv("GEMINI_MODEL_NAME", "gemini-1.5-flash").strip(),
                                          system_instruction=SYSTEM_INSTR)
            prompt = build_selector_prompt(thread_id, emails)
            resp = model.generate_content(prompt)
            llm_meta = {"raw": get_response_text(resp)}
        except Exception:
            llm_meta = {}

        for idx in useful_idx:
            outp = save_important_email(thread_id, idx, emails[idx], path, llm_meta)
            saved += 1
            print(f"✅ IMPORTANTE (LLM) → {os.path.basename(outp)}")

    print(f"📦 Salvos {saved} e-mail(s) úteis em {IMPORTANT_DIR}/")

if __name__ == "__main__":
    main()
