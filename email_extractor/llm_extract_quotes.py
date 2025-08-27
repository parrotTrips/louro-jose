import os
import sys
import glob
import json
import re
from typing import Dict, List

from dotenv import load_dotenv
import google.generativeai as genai

# permitir imports do pacote "modules" e carregar variáveis do .env da raiz
sys.path.append("..")
load_dotenv("../.env")

from modules.headers import HEADER_FIELDS  # noqa: E402
from modules.prompt import SYSTEM_INSTRUCTIONS, build_user_prompt  # noqa: E402
from modules.text_clean import strip_forwarding_noise  # noqa: E402
from modules.io_email import load_email_json, infer_timestamp_from_filename  # noqa: E402
from modules.json_utils import blank_row, ensure_dir  # noqa: E402

RAW_DIR = "raw_messages"                # relativo a email_extractor/
OUT_DIR = "outputs"                     # relativo a email_extractor/
OUT_JSONL = os.path.join(OUT_DIR, "quotes_extracted.jsonl")


def _get_response_text(resp) -> str:
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

def _extract_json_block(text: str) -> str:
    """Extrai o bloco JSON (objeto ou array) de uma resposta possivelmente com markdown."""
    t = (text or "").strip()
    m = re.search(r"```json\s*([\s\S]*?)\s*```", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # tenta pegar array
    a0, a1 = t.find("["), t.rfind("]")
    if a0 != -1 and a1 != -1 and a1 > a0:
        return t[a0:a1+1].strip()
    # tenta pegar objeto
    o0, o1 = t.find("{"), t.rfind("}")
    if o0 != -1 and o1 != -1 and o1 > o0:
        return t[o0:o1+1].strip()
    return t

def _force_json_to_list(text: str) -> List[Dict[str, str]]:
    """Converte a resposta em lista de objetos (se vier objeto único, embrulha)."""
    block = _extract_json_block(text)
    data = json.loads(block)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [it for it in data if isinstance(it, dict)]
    return []

def _only_digits_str(s) -> str:
    if s is None:
        return ""
    m = re.search(r"\d+", str(s))
    return m.group(0) if m else ""

def _normalize_label(label: str) -> str:
    if not label:
        return ""
    s = str(label).lower()
    s = re.sub(r"\b(apto|apartamento|ap\.?)\b", "", s)
    s = s.replace("quarto", "").replace("quartos", "")
    s = s.replace(" - ", " ")
    s = re.sub(r"[:\-–—]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _parse_brl_price_to_float_string(val) -> str:
    """Aceita 'R$ 1.234,56' ou '1234.56' e devolve string '1234.56' (2 casas)."""
    if val is None:
        return ""
    s = str(val).strip()
    if s == "":
        return ""
    s = re.sub(r"[^\d\.,]", "", s)
    if s == "":
        return ""
    if "," in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return f"{float(s):.2f}"
    except Exception:
        return ""

def _coerce_to_header(item: Dict[str, str], meta: Dict[str, str]) -> Dict[str, str]:
    """Garante aderência ao HEADER_FIELDS e aplica pequenas normalizações."""
    out = blank_row(HEADER_FIELDS)
    # Copia o que veio
    for k in HEADER_FIELDS:
        v = item.get(k, "")
        out[k] = "" if v is None else str(v)

    # Metadados críticos
    if not out["Timestamp"]:
        out["Timestamp"] = meta.get("timestamp", "") or ""
    if not out["Fornecedor"]:
        out["Fornecedor"] = meta.get("from", "") or ""
    if not out["Assunto"]:
        out["Assunto"] = meta.get("subject", "") or ""

    # Normalizações
    out["Número de quartos"] = _only_digits_str(out.get("Número de quartos", ""))
    if not out.get("Tipo de quarto (normalizado)"):
        out["Tipo de quarto (normalizado)"] = _normalize_label(out.get("Tipo de quarto", ""))
    out["Preço (num)"] = _parse_brl_price_to_float_string(out.get("Preço (num)", ""))

    return out

def _call_llm(meta: Dict, body_clean: str) -> List[Dict[str, str]]:
    model_name = os.getenv("GEMINI_MODEL_NAME", "gemini-1.5-flash").strip()
    model = genai.GenerativeModel(model_name, system_instruction=SYSTEM_INSTRUCTIONS)
    prompt = build_user_prompt(HEADER_FIELDS, meta, body_clean)
    resp = model.generate_content(prompt)

    text = _get_response_text(resp)
    if not text:
        raise RuntimeError("Resposta vazia/bloqueada do modelo.")

    items = _force_json_to_list(text)
    if not items:
        raise RuntimeError("Resposta não contém objeto/array JSON válido.")

    # Coerção por item
    coerced = [_coerce_to_header(it, meta) for it in items]
    return coerced

# ----------------- Main -----------------

def main():
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("⛔ GEMINI_API_KEY não definido no .env")
    genai.configure(api_key=api_key)

    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.json")))
    if not files:
        print(f"⛔ Nenhum arquivo .json encontrado em {RAW_DIR}/")
        return

    print(f"🗃️ Encontrados {len(files)} arquivo(s) em {RAW_DIR}/")
    rows: List[Dict[str, str]] = []

    for path in files:
        meta_all = load_email_json(path)
        body_clean = strip_forwarding_noise(meta_all["body"])
        meta_for_llm = {
            "timestamp": meta_all["timestamp"] or infer_timestamp_from_filename(path),
            # NÃO usamos mais 'to' (destinatário) no prompt por ser fixo na sua operação
            "subject": meta_all["subject"],
            "from": meta_all["from"],
        }

        try:
            items = _call_llm(meta_for_llm, body_clean)
            # cada item já é uma "linha" no HEADER_FIELDS
            rows.extend(items)
            print(f"✅ Extraído: {meta_all.get('_filename','(sem nome)')} — {len(items)} linha(s)")
        except Exception as e:
            print(f"⚠️ Falha em {meta_all.get('_filename')}: {e}")
            # Ainda registra uma linha mínima com metadados
            row = blank_row(HEADER_FIELDS)
            row["Timestamp"]  = meta_for_llm["timestamp"]
            row["Fornecedor"] = meta_for_llm["from"]
            row["Assunto"]    = meta_for_llm["subject"]
            rows.append(row)

    ensure_dir(OUT_DIR)
    with open(OUT_JSONL, "w", encoding="utf-8") as f:
        for obj in rows:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    print(f"📄 Gerado: {OUT_JSONL} ({len(rows)} linha(s))")
    if rows:
        print("🔎 Preview (1ª linha):")
        print(json.dumps(rows[0], ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
