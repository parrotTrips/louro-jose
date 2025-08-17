import os
import glob
import json
from typing import Dict, List

from dotenv import load_dotenv
import google.generativeai as genai

from utils.headers import HEADER_FIELDS
from utils.prompt import SYSTEM_INSTRUCTIONS, build_user_prompt
from utils.text_clean import strip_forwarding_noise
from utils.io_email import load_email_json, infer_timestamp_from_filename
from utils.json_utils import force_json_object, blank_row, ensure_dir

RAW_DIR = "raw_messages"
OUT_DIR = "outputs"
OUT_JSONL = os.path.join(OUT_DIR, "quotes_extracted.jsonl")


def _get_response_text(resp) -> str:
    """Extrai texto do objeto de resposta do SDK do Gemini de forma resiliente."""
    text = (getattr(resp, "text", None) or "").strip()
    if text:
        return text
    # fallback (candidates/parts), compatível com google-generativeai >= 0.6
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


def _call_llm(meta: Dict, body_clean: str) -> Dict[str, str]:
    model_name = os.getenv("GEMINI_MODEL_NAME", "gemini-1.5-flash").strip()
    model = genai.GenerativeModel(model_name, system_instruction=SYSTEM_INSTRUCTIONS)
    prompt = build_user_prompt(HEADER_FIELDS, meta, body_clean)
    resp = model.generate_content(prompt)

    text = _get_response_text(resp)
    if not text:
        raise RuntimeError("Resposta vazia/bloqueada do modelo.")

    data = force_json_object(text)
    out = blank_row(HEADER_FIELDS)
    for k in HEADER_FIELDS:
        v = data.get(k, "")
        out[k] = "" if v is None else str(v)
    return out


def main():
    load_dotenv()
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
            "to": meta_all["to"],
            "subject": meta_all["subject"],
            "from": meta_all["from"],
        }

        try:
            row = _call_llm(meta_for_llm, body_clean)
            # se o LLM não preencher, garantimos metadados mínimos
            row["Timestamp"] = row.get("Timestamp") or meta_for_llm["timestamp"]
            row["Destinatário"] = row.get("Destinatário") or meta_for_llm["to"]
            row["Assunto"] = row.get("Assunto") or meta_for_llm["subject"]
            rows.append(row)
            print(f"✅ Extraído: {meta_all.get('_filename','(sem nome)')}")
        except Exception as e:
            print(f"⚠️ Falha em {meta_all.get('_filename')}: {e}")
            # ainda assim registra linha com metadados mínimos
            row = blank_row(HEADER_FIELDS)
            row["Timestamp"] = meta_for_llm["timestamp"]
            row["Destinatário"] = meta_for_llm["to"]
            row["Assunto"] = meta_for_llm["subject"]
            rows.append(row)

    ensure_dir(OUT_DIR)
    with open(OUT_JSONL, "w", encoding="utf-8") as f:
        for obj in rows:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    print(f"📄 Gerado: {OUT_JSONL}")
    if rows:
        print("🔎 Preview (1ª linha):")
        print(json.dumps(rows[0], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
