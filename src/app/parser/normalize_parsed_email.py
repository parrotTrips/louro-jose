# src/app/parser/normalize_parsed_email.py
from __future__ import annotations
import json
import hashlib
from datetime import datetime
from typing import Dict, Any, List

from app.core.config import load_config
from app.core import io_gcs
from .llm_extract import extract_quotes_from_context

# Campos mínimos exigidos por linha (para considerarmos “válida”)
REQUIRED = {"_key", "Timestamp", "Fornecedor", "Nome do hotel",
            "Categoria do quarto", "Configuração do quarto", "Preço (num)"}

def _sha1_short(s: str, n: int = 8) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:n]

def _build_key(thread_id: str, message_id: str,
               hotel: str, categoria: str, config: str, preco: float) -> str:
    base = f"{thread_id}|{message_id}|{hotel}|{categoria}|{config}|{preco:.2f}"
    return _sha1_short(base, 16)

def _valid(r: Dict[str, Any]) -> bool:
    return isinstance(r, dict) and REQUIRED.issubset(r.keys())

def _finalize_rows(doc: Dict[str, Any], quotes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    th_id = doc.get("threadId") or "unknown_thread"
    msg_id = doc.get("messageId") or "unknown_msg"
    subject = doc.get("header_subject") or ""
    date_hdr = doc.get("header_date") or ""
    rows: List[Dict[str, Any]] = []

    for q in quotes:
        hotel = (q.get("Nome do hotel") or "").strip() or "Hotel"
        categoria = (q.get("Categoria do quarto") or "").strip() or "-"
        config = (q.get("Configuração do quarto") or "").strip() or "-"
        preco = float(q.get("Preço (num)") or 0.0)

        # Completa alguns campos úteis quando ausentes
        if not q.get("Assunto"):
            q["Assunto"] = subject
        if not q.get("Timestamp"):
            # usa o header original quando houver, senão um UTC agora
            q["Timestamp"] = date_hdr or (datetime.utcnow().isoformat() + "Z")

        q["_key"] = _build_key(th_id, msg_id, hotel, categoria, config, preco)
        rows.append(q)
    return rows

def run() -> Dict[str, int]:
    """
    Lê parsed/ (com plain_text + headers) → chama LLM → grava parsed_fixed/ com lista de cotações.
    """
    cfg = load_config()
    bucket = cfg.gcs_bucket

    names = [n for n in io_gcs.list_objects(bucket, prefix="parsed/") if n.endswith(".json")]
    seen = saved = skipped = errors = 0

    for name in names:
        seen += 1
        try:
            doc = io_gcs.load_json_from_gcs(bucket, name)
            if not isinstance(doc, dict) or "plain_text" not in doc:
                skipped += 1
                continue

            subject = doc.get("header_subject") or ""
            email_from = doc.get("header_from") or ""
            date_hdr = doc.get("header_date") or ""
            body = doc.get("plain_text") or ""

            # 1) Extrai via LLM (pode retornar 1..N cotações)
            quotes = extract_quotes_from_context(
                subject=subject,
                email_from=email_from,
                date=date_hdr,
                body=body,
            )
            if not quotes:
                skipped += 1
                continue

            # 2) Ajusta _key, preenche Timestamp/Assunto se faltando
            rows = _finalize_rows(doc, quotes)
            if not rows:
                skipped += 1
                continue

            # 3) Grava na pasta parsed_fixed/THREAD/MSG__SHA.json
            th_id = doc.get("threadId") or "unknown_thread"
            msg_id = doc.get("messageId") or "unknown_msg"
            sha = doc.get("sha1") or "fix"
            dst = f"parsed_fixed/{th_id}/{msg_id}__{sha}.json"

            io_gcs.save_json_to_gcs(bucket, dst, rows)
            saved += 1

        except Exception as e:
            errors += 1
            # Registra erro para depuração futura
            err_obj = {"_error": repr(e), "_source": name}
            err_dst = f"parsed_fixed_errors/{name.rsplit('/', 1)[-1]}"
            try:
                io_gcs.save_json_to_gcs(bucket, err_dst, err_obj)
            except Exception:
                pass

    summary = {"seen": seen, "saved": saved, "skipped": skipped, "errors": errors}
    print(json.dumps(summary, ensure_ascii=False))
    return summary

if __name__ == "__main__":
    run()
