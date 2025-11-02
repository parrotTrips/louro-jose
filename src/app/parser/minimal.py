# src/app/parser/minimal.py
from __future__ import annotations
import argparse
import json
import re
import hashlib
from html import unescape
from typing import Dict, Any, Iterable, Optional, Tuple

from app.core.config import get_settings
from app.core import io_gcs

# ------------------------ Utilidades de parsing --------------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]+")
_NL_RE = re.compile(r"\s*\n\s*")

def _sha1short(text: str, length: int = 8) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:length]

def _html_to_text(html: str) -> str:
    if not isinstance(html, str):
        return ""
    # remove scripts/styles
    html = re.sub(r"(?is)<(script|style)\b[^>]*>.*?</\1>", "", html)
    # remove tags
    txt = _TAG_RE.sub("", html)
    # unescape entidades
    txt = unescape(txt)
    # normaliza espaços/linhas
    txt = _WS_RE.sub(" ", txt)
    txt = _NL_RE.sub("\n", txt).strip()
    return txt

def _normalize_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _NL_RE.sub("\n", text)
    text = text.strip()
    return text

def _extract_plain_text_from_gmail_payload(msg: Dict[str, Any]) -> str:
    """
    Extrai texto priorizando 'text/plain'. Se não houver, tenta 'text/html' e faz strip.
    Espera-se um RAW no formato do Gmail (payload/parts/body.data etc.).
    """
    payload = msg.get("payload", {}) or msg.get("Payload", {})  # tolerante a variações
    mimeType = payload.get("mimeType", "")

    def _decode_data(data_b64url: Optional[str]) -> str:
        if not data_b64url:
            return ""
        import base64
        try:
            return base64.urlsafe_b64decode(data_b64url + "==").decode("utf-8", errors="replace")
        except Exception:
            pad = "=" * ((4 - len(data_b64url) % 4) % 4)
            return base64.urlsafe_b64decode((data_b64url + pad).encode("utf-8")).decode("utf-8", errors="replace")

    def _walk_parts(part: Dict[str, Any]) -> Iterable[Tuple[str, str]]:
        mt = part.get("mimeType", "")
        body = part.get("body", {}) or {}
        data = body.get("data")
        if data:
            yield (mt, _decode_data(data))
        for sub in part.get("parts", []) or []:
            yield from _walk_parts(sub)

    # Caso simples: sem parts (mensagem simples)
    if mimeType and not payload.get("parts"):
        body = payload.get("body", {}) or {}
        data = body.get("data")
        text = _decode_data(data) if data else ""
        if mimeType.startswith("text/plain"):
            return _normalize_text(text)
        if mimeType.startswith("text/html"):
            return _normalize_text(_html_to_text(text))

    # Caso multipart: percorre parts
    plain_candidates = []
    html_candidates = []
    for mt, content in _walk_parts(payload):
        if mt.startswith("text/plain") and content:
            plain_candidates.append(content)
        elif mt.startswith("text/html") and content:
            html_candidates.append(content)

    if plain_candidates:
        return _normalize_text("\n".join(plain_candidates))
    if html_candidates:
        html_join = "\n".join(html_candidates)
        return _normalize_text(_html_to_text(html_join))

    # Fallback: snippet
    snippet = msg.get("snippet", "")
    return _normalize_text(snippet)

# ------------------------ Caminhos GCS -----------------------------------------

def _parsed_gcs_path(thread_id: str, message_id: str, sha1: str) -> str:
    return f"parsed/{thread_id}/{message_id}__{sha1}.json"

def _iter_raw_objects() -> Iterable[str]:
    """
    Itera caminhos 'raw/<threadId>/<messageId>.json' no GCS.
    """
    yield from io_gcs.list_objects(prefix="raw/")

# ------------------------ Núcleo do parser -------------------------------------

def _parse_one_raw(raw_path: str) -> Optional[Dict[str, Any]]:
    """
    Carrega um RAW e retorna o dicionário pronto para salvar em parsed/.
    Retorna None se não conseguir extrair um texto útil.
    """
    try:
        raw = io_gcs.load_json_from_gcs(raw_path)
    except Exception:
        return None

    message_id = raw.get("id") or raw.get("messageId")
    thread_id  = raw.get("threadId") or raw.get("thread_id")
    if not message_id or not thread_id:
        return None

    # Headers úteis para a etapa LLM (salvamos já no parsed/)
    payload = raw.get("payload", {}) or {}
    headers_list = payload.get("headers", []) or []
    headers = { (h.get("name") or "").lower(): (h.get("value") or "") for h in headers_list }

    email_from = headers.get("from", "") or raw.get("From", "")
    subject    = headers.get("subject", "") or raw.get("Subject", "")
    date_hdr   = headers.get("date", "") or raw.get("Date", "")

    text = _extract_plain_text_from_gmail_payload(raw) or ""
    if not text.strip():
        return None

    sha1 = _sha1short(text)
    parsed = {
        "threadId": thread_id,
        "messageId": message_id,
        "sha1": sha1,
        "plain_text": text,
        "size": len(text),
        # Headers que ajudarão o LLM na normalização
        "header_from": email_from,
        "header_subject": subject,
        "header_date": date_hdr,
    }
    return parsed

def run() -> Dict[str, int]:
    """
    Varre GCS 'raw/' e cria 'parsed/<threadId>/<messageId>__<sha1>.json'.
    Idempotente: se parsed existir, pula.
    Retorna e imprime: {'seen', 'parsed', 'skipped', 'errors'}.
    """
    _ = get_settings()  # mantém compatível se você usa env aqui

    seen = parsed = skipped = errors = 0

    for raw_path in _iter_raw_objects():
        seen += 1
        try:
            raw_name = raw_path.split("/", 2)[-1]  # threadId/messageId.json
            parts = raw_name.split("/")
            if len(parts) != 2 or not parts[1].endswith(".json"):
                skipped += 1
                continue

            thread_id = parts[0]
            message_id = parts[1][:-5]  # remove ".json"

            parsed_doc = _parse_one_raw(raw_path)
            if not parsed_doc:
                skipped += 1
                continue

            sha1 = parsed_doc["sha1"]
            out_path = _parsed_gcs_path(thread_id, message_id, sha1)

            if io_gcs.exists(out_path):
                skipped += 1
                continue

            io_gcs.save_json_to_gcs(out_path, parsed_doc)
            parsed += 1

        except Exception:
            errors += 1
            continue

    summary = {"seen": seen, "parsed": parsed, "skipped": skipped, "errors": errors}
    print(json.dumps(summary, ensure_ascii=False))
    return summary

# ------------------------ CLI --------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Parser mínimo: raw/ → parsed/")
    return p.parse_args()

if __name__ == "__main__":
    parse_args()
    run()
