# src/app/parser/repair_parsed_headers.py
from __future__ import annotations
import json
import re
import hashlib
from html import unescape
from typing import Dict, Any, Iterable, Optional, Tuple, List

from app.core.config import load_config
from app.core import io_gcs

# ================= Utils de normalização/extração =================

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]+")
_NL_RE = re.compile(r"\s*\n\s*")

def _sha1short(text: str, length: int = 8) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:length]

def _html_to_text(html: str) -> str:
    if not isinstance(html, str):
        return ""
    # remove <script>/<style>
    html = re.sub(r"(?is)<(script|style)\b[^>]*>.*?</\1>", "", html)
    # tira tags
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
    return text.strip()

def _extract_plain_text_from_gmail_payload(msg: Dict[str, Any]) -> str:
    """
    Extrai texto priorizando 'text/plain'; se não houver, usa 'text/html' e faz strip.
    Aceita o JSON raw direto retornado pelo Gmail API (payload/parts/body.data base64url).
    """
    payload = msg.get("payload", {}) or msg.get("Payload", {})  # tolerante a nome
    mimeType = payload.get("mimeType", "")

    def _decode_data(data_b64url: Optional[str]) -> str:
        if not data_b64url:
            return ""
        import base64
        try:
            # Gmail usa base64url
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

    # caso simples: sem parts
    if mimeType and not payload.get("parts"):
        body = payload.get("body", {}) or {}
        data = body.get("data")
        text = _decode_data(data) if data else ""
        if mimeType.startswith("text/plain"):
            return _normalize_text(text)
        if mimeType.startswith("text/html"):
            return _normalize_text(_html_to_text(text))

    # multipart: coleciona
    plain_candidates: List[str] = []
    html_candidates: List[str] = []
    for mt, content in _walk_parts(payload):
        if mt.startswith("text/plain") and content:
            plain_candidates.append(content)
        elif mt.startswith("text/html") and content:
            html_candidates.append(content)

    if plain_candidates:
        return _normalize_text("\n".join(plain_candidates))
    if html_candidates:
        return _normalize_text(_html_to_text("\n".join(html_candidates)))

    # fallback: snippet
    return _normalize_text(msg.get("snippet", ""))

def _headers_from_payload(msg: Dict[str, Any]) -> dict:
    """
    Constrói um dict {name_lower: value} a partir de payload.headers[][], com fallbacks.
    """
    payload = msg.get("payload", {}) or {}
    headers_list = payload.get("headers", []) or []
    h = { (x.get("name") or "").lower(): (x.get("value") or "") for x in headers_list }
    # fallbacks básicos (casos em que o raw veio com campos soltos)
    h.setdefault("from",    msg.get("From", "") or "")
    h.setdefault("subject", msg.get("Subject", "") or "")
    h.setdefault("date",    msg.get("Date", "") or "")
    return h

# ================= Rebuild parsed/ =================

def _rebuild_parsed_from_raw(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Recebe um RAW (JSON do Gmail) e monta o documento de parsed/ no formato novo:
    {
      "threadId", "messageId", "sha1", "plain_text", "size",
      "header_from", "header_subject", "header_date"
    }
    """
    message_id = raw.get("id") or raw.get("messageId")
    thread_id  = raw.get("threadId") or raw.get("thread_id")
    if not message_id or not thread_id:
        return None

    headers = _headers_from_payload(raw)
    email_from = headers.get("from", "")
    subject    = headers.get("subject", "")
    date_hdr   = headers.get("date", "")

    text = _extract_plain_text_from_gmail_payload(raw) or ""
    if not text.strip():
        return None

    sha1 = _sha1short(text)
    return {
        "threadId": thread_id,
        "messageId": message_id,
        "sha1": sha1,
        "plain_text": text,
        "size": len(text),
        "header_from": email_from,
        "header_subject": subject,
        "header_date": date_hdr,
    }

def run() -> dict:
    """
    Varre todos os RAW em raw/ e (re)grava seus correspondentes em parsed/ no formato novo.
    SOBRESCREVE arquivos existentes em parsed/.
    """
    cfg = load_config()
    bucket = cfg.gcs_bucket

    seen = written = skipped = errors = 0
    samples: List[dict] = []

    raw_paths = [p for p in io_gcs.list_objects(bucket, prefix="raw/") if p.endswith(".json")]

    for raw_path in raw_paths:
        seen += 1
        try:
            raw = io_gcs.load_json_from_gcs(bucket, raw_path)
            doc = _rebuild_parsed_from_raw(raw)
            if not doc:
                skipped += 1
                continue
            th = doc["threadId"]; msg = doc["messageId"]; sha = doc["sha1"]
            dst = f"parsed/{th}/{msg}__{sha}.json"
            io_gcs.save_json_to_gcs(bucket, dst, doc)
            written += 1
        except Exception as e:
            errors += 1
            if len(samples) < 5:
                samples.append({"raw": raw_path, "error": repr(e)})
            continue

    out = {
        "seen_raw": seen,
        "parsed_rewritten": written,
        "skipped": skipped,
        "errors": errors,
        "error_samples": samples,
    }
    print(json.dumps(out, ensure_ascii=False))
    return out

if __name__ == "__main__":
    run()
