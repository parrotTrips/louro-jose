# src/app/parser/minimal.py
from __future__ import annotations

import base64
import hashlib
import html
import json
import re
from typing import Dict, Any, Optional, Tuple, List

from google.cloud import storage

from app.core import io_gcs  # save_json_to_gcs, load_json_from_gcs, object_exists_in_gcs
try:
    from app.core import state  # opcional
except Exception:
    state = None


# -----------------------------
# Helpers de extração de texto
# -----------------------------
def _b64url_decode(data: str) -> bytes:
    if not data:
        return b""
    padding = '=' * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)

def _strip_html(html_text: str) -> str:
    no_scripts = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", html_text or "")
    text = re.sub(r"(?s)<[^>]+>", " ", no_scripts)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()

def _walk_parts_for_mime(part: Dict[str, Any], wanted: str) -> Optional[str]:
    if not part:
        return None
    mime = part.get("mimeType") or part.get("mimetype")
    body = part.get("body", {})
    data = body.get("data")
    parts = part.get("parts") or []

    if mime == wanted and data:
        try:
            return _b64url_decode(data).decode(errors="replace")
        except Exception:
            return None

    if wanted.startswith("text/") and data and not parts and isinstance(data, str) and mime and mime.startswith("text/"):
        try:
            return _b64url_decode(data).decode(errors="replace")
        except Exception:
            pass

    for p in parts:
        got = _walk_parts_for_mime(p, wanted)
        if got:
            return got
    return None

def extract_plain_text_from_payload(payload: Dict[str, Any]) -> Tuple[str, str]:
    if not payload:
        return ("", "empty")

    txt = _walk_parts_for_mime(payload, "text/plain")
    if isinstance(txt, str) and txt.strip():
        return (txt.strip(), "text/plain")

    html_text = _walk_parts_for_mime(payload, "text/html")
    if isinstance(html_text, str) and html_text.strip():
        return (_strip_html(html_text), "text/html")

    body = payload.get("body", {})
    data = body.get("data")
    if isinstance(data, str) and data.strip():
        raw = _b64url_decode(data).decode(errors="replace")
        mime = payload.get("mimeType") or payload.get("mimetype") or ""
        if isinstance(raw, str) and raw.strip():
            if str(mime).lower().startswith("text/html"):
                return (_strip_html(raw), "text/html")
            return (raw.strip(), "text/plain")

    return ("", "empty")

def sha1short(text: str, length: int = 8) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()[:length]

def _headers_to_dict(payload: Dict[str, Any]) -> Dict[str, str]:
    hdrs = {}
    for h in (payload or {}).get("headers", []):
        name = h.get("name")
        value = h.get("value")
        if name:
            hdrs[name.lower()] = value or ""
    return hdrs


# -----------------------------
# Listagem de objetos raw/ no GCS
# -----------------------------
def _list_objects(bucket: str, prefix: str) -> List[str]:
    client = storage.Client()
    bkt = client.bucket(bucket)
    # Usamos list_blobs; retornar nomes (blob.name)
    return [blob.name for blob in client.list_blobs(bkt, prefix=prefix)]


# -----------------------------
# Núcleo do PASSO 5 (Parser)
# -----------------------------
def parse_raw_to_parsed(bucket: str, limit: int | None = None) -> Dict[str, int]:
    summary = {"seen": 0, "parsed": 0, "skipped": 0, "errors": 0}

    # lista todos os objetos sob raw/
    raw_keys: List[str] = _list_objects(bucket, prefix="raw/")
    # filtra só arquivos .json de mensagem (raw/<threadId>/<messageId>.json)
    raw_keys = [k for k in raw_keys if k.endswith(".json") and k.count("/") >= 2]

    if isinstance(limit, int) and limit > 0:
        raw_keys = raw_keys[:limit]

    for raw_key in raw_keys:
        summary["seen"] += 1
        try:
            parts = raw_key.split("/")
            thread_id = parts[1] if len(parts) >= 3 else ""
            message_file = parts[2] if len(parts) >= 3 else ""
            message_id = message_file.replace(".json", "")

            # (opcional) idempotência por state
            if state and hasattr(state, "is_message_processed"):
                try:
                    if state.is_message_processed(bucket=bucket, message_id=message_id):
                        print(f"→ SKIP (state): {message_id}")
                        summary["skipped"] += 1
                        continue
                except Exception:
                    pass

            raw_obj = io_gcs.load_json_from_gcs(bucket, raw_key)
            if not isinstance(raw_obj, dict):
                raise ValueError("Objeto raw não é um dict JSON.")

            msg_id = raw_obj.get("id") or message_id
            th_id = raw_obj.get("threadId") or thread_id
            snippet = raw_obj.get("snippet") or ""

            payload = raw_obj.get("payload") or {}
            headers = _headers_to_dict(payload)

            date = headers.get("date", "")
            subject = headers.get("subject", "")
            from_ = headers.get("from", "")
            to_ = headers.get("to", "")

            plain_text, source = extract_plain_text_from_payload(payload)
            if not plain_text.strip() and snippet:
                plain_text = snippet.strip()
                source = "snippet"

            hshort = sha1short(plain_text)
            parsed_key = f"parsed/{th_id}/{msg_id}__{hshort}.json"

            # idempotência por existência do arquivo de saída
            if io_gcs.object_exists_in_gcs(bucket, parsed_key):
                print(f"→ SKIP (exists): {parsed_key}")
                summary["skipped"] += 1
                if state and hasattr(state, "mark_message_processed"):
                    try:
                        state.mark_message_processed(bucket=bucket, message_id=msg_id)
                    except Exception:
                        pass
                continue

            out = {
                "message_id": msg_id,
                "thread_id": th_id,
                "date": date,
                "from": from_,
                "to": to_,
                "subject": subject,
                "snippet": snippet,
                "plain_text": plain_text,
                "_text_source": source,
                "_raw_object_path": raw_key,
                "_parsed_key": parsed_key,
                "_sha1short": hshort,
            }

            io_gcs.save_json_to_gcs(bucket, parsed_key, out)
            print(f"✓ PARSED: {parsed_key}")
            summary["parsed"] += 1

            if state and hasattr(state, "mark_message_processed"):
                try:
                    state.mark_message_processed(bucket=bucket, message_id=msg_id)
                except Exception:
                    pass

        except FileNotFoundError as e:
            print(f"✗ ERROR (not found) {raw_key}: {e}")
            summary["errors"] += 1
        except KeyError as e:
            print(f"✗ ERROR (missing key) {raw_key}: {e}")
            summary["errors"] += 1
        except ValueError as e:
            print(f"✗ ERROR (value) {raw_key}: {e}")
            summary["errors"] += 1
        except Exception as e:
            try:
                ctx = {"raw_key": raw_key, "err": str(e)}
                print(f"✗ ERROR (unexpected) {json.dumps(ctx, ensure_ascii=False)}")
            except Exception:
                print(f"✗ ERROR (unexpected) {raw_key}: {e}")
            summary["errors"] += 1

    return summary


if __name__ == "__main__":
    from app.core.config import get_settings
    bucket = get_settings().gcs_bucket
    s = parse_raw_to_parsed(bucket=bucket, limit=None)
    print(json.dumps(s, ensure_ascii=False, indent=2))
