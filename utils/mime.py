from __future__ import annotations
import base64
from bs4 import BeautifulSoup
from typing import Dict, Iterable, Optional, Any

def _pad_b64url(data: str) -> str:
    return data + "=" * (-len(data) % 4)

def b64url_decode(data: str) -> bytes:
    return base64.urlsafe_b64decode(_pad_b64url(data))

def get_header(headers: Iterable[Dict[str, str]], name: str) -> Optional[str]:
    for h in headers or []:
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return None

def _walk_parts(payload: Dict[str, Any]):
    if not payload:
        return
    yield payload
    parts = payload.get("parts") or []
    for p in parts:
        yield from _walk_parts(p)

def _extract_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    lines = [ln.strip() for ln in text.splitlines()]
    return "\n".join([ln for ln in lines if ln])

def extract_prefer_plaintext(payload: Dict[str, Any]) -> str:
    """
    Retorna corpo como texto:
      1) Prioriza 'text/plain'
      2) Se não houver, converte 'text/html' para texto
      3) Caso contrário, string vazia
    """
    text_plain = None
    text_html = None

    for part in _walk_parts(payload):
        mime_type = part.get("mimeType", "")
        body = part.get("body", {}) or {}
        data = body.get("data")
        if not data:
            continue

        decoded = b64url_decode(data)
        try:
            content = decoded.decode("utf-8", errors="replace")
        except Exception:
            content = decoded.decode(errors="replace")

        if mime_type.startswith("text/plain") and not text_plain:
            text_plain = content
        elif mime_type.startswith("text/html") and not text_html:
            text_html = content

    if text_plain:
        return text_plain.strip()
    if text_html:
        return _extract_text_from_html(text_html)
    return ""
