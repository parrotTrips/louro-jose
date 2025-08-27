# email_labeling/label_quotes_messages.py
"""
Rotular SOMENTE mensagens (não a conversa inteira) que parecem "cotação" com o rótulo QUOTES.
[... docstring original mantida ...]
"""

from __future__ import annotations

import os
import re
import json
import argparse
import unicodedata
from typing import Dict, List, Tuple

# --- bootstrap: garantir que a raiz do repo está no sys.path ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]      # .../louro-jose
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from googleapiclient.errors import HttpError
# agora este import funciona independente do CWD
from modules.login_gmail import create_login

# =========================
# Configuração (ajustável)
# =========================

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]

# Resolvidos RELATIVOS À RAIZ do repo (não ao CWD)
CREDENTIALS_PATH = os.getenv(
    "GMAIL_OAUTH_CLIENT",
    str(ROOT / "credentials" / "real-credentials-parrots-gmail.json")
)
TOKEN_DIR = os.getenv("GMAIL_TOKEN_DIR", str(ROOT / "token_files"))
TOKEN_PATH = str(Path(TOKEN_DIR) / "token_gmail_v1.json")

DEFAULT_QUERY = "newer_than:60d in:anywhere"
DEFAULT_LABEL = os.getenv("QUOTES_LABEL_NAME", "QUOTES")

# Heurísticas (palavras, padrões, limiar)
KEYWORDS = [
    "cotacao", "orçamento", "orcamento", "quote", "quotation", "proposal",
    "tarifa", "tarifas", "diaria", "diárias", "diarias", "disponibilidade",
    "sgl", "dbl", "twin", "triplo", "standard", "luxo", "superior",
    "iss", "net", "comissionada", "não reembolsável", "nao reembolsavel",
    "formas de pagamento", "pré pagamento", "pre pagamento", "bloqueio", "apartamentos",
    "categoria", "frente mar", "vista", "café da manhã", "cafe da manha"
]
CURRENCY_RE = re.compile(r"(?:R\$\s?|\bBRL\b|\bUSD\b|\$\s?)\d{1,3}(?:[\.\,]\d{3})*(?:[\.\,]\d{2})?")
PERCENT_RE = re.compile(r"\b\d{1,2}\s?%")
DATE_HINT_RE = re.compile(
    r"\b(?:check[-\s]?in|check[-\s]?out|diaria|diárias|noite|noites|"
    r"jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez|"
    r"[0-3]?\d/[0-1]?\d(?:/\d{2,4})?)\b", re.I
)
HEURISTIC_THRESHOLD = 3  # pontuação mínima para considerar "cotação"

# =========================
# Auxiliares de sistema
# =========================

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s

# =========================
# Gmail: labels
# =========================

def get_or_create_label_id(service, label_name: str) -> str:
    resp = service.users().labels().list(userId="me").execute()
    for lb in resp.get("labels", []):
        if lb.get("name") == label_name:
            return lb["id"]
    body = {"name": label_name, "labelListVisibility": "labelShow", "messageListVisibility": "show"}
    created = service.users().labels().create(userId="me", body=body).execute()
    return created["id"]

# =========================
# Gmail: busca/extração
# =========================

def search_thread_ids(service, q: str) -> List[str]:
    out, page_token = [], None
    while True:
        resp = service.users().threads().list(
            userId="me", q=q, pageToken=page_token, maxResults=200
        ).execute()
        out.extend([t["id"] for t in resp.get("threads", [])])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return out

def _decode_b64url(data: str) -> bytes:
    import base64 as _b64
    return _b64.urlsafe_b64decode(data.encode("utf-8"))

def _flatten_payload(payload: Dict) -> List[Tuple[str, bytes]]:
    results: List[Tuple[str, bytes]] = []
    if not payload:
        return results
    mime = payload.get("mimeType", "")
    body = payload.get("body", {})
    data = body.get("data")
    parts = payload.get("parts")
    if data:
        results.append((mime, _decode_b64url(data)))
    if parts:
        for p in parts:
            results.extend(_flatten_payload(p))
    return results

def _strip_html(html: str) -> str:
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    html = html.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return re.sub(r"\s+", " ", html).strip()

def get_plain_text_from_message(msg: Dict) -> str:
    payload = msg.get("payload", {})
    parts = _flatten_payload(payload)
    if not parts and payload.get("body", {}).get("data"):
        parts = [(payload.get("mimeType", "text/plain"), _decode_b64url(payload["body"]["data"]))]
    texts: List[str] = []
    for mime, data_bytes in parts:
        if not data_bytes:
            continue
        try:
            raw = data_bytes.decode(errors="ignore")
            if mime.startswith("text/plain"):
                texts.append(raw)
            elif mime.startswith("text/html"):
                texts.append(_strip_html(raw))
        except Exception:
            continue
    return "\n".join(t for t in texts if t).strip()

def get_header(msg: Dict, name: str) -> str:
    for h in msg.get("payload", {}).get("headers", []):
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""

# =========================
# Heurística de "cotação"
# =========================

def looks_like_quote(subject: str, body: str, threshold: int = HEURISTIC_THRESHOLD) -> bool:
    subj_n = _normalize_text(subject)
    body_n = _normalize_text(body)
    text = subj_n + "\n" + body_n

    score = 0
    if CURRENCY_RE.search(text):
        score += 2
    if PERCENT_RE.search(text):
        score += 1
    if DATE_HINT_RE.search(text):
        score += 1

    kw_hits = sum(1 for kw in KEYWORDS if kw in text)
    score += min(kw_hits, 3)

    if not CURRENCY_RE.search(text):
        if body.count("?") >= 3:
            score -= 1

    return score >= threshold

# =========================
# Aplicar rótulo por mensagem
# =========================

def message_has_label(msg: Dict, label_id: str) -> bool:
    return label_id in set(msg.get("labelIds", []))

def add_label_to_message(service, msg_id: str, label_id: str) -> None:
    service.users().messages().modify(
        userId="me", id=msg_id, body={"addLabelIds": [label_id], "removeLabelIds": []}
    ).execute()

# =========================
# Pipeline principal
# =========================

def process_threads(service, q: str, label_id: str) -> Dict[str, int]:
    stats = {"threads": 0, "messages": 0, "labeled": 0}
    for th_id in search_thread_ids(service, q):
        stats["threads"] += 1
        thread = service.users().threads().get(userId="me", id=th_id, format="full").execute()
        for msg in thread.get("messages", []):
            stats["messages"] += 1
            if message_has_label(msg, label_id):
                continue
            subject = get_header(msg, "Subject")
            body_text = get_plain_text_from_message(msg)
            if looks_like_quote(subject, body_text):
                add_label_to_message(service, msg["id"], label_id)
                stats["labeled"] += 1
    return stats

# =========================
# CLI
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Rotula apenas mensagens de cotação com o rótulo especificado (padrão: QUOTES)."
    )
    parser.add_argument("--q", type=str, default=DEFAULT_QUERY,
                        help=f"Consulta Gmail para filtrar threads (padrão: '{DEFAULT_QUERY}').")
    parser.add_argument("--label", type=str, default=DEFAULT_LABEL,
                        help=f"Nome do rótulo a aplicar (padrão: '{DEFAULT_LABEL}').")
    args = parser.parse_args()

    try:
        _ensure_dir(TOKEN_DIR)   # <-- corrigido (garante a pasta, não o arquivo)
        service = create_login(
            credentials_path=CREDENTIALS_PATH,
            token_path=TOKEN_PATH,
            scopes=SCOPES,
        )
        label_id = get_or_create_label_id(service, args.label)
        stats = process_threads(service, args.q, label_id)

        print("✅ Finalizado.")
        print(f"• Threads analisados : {stats['threads']}")
        print(f"• Mensagens analisadas: {stats['messages']}")
        print(f"• Mensagens rotuladas : {stats['labeled']}")
        print(f"→ Rótulo aplicado por MENSAGEM: {args.label}")

    except HttpError as e:
        print(f"❌ Erro Gmail API: {e}")
    except FileNotFoundError as e:
        print(f"❌ Arquivo não encontrado: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")

if __name__ == "__main__":
    main()
