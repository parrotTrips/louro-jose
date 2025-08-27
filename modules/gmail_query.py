from __future__ import annotations
from typing import List, Dict, Optional, Any
from datetime import datetime
from dateutil import tz

from googleapiclient.discovery import Resource

from modules.mime import get_header, extract_prefer_plaintext

TZ_SAO_PAULO = tz.gettz("America/Sao_Paulo")

def find_label_id(service: Resource, label_name: str) -> Optional[str]:
    resp = service.users().labels().list(userId="me").execute()
    for l in resp.get("labels", []):
        if l.get("name") == label_name:
            return l.get("id")
    return None

def list_messages(
    service: Resource,
    label_ids: Optional[List[str]] = None,
    query: Optional[str] = None,
    max_results: Optional[int] = None,
) -> List[Dict[str, str]]:
    """Retorna dicionários com {id, threadId} das mensagens."""
    userId = "me"
    label_ids = label_ids or []
    msgs: List[Dict[str, str]] = []
    page_token = None
    fetched = 0

    while True:
        req = service.users().messages().list(
            userId=userId,
            labelIds=label_ids or None,
            q=query or None,
            pageToken=page_token,
            maxResults=min(max_results - fetched, 500) if max_results else 500,
        )
        resp = req.execute()
        msgs_batch = resp.get("messages", [])
        msgs.extend(msgs_batch)
        fetched += len(msgs_batch)
        page_token = resp.get("nextPageToken")
        if not page_token or (max_results and fetched >= max_results):
            break
    return msgs

def get_thread(service: Resource, thread_id: str) -> Dict[str, Any]:
    return service.users().threads().get(userId="me", id=thread_id, format="full").execute()

def _iso_from_internal_date(internal_ms: str) -> str:
    """Converte internalDate (ms since epoch) em ISO local São Paulo."""
    ts_ms = int(internal_ms or "0")
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=TZ_SAO_PAULO)
    return dt.isoformat(timespec="seconds")

def simplify_message(m: Dict[str, Any]) -> Dict[str, str]:
    headers = (m.get("payload", {}) or {}).get("headers", [])
    sender = get_header(headers, "From") or ""
    recipient = get_header(headers, "To") or ""
    subject = get_header(headers, "Subject") or ""
    body = extract_prefer_plaintext(m.get("payload", {}) or {})
    ts_iso = _iso_from_internal_date(m.get("internalDate", "0"))
    return {
        "timestamp": ts_iso,
        "sender": sender,
        "recipient": recipient,
        "subject": subject,
        "body": body,
    }

def build_gmail_query(q: Optional[str], after: Optional[str], before: Optional[str]) -> Optional[str]:
    """
    Monta string de busca do Gmail. Datas no formato YYYY/MM/DD.
    Exemplos válidos:
      q="from:foo@bar.com has:attachment"
      after="2025/08/01" before="2025/08/13"
    """
    parts: List[str] = []
    if q:
        parts.append(q.strip())
    if after:
        parts.append(f"after:{after}")
    if before:
        parts.append(f"before:{before}")
    return " ".join(parts) if parts else None

def unique_thread_ids(msgs: List[Dict[str, str]]) -> List[str]:
    """Deduplica por threadId preservando a ordem de chegada."""
    seen = set()
    order = []
    for m in msgs:
        tid = m.get("threadId")
        if tid and tid not in seen:
            seen.add(tid)
            order.append(tid)
    return order
