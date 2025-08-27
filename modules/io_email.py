import os
import json
from datetime import datetime

def infer_timestamp_from_filename(filename: str) -> str:
    """Ex.: 20250808_1241__*.json -> '2025-08-08 12:41'"""
    base = os.path.basename(filename)
    try:
        prefix = base.split("__", 1)[0]  # 20250808_1241
        dt = datetime.strptime(prefix, "%Y%m%d_%H%M")
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""

def normalize_addr(s: str) -> str:
    return s.strip() if isinstance(s, str) else ""

def load_email_json(path: str) -> dict:
    """
    Suporta:
      - envelope com 'emails[0]'
      - JSON "flat"
    Retorna: {timestamp, subject, to, from, body, _filename}
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and isinstance(data.get("emails"), list) and data["emails"]:
        msg = data["emails"][0] or {}
        timestamp = (msg.get("timestamp") or data.get("timestamp") or infer_timestamp_from_filename(path) or "")
        subject   = msg.get("subject") or data.get("subject") or ""
        to_       = msg.get("recipient") or msg.get("to") or data.get("to") or ""
        sender    = msg.get("sender") or msg.get("from") or data.get("from") or ""
        body      = msg.get("body") or msg.get("text") or data.get("body") or ""
    else:
        timestamp = data.get("timestamp") or data.get("date") or infer_timestamp_from_filename(path) or ""
        subject   = data.get("subject") or ""
        to_       = data.get("recipient") or data.get("to") or ""
        sender    = data.get("sender") or data.get("from") or ""
        body      = data.get("body") or data.get("text") or data.get("content") or ""

    return {
        "timestamp": timestamp,
        "subject": subject,
        "to": normalize_addr(to_),
        "from": normalize_addr(sender),
        "body": body,
        "_filename": os.path.basename(path),
    }
