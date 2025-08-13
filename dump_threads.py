from __future__ import annotations
import argparse
import json
import re
from pathlib import Path

from login_gmail import create_login
from utils.gmail_query import (
    find_label_id, list_messages, get_thread, simplify_message,
    build_gmail_query, unique_thread_ids
)

DEFAULT_CREDENTIALS = "credentials/real-credentials-parrots-gmail.json"
DEFAULT_TOKEN = "token_files/token_gmail_v1.json"
DEFAULT_OUTDIR = "raw_messages"

def _sanitize(s: str) -> str:
    s = s.strip().replace(" ", "_")
    s = re.sub(r"[^a-zA-Z0-9_\-\.@]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s[:120].strip("_")

def _name_from_sender(sender: str) -> str:
    # Extrai "Nome" e "email" se vier no formato "Nome <email>"
    m = re.match(r"(?:(.*?)\s*)?<([^>]+)>", sender)
    if m:
        name = _sanitize(m.group(1) or "Unknown")
        email = _sanitize(m.group(2))
        return f"{name}_{email}"
    return _sanitize(sender or "Unknown")

def _prefix_from_first_email(email: dict) -> str:
    # timestamp: YYYYMMDD_HHMM
    ts = email.get("timestamp", "")
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})", ts)
    if not m:
        return "00000000_0000"
    return f"{m.group(1)}{m.group(2)}{m.group(3)}_{m.group(4)}{m.group(5)}"

def dump_threads(
    label: str | None,
    q: str | None,
    after: str | None,
    before: str | None,
    max_results: int | None,
    outdir: str = DEFAULT_OUTDIR,
):
    Path(outdir).mkdir(parents=True, exist_ok=True)

    service = create_login(
        credentials_path=DEFAULT_CREDENTIALS,
        token_path=DEFAULT_TOKEN,
    )

    label_id = None
    if label:
        label_id = find_label_id(service, label)
        if not label_id:
            raise SystemExit(f"❌ Label '{label}' não encontrado na conta.")

    query = build_gmail_query(q=q, after=after, before=before)
    msgs = list_messages(service, label_ids=[label_id] if label_id else None, query=query, max_results=max_results)

    thread_ids = unique_thread_ids(msgs)
    print(f"🧵 Threads únicas encontradas: {len(thread_ids)}")

    saved = 0
    for tid in thread_ids:
        t = get_thread(service, tid)
        messages = t.get("messages", [])
        emails = [simplify_message(m) for m in messages]
        emails.sort(key=lambda e: e.get("timestamp", ""))  # ordem cronológica

        data = {
            "thread_id": tid,
            "label": label or "",
            "message_count": len(emails),
            "emails": emails,
        }

        first = emails[0] if emails else {}
        sender_key = _name_from_sender(first.get("sender", ""))
        subject_key = _sanitize(first.get("subject", "") or "Sem_assunto")
        prefix = _prefix_from_first_email(first)
        fname = f"{prefix}__{sender_key}__{subject_key}.json"

        path = Path(outdir) / fname
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        saved += 1

    print(f"✅ {saved} arquivo(s) salvo(s) em '{outdir}'")

def parse_args():
    p = argparse.ArgumentParser(description="Baixa threads do Gmail para JSON (um arquivo por thread).")
    p.add_argument("--label", help="Nome do rótulo (ex.: COMPLETE_DATA)", default="COMPLETE_DATA")
    p.add_argument("--q", help="Consulta Gmail (ex.: 'from:foo@bar.com has:attachment')", default=None)
    p.add_argument("--after", help="Data inicial no formato YYYY/MM/DD", default=None)
    p.add_argument("--before", help="Data final no formato YYYY/MM/DD", default=None)
    p.add_argument("--max", type=int, help="Máximo de mensagens para varrer (não threads).", default=500)
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    dump_threads(
        label=args.label,
        q=args.q,
        after=args.after,
        before=args.before,
        max_results=args.max,
    )
