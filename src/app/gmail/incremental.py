# src/app/gmail/incremental.py
from __future__ import annotations

import os
import json
from datetime import date, timedelta
from typing import Optional, Dict, Any, Iterable, List

from dotenv import load_dotenv
load_dotenv()

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

from app.core.config import get_settings
from app.core import io_gcs
from app.core import state as state_store

# ----------------------------------
# Config / Env
# ----------------------------------
SCOPES_READONLY = ["https://www.googleapis.com/auth/gmail.readonly"]

GMAIL_CLIENT_SECRETS = os.getenv("GMAIL_CLIENT_SECRETS", "credentials/real-credentials-parrots-gmail.json")
GMAIL_TOKEN_FILE = os.getenv("GMAIL_TOKEN_FILE", "tokens/gmail_token.json")
GMAIL_USER = os.getenv("GMAIL_USER", "me")


# ----------------------------------
# Gmail Auth
# ----------------------------------
def _ensure_dir_for(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def _build_service_readonly():
    creds = None
    if os.path.exists(GMAIL_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(GMAIL_TOKEN_FILE, SCOPES_READONLY)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None
        if not creds:
            _ensure_dir_for(GMAIL_TOKEN_FILE)
            flow = InstalledAppFlow.from_client_secrets_file(GMAIL_CLIENT_SECRETS, SCOPES_READONLY)
            creds = flow.run_local_server(port=0)
            with open(GMAIL_TOKEN_FILE, "w") as f:
                f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds, cache_discovery=False)


# ----------------------------------
# Query helpers
# ----------------------------------
def _ymd(d: str) -> str:
    # Gmail aceita YYYY/MM/DD
    return d.replace("-", "/")

def _build_query(after: Optional[str], before: Optional[str], label: Optional[str]) -> str:
    parts: List[str] = []
    if label:
        parts.append(f'label:"{label}"')
    if after:
        parts.append(f"after:{_ymd(after)}")
    if before:
        parts.append(f"before:{_ymd(before)}")
    parts += ["-in:spam", "-in:trash"]
    return " ".join(parts) if parts else ""


# ----------------------------------
# Gmail fetchers
# ----------------------------------
def _list_thread_ids(service, q: str, max_pages: int = 100) -> List[str]:
    out: List[str] = []
    page_token = None
    pages = 0
    while True:
        resp = service.users().threads().list(
            userId=GMAIL_USER, q=q, pageToken=page_token, maxResults=200
        ).execute()
        out.extend([t["id"] for t in resp.get("threads", [])])
        page_token = resp.get("nextPageToken")
        pages += 1
        if not page_token or pages >= max_pages:
            break
    return out

def _get_thread_full(service, thread_id: str) -> Dict[str, Any]:
    return service.users().threads().get(userId=GMAIL_USER, id=thread_id, format="full").execute()

def _iter_messages_full(query: str) -> Iterable[Dict[str, Any]]:
    """
    Itera mensagens 'full' (dicts contendo 'id', 'threadId', 'payload', etc.)
    respeitando a consulta Gmail `query`.
    """
    service = _build_service_readonly()
    thread_ids = _list_thread_ids(service, query)
    for th_id in thread_ids:
        try:
            thread_full = _get_thread_full(service, th_id)
        except HttpError:
            continue
        for msg in thread_full.get("messages", []):
            # cada `msg` já é o raw completo desta mensagem
            yield msg


# ----------------------------------
# GCS helpers (compat com seus helpers)
# ----------------------------------
def _raw_gcs_path(thread_id: str, message_id: str) -> str:
    """
    Tenta usar seus helpers; se não existirem, cai no padrão raw/<thread>/<msg>.json
    """
    # build_raw_path(...)
    if hasattr(io_gcs, "build_raw_path"):
        return io_gcs.build_raw_path(thread_id=thread_id, message_id=message_id)
    # path_raw_message(...)
    if hasattr(io_gcs, "path_raw_message"):
        return io_gcs.path_raw_message(thread_id, message_id)  # type: ignore[attr-defined]
    # fallback
    return f"raw/{thread_id}/{message_id}.json"

def _gcs_exists(bucket: str, path: str) -> bool:
    if hasattr(io_gcs, "exists"):
        return io_gcs.exists(path)  # type: ignore[attr-defined]
    if hasattr(io_gcs, "object_exists_in_gcs"):
        return io_gcs.object_exists_in_gcs(bucket, path)  # type: ignore[attr-defined]
    raise RuntimeError("Nenhum helper de existência no GCS encontrado em io_gcs.")

def _gcs_save_json(bucket: str, path: str, obj: Dict[str, Any]) -> None:
    if hasattr(io_gcs, "save_json_to_gcs"):
        # assinatura save_json_to_gcs(bucket, path, data)
        try:
            io_gcs.save_json_to_gcs(bucket, path, obj)  # type: ignore[attr-defined]
            return
        except TypeError:
            pass
    # alguns projetos expõem save_json_to_gcs(path, data) sem bucket
    if hasattr(io_gcs, "save_json_to_gcs"):
        io_gcs.save_json_to_gcs(path, obj)  # type: ignore[misc]
        return
    raise RuntimeError("Nenhum helper de gravação JSON no GCS encontrado em io_gcs.")


# ----------------------------------
# Execução principal
# ----------------------------------
def run(
    after: Optional[str] = None,
    before: Optional[str] = None,
    label: Optional[str] = None,
) -> Dict[str, int]:
    """
    Executa a coleta incremental (janela after/before + label).
    Salva raw/<threadId>/<messageId>.json no GCS e atualiza estado.

    Returns:
        {
          'seen': X,
          'saved': Y,
          'skipped_already_processed': A,
          'skipped_existing_raw': B,
          'errors': E
        }
    """
    settings = get_settings()
    bucket = settings.gcs_bucket
    label = label or settings.gmail_label

    # garante arquivos de estado (idempotente)
    try:
        state_store.ensure_state_initialized(bucket)
    except Exception:
        # não bloqueia o resto; segue com defaults
        pass

    query = _build_query(after=after, before=before, label=label)

    # estado em memória (set de ids já processados)
    try:
        processed = set(state_store.load_processed_messages(bucket))
    except Exception:
        processed = set()

    seen = saved = skipped_proc = skipped_raw = errors = 0

    for msg in _iter_messages_full(query=query):
        seen += 1
        message_id = msg.get("id")
        thread_id = msg.get("threadId")
        if not message_id or not thread_id:
            errors += 1
            continue

        # 1) já processado?
        if message_id in processed:
            skipped_proc += 1
            continue

        # 2) já existe RAW no bucket?
        raw_path = _raw_gcs_path(thread_id, message_id)
        try:
            if _gcs_exists(bucket, raw_path):
                skipped_raw += 1
                processed.add(message_id)  # marca para evitar rechecagens
                continue
        except Exception:
            errors += 1
            continue

        # 3) salvar RAW
        try:
            _gcs_save_json(bucket, raw_path, msg)
            saved += 1
            processed.add(message_id)
        except Exception:
            errors += 1
            continue

    # persistir estado atualizado (melhor esforço)
    try:
        state_store.save_processed_messages(processed, bucket=bucket)
    except Exception:
        errors += 1

    summary = {
        "seen": seen,
        "saved": saved,
        "skipped_already_processed": skipped_proc,
        "skipped_existing_raw": skipped_raw,
        "errors": errors,
    }
    print(json.dumps(summary, ensure_ascii=False))
    return summary


# ------------------ CLI ------------------
def _parse_range_shortcut(range_expr: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Suporta --range newer_than:7d → (after, before).
    """
    if not range_expr:
        return None, None
    if range_expr.startswith("newer_than:") and range_expr.endswith("d"):
        days = int(range_expr.split(":")[1][:-1])
        before = date.today().isoformat()
        after = (date.today() - timedelta(days=days)).isoformat()
        return after, before
    return None, None

def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Coleta incremental do Gmail → GCS (raw/)")
    p.add_argument("--after", help="YYYY-MM-DD")
    p.add_argument("--before", help="YYYY-MM-DD")
    p.add_argument("--label", help='label do Gmail (default: settings.gmail_label)')
    p.add_argument("--range", help='atalho: ex. newer_than:7d')
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    after, before = args.after, args.before
    if args.range and not (after or before):
        _a, _b = _parse_range_shortcut(args.range)
        after = after or _a
        before = before or _b
    run(after=after, before=before, label=args.label)
