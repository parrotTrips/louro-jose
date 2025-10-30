# src/app/gmail/incremental.py
"""
incremental.py — Coleta incremental do Gmail por janela de datas e salva em GCS.

Fluxo:
  - Constrói o serviço Gmail (OAuth local; token em .tokens/)
  - Lista mensagens por janela (after/before) e label por NOME via query (ex.: label:"QUOTES")
  - Pula message_ids já processados (state/processed_messages.json no GCS)
  - Baixa mensagem (formato 'full') e salva em raw/<threadId>/<messageId>.json
  - Marca message_id como processado

Observação: não usamos ainda a History API; usamos janela por data + filtro local.
"""

from __future__ import annotations

import os
from datetime import datetime, date, timedelta
from typing import Iterable, Optional, Dict, Any, List

from dateutil.parser import parse as dtparse

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.core.config import load_config
from app.core.state import ensure_state_initialized, has_processed, add_processed
from app.core.io_gcs import save_json_to_gcs, object_exists_in_gcs

# ----- Constantes e helpers de ambiente -----

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

GMAIL_CLIENT_SECRETS = os.getenv("GMAIL_CLIENT_SECRETS", "credentials/real-credentials-parrots-gmail.json")
GMAIL_TOKEN_FILE = os.getenv("GMAIL_TOKEN_FILE", ".tokens/gmail_token.json")
GMAIL_USER = os.getenv("GMAIL_USER", "me")
GMAIL_LABEL_DEFAULT = os.getenv("GMAIL_LABEL", "QUOTES")  # usado como label:"QUOTES" na query


def _ensure_token_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def build_gmail_service() -> Any:
    """
    Autentica via OAuth local. Na 1ª execução abre o navegador; depois reutiliza o token.
    """
    creds = None
    if os.path.exists(GMAIL_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(GMAIL_TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None
        if not creds:
            _ensure_token_dir(GMAIL_TOKEN_FILE)
            flow = InstalledAppFlow.from_client_secrets_file(GMAIL_CLIENT_SECRETS, SCOPES)
            creds = flow.run_local_server(port=0)
            with open(GMAIL_TOKEN_FILE, "w") as f:
                f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds, cache_discovery=False)


# ----- Query helpers -----

def _to_ymd(d: date | datetime | str) -> str:
    if isinstance(d, str):
        d = dtparse(d).date()
    elif isinstance(d, datetime):
        d = d.date()
    return d.strftime("%Y/%m/%d")


def build_gmail_query(
    date_from: date | datetime | str,
    date_to: date | datetime | str,
    label_name: str = "",
    extra: str = "",
) -> str:
    """
    after/before com YYYY/MM/DD e label por NOME (label:"NOME").
    before é exclusivo (before:2025/10/30 abrange até 2025-10-29).
    """
    a = _to_ymd(date_from)
    b = _to_ymd(date_to)
    parts = [f"after:{a}", f"before:{b}"]
    if label_name:
        parts.append(f'label:"{label_name}"')
    if extra:
        parts.append(extra)
    return " ".join(parts)


# ----- Listagem + download -----

def iter_message_ids(service, user_id: str, q: str, max_pages: int = 50) -> Iterable[str]:
    """
    Itera messageIds que satisfazem a query (paginação a 100 itens).
    """
    page_token = None
    pages = 0
    while True:
        try:
            req = service.users().messages().list(
                userId=user_id,
                q=q,
                pageToken=page_token,
                maxResults=100,
            )
            resp = req.execute()
        except HttpError as e:
            raise RuntimeError(f"Erro ao listar mensagens: {e}")

        for item in resp.get("messages", []):
            yield item["id"]

        page_token = resp.get("nextPageToken")
        pages += 1
        if not page_token or pages >= max_pages:
            break


def get_message_full(service, user_id: str, message_id: str) -> Dict[str, Any]:
    """
    Busca o conteúdo 'full' (com payload/parts).
    """
    try:
        return service.users().messages().get(userId=user_id, id=message_id, format="full").execute()
    except HttpError as e:
        raise RuntimeError(f"Erro ao obter mensagem {message_id}: {e}")


def raw_path(thread_id: str, message_id: str) -> str:
    return f"raw/{thread_id}/{message_id}.json"


def fetch_window_and_dump(
    bucket: str,
    date_from: date | datetime | str,
    date_to: date | datetime | str,
    label: str = GMAIL_LABEL_DEFAULT,
    extra_query: str = "",
    max_pages: int = 20,
    hard_limit: Optional[int] = None,
) -> dict:
    """
    Executa a coleta na janela, salva no GCS e retorna um resumo.
    Regras:
      - Pula IDs já processados (state).
      - Pula se objeto raw/<threadId>/<messageId>.json já existir no GCS.
      - Respeita hard_limit para smoke tests.
    """
    ensure_state_initialized(bucket)
    service = build_gmail_service()

    q = build_gmail_query(date_from, date_to, label_name=label, extra=extra_query)

    seen = 0
    skipped = 0
    skipped_exists = 0
    saved = 0
    errors = 0

    for mid in iter_message_ids(service, GMAIL_USER, q=q, max_pages=max_pages):
        if hard_limit and saved >= hard_limit:
            break

        try:
            if has_processed(bucket, mid):
                skipped += 1
                continue

            msg = get_message_full(service, GMAIL_USER, mid)

            # IDs SEMPRE vindos do Gmail:
            thread_id = msg.get("threadId")
            msg_id = msg.get("id")
            if not thread_id or not msg_id:
                raise RuntimeError(f"Mensagem sem threadId/id. mid={mid}")

            # Idempotência adicional: se já existir o RAW, pula sem baixar de novo
            dest = raw_path(thread_id, msg_id)
            if object_exists_in_gcs(bucket, dest):
                # ainda assim marca como processado, para não insistir em janelas futuras
                add_processed(bucket, msg_id)
                skipped_exists += 1
                continue

            # Salva o JSON bruto e marca processado
            save_json_to_gcs(bucket, dest, msg)
            add_processed(bucket, msg_id)

            print(f"✓ SAVED: {dest}")
            saved += 1
        except Exception as e:
            print(f"✗ ERROR mid={mid}: {e}")
            errors += 1
        finally:
            seen += 1

    return {
        "query": q,
        "label": label,
        "seen": seen,
        "saved": saved,
        "skipped_already_processed": skipped,
        "skipped_existing_raw": skipped_exists,
        "errors": errors,
    }


# ---- CLI rápido (opcional) ----

if __name__ == "__main__":
    from argparse import ArgumentParser

    cfg = load_config()
    bucket = cfg.gcs_bucket

    ap = ArgumentParser(description="Coleta incremental do Gmail (janela por data).")
    ap.add_argument("--from", dest="date_from", required=False, default=(date.today() - timedelta(days=3)).isoformat(), help="Data inicial (YYYY-MM-DD).")
    ap.add_argument("--to", dest="date_to", required=False, default=(date.today() + timedelta(days=1)).isoformat(), help="Data final exclusiva (YYYY-MM-DD).")
    ap.add_argument("--label", dest="label", required=False, default=GMAIL_LABEL_DEFAULT, help='Nome da label (ex.: QUOTES). Será usado como label:"NOME" na query.')
    ap.add_argument("--extra", dest="extra", required=False, default="", help='Query extra (ex.: \'subject:"cotação" -category:promotions\').')
    ap.add_argument("--limit", dest="limit", type=int, required=False, default=10, help="Hard limit de mensagens para salvar.")
    args = ap.parse_args()

    summary = fetch_window_and_dump(
        bucket=bucket,
        date_from=args.date_from,
        date_to=args.date_to,
        label=args.label,
        extra_query=args.extra,
        max_pages=20,
        hard_limit=args.limit,
    )
    print(summary)
