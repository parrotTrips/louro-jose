#!/usr/bin/env python3
"""
Configura filtros Gmail para rotular emails de cotação com QUOTES.

Etapa 1: Garante que o label QUOTES existe.
Etapa 2: Cria filtros Gmail (aplicados em emails futuros).
Etapa 3: Varredura retroativa — aplica QUOTES em emails existentes que batem nas keywords.

Uso:
    python scripts/setup_gmail_filters.py
    python scripts/setup_gmail_filters.py --lookback-days 365
"""
import argparse
import datetime
import logging
import sys

from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]
TOKEN_FILE = "credentials/gmail-token.json"
LABEL_NAME = "QUOTES"

# Keywords para filtro por assunto
SUBJECT_KEYWORDS = [
    "cotação", "cotacao",
    "tarifa",
    "diária", "diaria",
    "proposta",
    "disponibilidade",
    "hospedagem",
    "reserva",
]

# Frases para filtro por corpo
BODY_PHRASES = [
    "cotação hotel", "cotacao hotel",
    "tarifa hotel",
    "proposta comercial",
    "diária hotel", "diaria hotel",
]

SUBJECT_QUERY = "subject:(" + " OR ".join(SUBJECT_KEYWORDS) + ")"
BODY_QUERY = " OR ".join(f'"{phrase}"' for phrase in BODY_PHRASES)
COMBINED_QUERY = f"({SUBJECT_QUERY}) OR ({BODY_QUERY})"


def get_or_create_label(service, label_name: str) -> str:
    resp = service.users().labels().list(userId="me").execute()
    for label in resp.get("labels", []):
        if label["name"].lower() == label_name.lower():
            logger.info("Label '%s' já existe (id=%s).", label_name, label["id"])
            return label["id"]
    body = {
        "name": label_name,
        "labelListVisibility": "labelShow",
        "messageListVisibility": "show",
    }
    created = service.users().labels().create(userId="me", body=body).execute()
    logger.info("Label '%s' criado (id=%s).", label_name, created["id"])
    return created["id"]


def create_filters(service, label_id: str) -> None:
    existing_resp = service.users().settings().filters().list(userId="me").execute()
    existing_queries = {
        f.get("criteria", {}).get("query", "")
        for f in existing_resp.get("filter", [])
    }

    filters_to_create = [
        {"criteria": {"query": SUBJECT_QUERY}, "action": {"addLabelIds": [label_id]}},
        {"criteria": {"query": BODY_QUERY}, "action": {"addLabelIds": [label_id]}},
    ]

    for body in filters_to_create:
        query = body["criteria"]["query"]
        if query in existing_queries:
            logger.info("Filtro já existe para: %.60s...", query)
            continue
        result = service.users().settings().filters().create(
            userId="me", body=body
        ).execute()
        logger.info("Filtro criado (id=%s): %.60s...", result.get("id"), query)


def apply_label_retroactively(service, label_id: str, lookback_days: int) -> int:
    since = (
        datetime.date.today() - datetime.timedelta(days=lookback_days)
    ).strftime("%Y/%m/%d")
    query = f"({COMBINED_QUERY}) after:{since} -label:{LABEL_NAME}"
    logger.info("Buscando emails sem '%s': %s", LABEL_NAME, query)

    messages = []
    page_token = None
    while True:
        resp = service.users().messages().list(
            userId="me", q=query, pageToken=page_token
        ).execute()
        messages.extend(resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    total = len(messages)
    logger.info("%d emails encontrados para rotular.", total)

    for i, msg in enumerate(messages, 1):
        service.users().messages().modify(
            userId="me",
            id=msg["id"],
            body={"addLabelIds": [label_id]},
        ).execute()
        if i % 20 == 0 or i == total:
            logger.info("Progresso: %d/%d", i, total)

    return total


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Configura filtros Gmail para o pipeline Louro José."
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=180,
        help="Quantos dias retroativos varrer para aplicar QUOTES (padrão: 180).",
    )
    args = parser.parse_args()

    try:
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, scopes=GMAIL_SCOPES)
    except FileNotFoundError:
        logger.error(
            "Token não encontrado em %s. Execute: python scripts/generate_gmail_token.py",
            TOKEN_FILE,
        )
        sys.exit(1)

    service = build("gmail", "v1", credentials=creds)

    logger.info("=== Etapa 1: Garantindo label '%s' ===", LABEL_NAME)
    label_id = get_or_create_label(service, LABEL_NAME)

    logger.info("=== Etapa 2: Criando filtros Gmail ===")
    create_filters(service, label_id)

    logger.info("=== Etapa 3: Varredura retroativa (%d dias) ===", args.lookback_days)
    count = apply_label_retroactively(service, label_id, args.lookback_days)

    logger.info("=== Setup concluído: %d emails rotulados retroativamente ===", count)


if __name__ == "__main__":
    main()
