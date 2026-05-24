import base64
import logging
from email import message_from_bytes
from typing import Any, Dict, List, Optional

from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.core.config import settings

logger = logging.getLogger(__name__)

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]


class GmailClient:
    def __init__(self, token_file: str) -> None:
        creds = Credentials.from_authorized_user_file(token_file, scopes=GMAIL_SCOPES)
        self.service = build("gmail", "v1", credentials=creds)

    def _execute(self, request: Any, action: str) -> Any:
        try:
            return request.execute()
        except RefreshError as e:
            logger.error(
                "Gmail token expirado/invalidado durante '%s'. "
                "Execute: python scripts/generate_gmail_token.py\n"
                "Detalhes: %s",
                action,
                e,
            )
            raise
        except HttpError as e:
            status = getattr(e, "status_code", None) or (
                e.resp.status if e.resp else None
            )
            if status in (401, 403):
                logger.error(
                    "Erro de autenticação no Gmail durante '%s' (HTTP %s).",
                    action,
                    status,
                )
            else:
                logger.exception("Erro no Gmail API durante '%s'.", action)
            raise

    # --- Labels ---

    def get_or_create_label(self, label_name: str) -> str:
        resp = self._execute(
            self.service.users().labels().list(userId="me"),
            "listar labels",
        )
        for label in resp.get("labels", []):
            if label["name"].lower() == label_name.lower():
                return label["id"]
        body = {
            "name": label_name,
            "labelListVisibility": "labelShow",
            "messageListVisibility": "show",
        }
        created = self._execute(
            self.service.users().labels().create(userId="me", body=body),
            "criar label",
        )
        return created["id"]

    # --- Threads ---

    def list_threads_with_label(self, label_id: str) -> List[Dict[str, Any]]:
        threads: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            resp = self._execute(
                self.service.users().threads().list(
                    userId="me",
                    labelIds=[label_id],
                    pageToken=page_token,
                ),
                "listar threads com label",
            )
            threads.extend(resp.get("threads", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return threads

    def get_thread(self, thread_id: str) -> Dict[str, Any]:
        return self._execute(
            self.service.users().threads().get(
                userId="me", id=thread_id, format="full"
            ),
            "obter thread",
        )

    # --- Messages ---

    def search_messages(self, query: str) -> List[Dict[str, Any]]:
        messages: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            resp = self._execute(
                self.service.users().messages().list(
                    userId="me", q=query, pageToken=page_token
                ),
                "buscar mensagens",
            )
            messages.extend(resp.get("messages", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return messages

    def add_label_to_message(self, message_id: str, label_id: str) -> None:
        self._execute(
            self.service.users().messages().modify(
                userId="me",
                id=message_id,
                body={"addLabelIds": [label_id]},
            ),
            "adicionar label",
        )


def make_gmail_client() -> GmailClient:
    return GmailClient(settings.GMAIL_TOKEN_FILE)
