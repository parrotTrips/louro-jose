# app/core/gmail_client.py

from __future__ import annotations
import base64
from email import message_from_bytes
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from app.core.config import settings


class GmailClient:
    def __init__(self):
        """
        Inicializa o cliente Gmail utilizando o token OAuth salvo localmente.
        Esse token já está autorizado para acessar o Gmail da Parrot Trips.
        """
        creds = Credentials.from_authorized_user_file(
            settings.GMAIL_TOKEN_FILE,
            scopes=[
                "https://www.googleapis.com/auth/gmail.readonly",
                "https://www.googleapis.com/auth/gmail.modify",
            ],
        )
        self.service = build("gmail", "v1", credentials=creds)

    # ------------------------------------------------------------
    # LABELS
    # ------------------------------------------------------------
    def get_or_create_label(self, label_name: str) -> str:
        """
        Verifica se o label existe.
        Se não existir, cria.
        Retorna o ID do label.
        """
        labels = (
            self.service.users()
            .labels()
            .list(userId="me")
            .execute()
            .get("labels", [])
        )

        for l in labels:
            if l["name"].lower() == label_name.lower():
                return l["id"]

        # Criar label se não existir
        body = {
            "name": label_name,
            "labelListVisibility": "labelShow",
            "messageListVisibility": "show",
        }
        created = (
            self.service.users()
            .labels()
            .create(userId="me", body=body)
            .execute()
        )
        return created["id"]

    # ------------------------------------------------------------
    # MENSAGENS
    # ------------------------------------------------------------
    def search_messages(self, query: str):
        """
        Busca mensagens no Gmail usando a query (ex: 'newer_than:50d').
        Faz paginação até acabar.
        Retorna uma lista de dicts com pelo menos {"id": ...}.
        """
        messages = []
        page_token = None

        while True:
            resp = (
                self.service.users()
                .messages()
                .list(userId="me", q=query, pageToken=page_token)
                .execute()
            )
            msgs = resp.get("messages", [])
            messages.extend(msgs)

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        return messages

    def get_message(self, message_id: str, fmt: str = "full"):
        """
        Retorna os dados de uma mensagem específica.
        """
        return (
            self.service.users()
            .messages()
            .get(userId="me", id=message_id, format=fmt)
            .execute()
        )

    def add_label_to_message(self, message_id: str, label_id: str):
        """
        Adiciona um label a uma mensagem.
        """
        (
            self.service.users()
            .messages()
            .modify(
                userId="me",
                id=message_id,
                body={"addLabelIds": [label_id]},
            )
            .execute()
        )

    # ------------------------------------------------------------
    # THREADS
    # ------------------------------------------------------------
    def list_threads_with_label(self, label_id: str):
        """
        Lista todas as threads que possuem o label especificado.
        Faz apenas uma página (para simplificar; se precisar, paginamos depois).
        """
        resp = (
            self.service.users()
            .threads()
            .list(userId="me", labelIds=[label_id])
            .execute()
        )

        return resp.get("threads", [])

    def get_thread(self, thread_id: str):
        """
        Retorna todos os emails de uma thread completa.
        """
        return (
            self.service.users()
            .threads()
            .get(userId="me", id=thread_id, format="full")
            .execute()
        )

    # ------------------------------------------------------------
    # EMAIL DECODER
    # ------------------------------------------------------------
    def decode_email(self, message):
        """
        Decodifica o corpo da mensagem (base64 → email.message.EmailMessage).
        Tenta pegar o primeiro 'part'; se não tiver, pega o body direto.
        """
        try:
            data = message["payload"]["parts"][0]["body"]["data"]
        except Exception:
            data = message["payload"]["body"]["data"]

        decoded = base64.urlsafe_b64decode(data)
        return message_from_bytes(decoded)


gmail_client = GmailClient()
