from __future__ import annotations
import os
from pathlib import Path
from typing import Sequence, Optional

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

DEFAULT_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

def create_login(
    credentials_path: str = "credentials/real-credentials-parrots-gmail.json",
    token_path: str = "token_files/token_gmail_v1.json",
    scopes: Optional[Sequence[str]] = None,
):
    """Cria e retorna o serviço Gmail v1 autenticado via OAuth."""
    scopes = list(scopes or DEFAULT_SCOPES)
    Path(os.path.dirname(token_path) or ".").mkdir(parents=True, exist_ok=True)

    creds: Optional[Credentials] = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, scopes)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as token:
            token.write(creds.to_json())

    service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    return service
