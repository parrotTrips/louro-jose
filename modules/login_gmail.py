# modules/login_gmail.py
from __future__ import annotations
import os
import json
from pathlib import Path
from typing import Sequence, Optional

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build

# Dar permissão de leitura+escrita (rotular, arquivar etc.)
DEFAULT_SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]

def _granted_scopes_from_file(token_path: str) -> set[str]:
    """Lê o token e retorna o set de escopos realmente concedidos."""
    try:
        with open(token_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        scopes = data.get("scopes", [])
        if isinstance(scopes, str):
            scopes = [s.strip() for s in scopes.split()]
        return set(scopes or [])
    except Exception:
        return set()

def create_login(
    credentials_path: str = "../credentials/real-credentials-parrots-gmail.json",
    token_path: str = "../token_files/token_gmail_v1.json",
    scopes: Optional[Sequence[str]] = None,
):
    """
    Cria e retorna o serviço Gmail v1 autenticado via OAuth.
    - Usa gmail.modify por padrão (superset de readonly).
    - Se o token existente não tiver os escopos pedidos, força reconsent.
    """
    scopes = list(scopes or DEFAULT_SCOPES)
    Path(os.path.dirname(token_path) or ".").mkdir(parents=True, exist_ok=True)

    need_reconsent = False
    creds: Optional[Credentials] = None

    if os.path.exists(token_path):
        # Verifica se o token realmente possui os escopos necessários
        granted = _granted_scopes_from_file(token_path)
        if not set(scopes).issubset(granted):
            need_reconsent = True
        # Carrega as credenciais a partir do arquivo
        with open(token_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        creds = Credentials.from_authorized_user_info(data, scopes=scopes)

    if not need_reconsent:
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except RefreshError as e:
                    # Ex.: invalid_scope -> precisa reconsentir
                    if "invalid_scope" in str(e):
                        need_reconsent = True
                    else:
                        raise
            else:
                need_reconsent = True

    if need_reconsent:
        flow = InstalledAppFlow.from_client_secrets_file(credentials_path, scopes=scopes)
        # prompt="consent" garante upgrade de escopo quando necessário
        creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")
        with open(token_path, "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    return service
