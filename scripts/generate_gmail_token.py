#!/usr/bin/env python3
"""
Gera ou renova o token OAuth do Gmail.

Pré-requisito: credentials/gmail_client_secret.json baixado do Google Cloud Console
  (APIs & Services -> Credentials -> OAuth 2.0 Client IDs -> Download JSON)

Uso:
    python scripts/generate_gmail_token.py
"""
import json
import os
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]
CLIENT_SECRET_FILE = "credentials/gmail_client_secret.json"
TOKEN_OUTPUT = "credentials/gmail-token.json"


def _check_testing_mode(client_secret_path: str) -> None:
    try:
        with open(client_secret_path) as f:
            data = json.load(f)
        client_type = list(data.keys())[0]
        print(
            "\n[AVISO] Verifique se o OAuth app está em modo 'In production' no Google Cloud Console."
            "\n        Apps em modo 'Testing' têm refresh tokens que expiram em 7 dias."
            "\n        Caminho: APIs & Services → OAuth consent screen → Publishing status"
        )
    except Exception:
        pass


def main() -> None:
    if not os.path.exists(CLIENT_SECRET_FILE):
        print(f"ERRO: {CLIENT_SECRET_FILE} não encontrado.")
        print(
            "Baixe em: Google Cloud Console → APIs & Services → Credentials"
            " → OAuth 2.0 Client IDs → Download JSON"
        )
        sys.exit(1)

    _check_testing_mode(CLIENT_SECRET_FILE)

    print("\nAbrindo browser para autenticação...")
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
    creds = flow.run_local_server(port=0)

    os.makedirs("credentials", exist_ok=True)
    with open(TOKEN_OUTPUT, "w") as f:
        f.write(creds.to_json())

    print(f"\nToken salvo em: {TOKEN_OUTPUT}")
    print("\nPróximo passo — atualizar no Secret Manager:")
    print(
        "  gcloud secrets versions add gmail-token"
        f" --data-file={TOKEN_OUTPUT}"
        " --project=louro-jose-479223"
    )


if __name__ == "__main__":
    main()
