from __future__ import annotations
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]

BASE_DIR = Path(__file__).resolve().parent.parent  # raiz do projeto
CLIENT_SECRET_FILE = BASE_DIR / "credentials" / "gmail_client_secret.json"
TOKEN_FILE = BASE_DIR / "credentials" / "gmail-token.json"


def main():
    if not CLIENT_SECRET_FILE.exists():
        raise FileNotFoundError(f"Client secret não encontrado em {CLIENT_SECRET_FILE}")

    flow = InstalledAppFlow.from_client_secrets_file(
        str(CLIENT_SECRET_FILE),
        scopes=SCOPES,
    )

    # Abre o navegador pra você logar e aceitar os escopos
    creds = flow.run_local_server(port=0)

    # Salva no formato esperado por Credentials.from_authorized_user_file
    TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")
    print(f"Token salvo em: {TOKEN_FILE}")


if __name__ == "__main__":
    main()
