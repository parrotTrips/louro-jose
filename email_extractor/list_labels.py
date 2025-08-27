from __future__ import annotations
import sys
from dotenv import load_dotenv

sys.path.append("..")
load_dotenv("../.env")

from modules.login_gmail import create_login

DEFAULT_CREDENTIALS = "../credentials/real-credentials-parrots-gmail.json"
DEFAULT_TOKEN = "../token_files/token_gmail_v1.json"

def main():
    service = create_login(
        credentials_path=DEFAULT_CREDENTIALS,
        token_path=DEFAULT_TOKEN,
    )
    labels = service.users().labels().list(userId="me").execute().get("labels", [])
    print("📬 Rótulos encontrados:")
    for l in labels:
        print(" -", l.get("name"))

if __name__ == "__main__":
    main()
