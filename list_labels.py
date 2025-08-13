from __future__ import annotations
from login_gmail import create_login

def main():
    service = create_login()
    labels = service.users().labels().list(userId="me").execute().get("labels", [])
    print("📬 Rótulos encontrados:")
    for l in labels:
        print(" -", l.get("name"))

if __name__ == "__main__":
    main()
