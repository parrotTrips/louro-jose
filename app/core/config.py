import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    GCS_BUCKET = os.getenv("GCS_BUCKET")
    GMAIL_LABEL = os.getenv("GMAIL_LABEL", "QUOTES")
    GMAIL_TOKEN_FILE = os.getenv("GMAIL_TOKEN_FILE", "credentials/gmail-token.json")
    SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "credentials/service-account.json")
    SHEETS_SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")
    SHEETS_QUOTE_SHEET_NAME = os.getenv("SHEETS_QUOTE_SHEET_NAME", "quotes_raw")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")


settings = Settings()
