# app/core/config.py

import os
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

class Settings:
    # --- Projeto GCP ---
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    GCS_BUCKET = os.getenv("GCS_BUCKET")

    # --- Gmail ---
    GMAIL_LABEL = os.getenv("GMAIL_LABEL", "QUOTES")
    GMAIL_TOKEN_FILE = "credentials/gmail-token.json"

    # --- Google Sheets (genérico antigo, se estiver em uso em outro lugar) ---
    SHEET_ID = os.getenv("SHEET_ID")

    # --- Google Sheets (para o louro-jose / quotes_raw) ---
    # ID da planilha (aquele do link)
    SHEETS_SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")
    # Nome da guia onde vamos jogar as cotações
    SHEETS_QUOTE_SHEET_NAME = os.getenv("SHEETS_QUOTE_SHEET_NAME", "quotes_raw")

    # --- LLM (OpenRouter) ---
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
    OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL")
    OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL")

    # --- Service Account ---
    SERVICE_ACCOUNT_FILE = "credentials/service-account.json"

settings = Settings()
