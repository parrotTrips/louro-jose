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

    # --- Google Sheets ---
    SHEET_ID = os.getenv("SHEET_ID")

    # --- LLM (OpenRouter) ---
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
    OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL")
    OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL")

    # --- Service Account ---
    SERVICE_ACCOUNT_FILE = "credentials/service-account.json"

settings = Settings()
