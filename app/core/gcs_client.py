# app/core/gcs_client.py

import json
from google.cloud import storage
from app.core.config import settings


class GCSClient:
    def __init__(self):
        """
        Inicializa o cliente do Google Cloud Storage usando a
        service account salva localmente.
        """
        self.client = storage.Client.from_service_account_json(
            settings.SERVICE_ACCOUNT_FILE
        )
        self.bucket = self.client.bucket(settings.GCS_BUCKET)

    # ------------------------------------------------------------
    # UPLOAD JSON
    # ------------------------------------------------------------
    def upload_json(self, path: str, data: dict):
        """
        Salva um dicionário JSON no bucket no caminho especificado.
        """
        blob = self.bucket.blob(path)
        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False, indent=2),
            content_type="application/json"
        )
        print(f"[GCS] Upload OK → {path}")

    # ------------------------------------------------------------
    # DOWNLOAD JSON
    # ------------------------------------------------------------
    def download_json(self, path: str):
        """
        Lê um JSON do bucket. Se não existir, retorna None.
        """
        blob = self.bucket.blob(path)
        if not blob.exists():
            return None

        content = blob.download_as_string()
        return json.loads(content)


# Instância global
gcs_client = GCSClient()
