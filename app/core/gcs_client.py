import json
import logging
from typing import Any, List, Optional

from google.cloud import storage

from app.core.config import settings

logger = logging.getLogger(__name__)


class GCSClient:
    def __init__(self, service_account_file: str, bucket_name: str) -> None:
        self._service_account_file = service_account_file
        self._bucket_name = bucket_name
        self._client: Optional[storage.Client] = None

    @property
    def _bucket(self) -> storage.Bucket:
        if self._client is None:
            self._client = storage.Client.from_service_account_json(
                self._service_account_file
            )
        return self._client.bucket(self._bucket_name)

    def upload_json(self, path: str, data: Any) -> None:
        blob = self._bucket.blob(path)
        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False, indent=2),
            content_type="application/json",
        )
        logger.info("GCS upload OK → %s", path)

    def download_json(self, path: str) -> Any:
        blob = self._bucket.blob(path)
        if not blob.exists():
            return None
        return json.loads(blob.download_as_string())

    def list_blobs(self, prefix: str) -> List[str]:
        return [b.name for b in self._bucket.list_blobs(prefix=prefix)]


def make_gcs_client() -> GCSClient:
    return GCSClient(settings.SERVICE_ACCOUNT_FILE, settings.GCS_BUCKET)
