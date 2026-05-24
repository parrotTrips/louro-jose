import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

STATE_PATH = "state/threads_state.json"


class ThreadsState:
    def __init__(self, gcs_client: Any) -> None:
        self._gcs = gcs_client
        self._data: Dict[str, Any] = gcs_client.download_json(STATE_PATH) or {}

    def get_last_email(self, thread_id: str) -> Optional[str]:
        return self._data.get(thread_id, {}).get("last_email_id")

    def update_thread(self, thread_id: str, last_email_id: str) -> None:
        self._data[thread_id] = {"last_email_id": last_email_id}
        self._gcs.upload_json(STATE_PATH, self._data)
        logger.debug("State: thread %s → last_email_id %s", thread_id, last_email_id)
