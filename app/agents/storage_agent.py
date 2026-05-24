import logging

from app.core.config import settings
from app.core.gmail_client import make_gmail_client
from app.core.gcs_client import make_gcs_client
from app.core.state import ThreadsState

logger = logging.getLogger(__name__)


class StorageAgent:
    def run(self) -> None:
        gmail = make_gmail_client()
        gcs = make_gcs_client()
        state = ThreadsState(gcs)

        label_id = gmail.get_or_create_label(settings.GMAIL_LABEL)
        logger.info("Label '%s' encontrado/criado (id=%s).", settings.GMAIL_LABEL, label_id)

        threads = gmail.list_threads_with_label(label_id)
        logger.info("Encontradas %d threads com '%s'.", len(threads), settings.GMAIL_LABEL)

        synced = 0
        skipped = 0

        for th in threads:
            thread_id = th["id"]
            thread_full = gmail.get_thread(thread_id)
            messages = thread_full.get("messages", [])

            if not messages:
                logger.warning("Thread %s sem mensagens, pulando.", thread_id)
                continue

            last_email_id = messages[-1]["id"]

            if state.get_last_email(thread_id) == last_email_id:
                skipped += 1
                continue

            gcs.upload_json(f"threads/{thread_id}.json", thread_full)
            state.update_thread(thread_id, last_email_id)
            synced += 1
            logger.info("Thread %s sincronizada.", thread_id)

        logger.info(
            "StorageAgent concluído: %d sincronizadas, %d sem novidades.", synced, skipped
        )


def run_storage() -> None:
    StorageAgent().run()
