import logging

from app.agents.storage_agent import run_storage
from app.agents.extractor_agent import run_extractor
from app.agents.sheets_sync_agent import run_sheets_sync


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    logging.info("STEP 1/3 — StorageAgent (Gmail QUOTES → GCS)...")
    run_storage()
    logging.info("STEP 1/3 — OK")

    logging.info("STEP 2/3 — ExtractorAgent (GCS → LLM → tables)...")
    run_extractor()
    logging.info("STEP 2/3 — OK")

    logging.info("STEP 3/3 — SheetsSyncAgent (tables → Google Sheets)...")
    run_sheets_sync()
    logging.info("STEP 3/3 — OK")

    logging.info("Pipeline concluído com sucesso.")


if __name__ == "__main__":
    main()
