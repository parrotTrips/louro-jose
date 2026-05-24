import logging
from typing import Any, Dict, List

import gspread

from app.core.config import settings
from app.core.gcs_client import make_gcs_client
from app.agents.extractor_agent import HEADER_FIELDS

logger = logging.getLogger(__name__)

EXTRA_FIELDS = ["_thread_id", "_row_index_in_thread", "_key"]
ALL_FIELDS = HEADER_FIELDS + EXTRA_FIELDS


class SheetsSyncAgent:
    def run(self) -> None:
        gcs = make_gcs_client()

        data = gcs.download_json("tables/quotes_raw.json")
        if not data:
            logger.info("quotes_raw.json vazio ou ausente, nada a sincronizar.")
            return

        if not isinstance(data, list):
            logger.error("Formato inesperado em quotes_raw.json (esperado: lista).")
            return

        quotes: List[Dict[str, Any]] = data
        logger.info("%d linhas lidas de quotes_raw.json.", len(quotes))

        gc = gspread.service_account(
            filename=settings.SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        sh = gc.open_by_key(settings.SHEETS_SPREADSHEET_ID)

        try:
            ws = sh.worksheet(settings.SHEETS_QUOTE_SHEET_NAME)
            logger.info("Aba '%s' encontrada.", settings.SHEETS_QUOTE_SHEET_NAME)
        except gspread.WorksheetNotFound:
            logger.info("Aba '%s' não encontrada, criando...", settings.SHEETS_QUOTE_SHEET_NAME)
            ws = sh.add_worksheet(
                title=settings.SHEETS_QUOTE_SHEET_NAME,
                rows="1000",
                cols=str(len(ALL_FIELDS) + 5),
            )

        if not ws.row_values(1):
            ws.update("A1", [ALL_FIELDS])
            logger.info("Cabeçalho escrito em A1.")

        key_col_index = ALL_FIELDS.index("_key") + 1
        existing_keys = set(ws.col_values(key_col_index)[1:])
        logger.info("%d chaves existentes na planilha.", len(existing_keys))

        new_rows: List[List[Any]] = []
        for q in quotes:
            key = q.get("_key")
            if not key or key in existing_keys:
                continue
            new_rows.append([q.get(field, "") for field in ALL_FIELDS])
            existing_keys.add(key)

        if not new_rows:
            logger.info("Nenhuma linha nova para adicionar.")
            return

        ws.append_rows(new_rows, value_input_option="RAW")
        logger.info("%d novas linhas adicionadas à planilha.", len(new_rows))


def run_sheets_sync() -> None:
    SheetsSyncAgent().run()
