# app/agents/sheets_sync_agent.py

from typing import List, Dict, Any

import gspread
from google.oauth2.service_account import Credentials

from app.core.config import settings
from app.core.gcs_client import gcs_client
from app.agents.extractor_agent import HEADER_FIELDS


# Campos extras técnicos que o ExtractorAgent adiciona
EXTRA_FIELDS = ["_thread_id", "_row_index_in_thread", "_key"]

# Ordem final das colunas na planilha
ALL_FIELDS = HEADER_FIELDS + EXTRA_FIELDS


class SheetsSyncAgent:
    def __init__(self) -> None:
        # Arquivo da service account (o mesmo que você já usa pro GCS)
        self.service_account_file = settings.SERVICE_ACCOUNT_FILE

        # ID da planilha (parte entre /d/ e /edit na URL)
        self.spreadsheet_id = settings.SHEETS_SPREADSHEET_ID

        # Nome da aba a ser atualizada
        self.worksheet_name = getattr(settings, "SHEETS_WORKSHEET_NAME", "quotes_raw")

        # Escopo de acesso ao Google Sheets
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]

        credentials = Credentials.from_service_account_file(
            self.service_account_file,
            scopes=scopes,
        )
        self.gc = gspread.authorize(credentials)

        # Caminho do arquivo tabular no GCS
        self.quotes_path = "tables/quotes_raw.json"

    def _load_quotes(self) -> List[Dict[str, Any]]:
        """Baixa o JSON tabular (lista de objetos) do GCS."""
        data = gcs_client.download_json(self.quotes_path)
        if not data:
            print(f"⚠️ Nenhum dado encontrado em gs://{settings.GCS_BUCKET}/{self.quotes_path}")
            return []

        if not isinstance(data, list):
            print("⚠️ Formato inesperado em quotes_raw.json (esperado: lista).")
            return []

        return data

    def _to_rows(self, quotes: List[Dict[str, Any]]) -> List[List[Any]]:
        """Converte lista de dicts em matriz de linhas, na ordem de colunas ALL_FIELDS."""
        rows: List[List[Any]] = []
        for q in quotes:
            row = [q.get(field, "") for field in ALL_FIELDS]
            rows.append(row)
        return rows

    def run(self) -> None:
        print("📊 Iniciando SheetsSyncAgent (tables/quotes_raw.json → Google Sheets)")

        quotes = self._load_quotes()
        print(f"→ {len(quotes)} linhas lidas de {self.quotes_path}")

        # Abre a planilha
        sh = self.gc.open_by_key(self.spreadsheet_id)

        # Tenta abrir a aba; se não existir, cria
        try:
            ws = sh.worksheet(self.worksheet_name)
            print(f"→ Aba '{self.worksheet_name}' encontrada.")
        except gspread.WorksheetNotFound:
            print(f"→ Aba '{self.worksheet_name}' não encontrada, criando nova...")
            ws = sh.add_worksheet(
                title=self.worksheet_name,
                rows="1000",
                cols=str(len(ALL_FIELDS) + 10),
            )

        # Monta matriz de valores: primeira linha = header
        values = [ALL_FIELDS] + self._to_rows(quotes)

        print("→ Limpando aba antes de escrever...")
        ws.clear()

        print(f"→ Escrevendo {len(values) - 1} linhas na planilha...")
        # Escreve tudo começando em A1
        ws.update("A1", values)

        print("✅ Planilha atualizada com sucesso.")


if __name__ == "__main__":
    agent = SheetsSyncAgent()
    agent.run()
