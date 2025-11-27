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

        # Caminho do arquivo tabular no GCS (lote da execução atual)
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

        if not quotes:
            print("ℹ️ Nenhuma cotação para sincronizar com a planilha.")
            return

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

        # --------------------------------------------------
        # 1) Garante que o cabeçalho esteja na linha 1
        # --------------------------------------------------
        header = ws.row_values(1)
        if not header:
            # aba vazia: escreve o header em A1
            print("→ Cabeçalho não encontrado, escrevendo header em A1.")
            ws.update("A1", [ALL_FIELDS])
        else:
            print("→ Cabeçalho já existente na planilha (não alterado).")

        # --------------------------------------------------
        # 2) Descobre em qual coluna está o _key
        # --------------------------------------------------
        try:
            key_col_index = ALL_FIELDS.index("_key") + 1  # 1-based (A=1, B=2, ...)
        except ValueError:
            print("⚠️ '_key' não encontrado em ALL_FIELDS; não será possível fazer deduplicação.")
            key_col_index = None

        # --------------------------------------------------
        # 3) Lê todas as _key já existentes na planilha
        # --------------------------------------------------
        existing_keys = set()
        if key_col_index is not None:
            col_values = ws.col_values(key_col_index)
            if col_values:
                # col_values[0] é o header; o resto são as chaves já existentes
                existing_keys = set(col_values[1:])

        print(f"→ Já existem {len(existing_keys)} chaves (_key) na planilha.")

        # --------------------------------------------------
        # 4) Monta apenas as linhas novas (por _key)
        # --------------------------------------------------
        rows_to_append = []
        new_count = 0
        dup_count = 0

        for q in quotes:
            key = q.get("_key")
            if not key:
                # segurança: se por algum motivo vier sem _key, ignora
                continue

            if key in existing_keys:
                dup_count += 1
                continue

            row = [q.get(field, "") for field in ALL_FIELDS]
            rows_to_append.append(row)
            existing_keys.add(key)
            new_count += 1

        if not rows_to_append:
            print(
                f"ℹ️ Nenhuma nova linha para adicionar à planilha "
                f"({dup_count} duplicadas ignoradas)."
            )
            return

        # --------------------------------------------------
        # 5) Append das novas linhas
        # --------------------------------------------------
        print(
            f"→ Adicionando {new_count} novas linhas na planilha "
            f"({dup_count} duplicadas ignoradas)..."
        )

        ws.append_rows(rows_to_append, value_input_option="RAW")

        print("✅ Planilha atualizada com sucesso (append com deduplicação por _key).")


sheets_sync_agent = SheetsSyncAgent()


if __name__ == "__main__":
    sheets_sync_agent.run()
