# main.py
from app.agents.labeler_storage_agent import labeler_storage_agent
from app.agents.extractor_agent import extractor_agent
from app.agents.sheets_sync_agent import sheets_sync_agent


if __name__ == "__main__":
    # 🔵 Etapa 1: Labeler + Storage (opcional, por enquanto vamos deixar comentado)
    # print("🔵 Rodando Labeler + Storage (Gmail → QUOTES → threads/ + state/threads_state.json)")
    # labeler_storage_agent.run()

    # 🟣 Etapa 2: Extrator (threads/ → tables/quotes_raw.json + state/extractor_state.json)
    print("🟣 Rodando ExtractorAgent (threads/ → tables/quotes_raw.json + state/extractor_state.json)")
    extractor_agent.run()

    # 📊 Etapa 3: Sync para Google Sheets (tables/quotes_raw.json → planilha)
    print("📊 Rodando SheetsSyncAgent (tables/quotes_raw.json → Google Sheets)")
    sheets_sync_agent.run()
