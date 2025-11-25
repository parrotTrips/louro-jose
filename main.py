# main.py
from app.agents.labeler_storage_agent import labeler_storage_agent
from app.agents.extractor_agent import extractor_agent


if __name__ == "__main__":
    print("🔵 Rodando Labeler + Storage (Gmail → QUOTES → threads/ + state/threads_state.json)")
    labeler_storage_agent.run()

    print("🟣 Rodando ExtractorAgent (threads/ → tables/quotes_raw.json + state/extractor_state.json)")
    extractor_agent.run()
