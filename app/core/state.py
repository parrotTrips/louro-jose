# app/core/state.py

from app.core.gcs_client import gcs_client

STATE_PATH = "state/threads_state.json"


class State:
    def __init__(self):
        """
        Carrega o estado atual do bucket.
        Se o arquivo não existir, inicia com um dicionário vazio.
        """
        self.data = gcs_client.download_json(STATE_PATH) or {}

    # ------------------------------------------------------------
    # LER ESTADO
    # ------------------------------------------------------------
    def get_last_email(self, thread_id: str):
        """
        Retorna o último email processado de uma thread.
        """
        return self.data.get(thread_id, {}).get("last_email_id")

    # ------------------------------------------------------------
    # ATUALIZAR ESTADO
    # ------------------------------------------------------------
    def update_thread(self, thread_id: str, last_email_id: str):
        """
        Atualiza o controle de uma thread e salva no bucket.
        """
        self.data[thread_id] = {"last_email_id": last_email_id}
        gcs_client.upload_json(STATE_PATH, self.data)
        print(f"[STATE] Thread {thread_id} atualizada → último email {last_email_id}")


# Instância global do estado
state = State()
