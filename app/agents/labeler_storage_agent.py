# app/agents/labeler_storage_agent.py

from app.core.gmail_client import gmail_client
from app.core.gcs_client import gcs_client
from app.core.state import state
from app.core.llm_client import llm_client
from app.core.config import settings


class LabelerStorageAgent:
    def run(self):
        print("🔵 Iniciando Labeler com LLM (2 fases)")

        # Garantir que o label QUOTES existe
        label_id = gmail_client.get_or_create_label(settings.GMAIL_LABEL)
        print(f"✓ Label encontrado/criado: {settings.GMAIL_LABEL} (id={label_id})")

        # 1) Fase de classificação + rotulagem
        self.classify_and_label_messages(label_id)

        # 2) Fase de sincronização de threads QUOTES → Storage
        self.sync_quotes_threads_to_storage(label_id)

        print("\n🏁 Labeler finalizado com sucesso.")

    # ------------------------------------------------------------
    # FASE 1: Classificação + rotulagem
    # ------------------------------------------------------------
    def classify_and_label_messages(self, label_id: str):
        print("\n📥 Fase 1: buscando emails (newer_than:200d)...")

        messages = gmail_client.search_messages("newer_than:200d")
        print(f"→ Encontrados {len(messages)} emails nos últimos 200 dias.")

        for msg in messages:
            msg_id = msg["id"]

            # Baixar mensagem completa
            email_obj = gmail_client.get_message(msg_id, fmt="full")

            # Se já tiver o label QUOTES, ignora (já classificado)
            label_ids = email_obj.get("labelIds", [])
            if label_id in label_ids:
                print(f"\n📨 Email {msg_id} já tem QUOTES, pulando fase de classificação.")
                continue

            # Extrair texto do email
            try:
                email_msg = gmail_client.decode_email(email_obj)

                if email_msg.is_multipart():
                    text_parts = []
                    for part in email_msg.walk():
                        ctype = part.get_content_type()
                        disp = str(part.get("Content-Disposition") or "")
                        if ctype == "text/plain" and "attachment" not in disp:
                            payload = part.get_payload(decode=True)
                            if payload:
                                text_parts.append(
                                    payload.decode(
                                        part.get_content_charset() or "utf-8",
                                        errors="ignore",
                                    )
                                )
                    text = "\n".join(text_parts)
                else:
                    payload = email_msg.get_payload(decode=True)
                    if payload:
                        text = payload.decode(
                            email_msg.get_content_charset() or "utf-8",
                            errors="ignore",
                        )
                    else:
                        text = email_msg.get_payload()
            except Exception as e:
                print(f"\n⚠️ Erro ao decodificar email {msg_id}: {e}")
                text = ""

            print(f"\n📨 Email {msg_id} sendo analisado pelo LLM...")

            # Classificar com LLM
            classification = llm_client.classify_email(text[:8000])  # corta se for muito grande
            print(f"→ Classificação LLM: {classification}")

            if classification != "VIAGEM":
                print("   → Classificado como NAO-VIAGEM, ignorando.")
                continue

            # Aplicar label QUOTES
            gmail_client.add_label_to_message(msg_id, label_id)
            print("   ✓ Label QUOTES aplicado a este email (e thread).")

    # ------------------------------------------------------------
    # FASE 2: Threads QUOTES → Storage
    # ------------------------------------------------------------
    def sync_quotes_threads_to_storage(self, label_id: str):
        print("\n📂 Fase 2: sincronizando threads com QUOTES para o Storage...")

        threads = gmail_client.list_threads_with_label(label_id)
        print(f"→ Encontradas {len(threads)} threads com QUOTES.")

        for th in threads:
            thread_id = th["id"]

            print(f"\n📦 Processando thread {thread_id}...")

            thread_full = gmail_client.get_thread(thread_id)

            # Último email da thread
            messages = thread_full.get("messages", [])
            if not messages:
                print("   ⚠️ Thread sem mensagens, pulando.")
                continue

            last_email_id = messages[-1]["id"]

            # Verificar no estado se já processamos até esse email
            last_processed = state.get_last_email(thread_id)

            if last_processed == last_email_id:
                print("   → Nenhum email novo na thread desde o último processamento, pulando.")
                continue

            # Salvar thread no bucket
            gcs_client.upload_json(f"threads/{thread_id}.json", thread_full)

            # Atualizar estado
            state.update_thread(thread_id, last_email_id)
            print("   ✓ Thread salva no Storage e estado atualizado.")


labeler_storage_agent = LabelerStorageAgent()
