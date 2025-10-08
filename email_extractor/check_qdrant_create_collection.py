"""
- Lê .env
- Conecta ao Qdrant local
- Cria (ou garante) a coleção com a dimensão correta do embedding escolhido
"""

import os
from dotenv import load_dotenv

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams

load_dotenv()

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "emails_quotes")

EMBED_MODEL = os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small")
if EMBED_MODEL == "text-embedding-3-small":
    VECTOR_SIZE = 1536
elif EMBED_MODEL == "text-embedding-3-large":
    VECTOR_SIZE = 3072
else:
    VECTOR_SIZE = 1536

def main():
    print("Conectando ao Qdrant em", f"{QDRANT_HOST}:{QDRANT_PORT}")
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    # Se a coleção já existir, só avisa; senão, cria.
    existing = [c.name for c in client.get_collections().collections]
    if QDRANT_COLLECTION in existing:
        print(f"✅ Coleção '{QDRANT_COLLECTION}' já existe.")
        # opcional: checar os params
        info = client.get_collection(QDRANT_COLLECTION)
        print("Dimensão atual:", info.vectors_count, "| Config:", info.config)
    else:
        print(f"⏳ Criando coleção '{QDRANT_COLLECTION}' com dim={VECTOR_SIZE} e métrica=Cosine...")
        client.recreate_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
        )
        print(f"✅ Coleção '{QDRANT_COLLECTION}' criada.")

    # teste básico do serviço
    health = client.get_locks()  # qualquer chamada barata já valida conexão
    print("🚦 Qdrant OK. Locks response:", health)

if __name__ == "__main__":
    main()
