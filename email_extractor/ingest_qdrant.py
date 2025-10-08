#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ingestão de e-mails no Qdrant com embeddings OpenAI (LangChain).

Fluxo:
- Lê data/emails_quotes.jsonl (uma mensagem por linha)
- Constrói Documents com metadados
- Faz chunking com índices/offsets e total de chunks
- Gera embeddings (OpenAI) conforme OPENAI_EMBED_MODEL
- Garante que a coleção do Qdrant exista na dimensão correta (ou recria sob demanda)
- Sobe os chunks via Qdrant.from_documents usando URL (sem passar client=),
  gravando o conteúdo em payload["text"] (compatível com qa.py)

Requisitos no ../.env:
  OPENAI_API_KEY=...
  OPENAI_EMBED_MODEL=text-embedding-3-small  # small=1536 | large=3072
  QDRANT_HOST=localhost
  QDRANT_PORT=6333
  QDRANT_COLLECTION=emails_quotes
  QDRANT_DISTANCE=COSINE                     # COSINE | DOT | EUCLID
  QDRANT_RECREATE_COLLECTION=false           # true para recriar (DROPA dados)
  QDRANT_CONTENT_KEY=text                    # (opcional) default "text"
"""

from __future__ import annotations
import os
import sys
import json
import uuid
from pathlib import Path
from typing import List, Dict, Any, Tuple

sys.path.append("..")
from dotenv import load_dotenv
load_dotenv("../.env")

# Chunking e Documents
from langchain.docstore.document import Document

# Embeddings + VectorStore
from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import Qdrant

# Cliente Qdrant para garantir/criar coleção com dimensão certa
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

# ==== Configs e paths ====
INPUT_JSONL = Path("data") / "emails_quotes.jsonl"

OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")
OPENAI_EMBED_MODEL = os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small").strip()

QDRANT_HOST        = os.getenv("QDRANT_HOST", "localhost").strip()
QDRANT_PORT        = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_COLLECTION  = os.getenv("QDRANT_COLLECTION", "emails_quotes").strip()
QDRANT_DISTANCE    = os.getenv("QDRANT_DISTANCE", "COSINE").strip().upper()
QDRANT_RECREATE    = os.getenv("QDRANT_RECREATE_COLLECTION", "false").strip().lower() in {"1", "true", "yes"}

# o qa.py espera o conteúdo em payload["text"]
CONTENT_PAYLOAD_KEY = (os.getenv("QDRANT_CONTENT_KEY", "text").strip() or "text")

CHUNK_SIZE = 1200
CHUNK_OVERLAP = 200

EMBED_DIMS_BY_MODEL = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
}


def get_expected_dim(model_name: str) -> int | None:
    """Retorna a dimensão esperada do embedding para o modelo informado, se conhecido."""
    return EMBED_DIMS_BY_MODEL.get(model_name)


def parse_distance(name: str) -> Distance:
    """Converte string para enum Distance do Qdrant."""
    if name == "COSINE":
        return Distance.COSINE
    if name in {"DOT", "INNER", "INNER_PRODUCT"}:
        return Distance.DOT
    if name in {"EUCLID", "EUCLIDEAN"}:
        return Distance.EUCLID
    return Distance.COSINE # Vamos usar essa caralha aqui porque todo mundo usa similaridade por cosseno


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Carrega mensagens do JSONL (uma por linha)."""
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def build_documents(records: List[Dict[str, Any]]) -> List[Document]:
    """
    Transfere cada mensagem para um Document (texto + metadados úteis).
    - Garante que os metadados tragam chaves que identifiquem univocamente o e-mail.
    """
    docs: List[Document] = []
    for r in records:
        text = (r.get("text") or "").strip()
        if not text:
            continue

        # doc_id: preferimos o message_id; se ausente, caímos para thread_id+timestamp
        message_id = r.get("message_id") or ""
        thread_id  = r.get("thread_id") or ""
        timestamp  = r.get("timestamp") or ""

        if message_id:
            doc_id = message_id
        else:
            base = f"{thread_id}:{timestamp}".strip(":")
            doc_id = base if base else thread_id or "doc"

        meta = {
            "doc_id": doc_id,
            "thread_id": thread_id or None,
            "message_id": message_id or None,
            "label": r.get("label"),
            "timestamp": timestamp,
            "sender": r.get("sender"),
            "to": r.get("to"),
            "cc": r.get("cc"),
            "subject": r.get("subject"),
        }
        docs.append(Document(page_content=text, metadata=meta))
    return docs


def split_with_positions(text: str, chunk_size: int, overlap: int) -> List[Tuple[str, int]]:
    """
    Divide 'text' em chunks com overlap, retornando lista de (chunk_text, start_index).
    Usamos isso para gravar 'chunk_start' e manter reprodutibilidade.
    """
    n = len(text)
    if n == 0:
        return []
    chunks: List[Tuple[str, int]] = []
    i = 0
    while i < n:
        j = min(i + chunk_size, n)
        chunk = text[i:j]
        chunks.append((chunk, i))
        if j == n:
            break
        i = max(j - overlap, i + 1)  # garante avanço
    return chunks


def chunk_documents(docs: List[Document]) -> List[Document]:
    """
    Faz chunking manual preservando 'chunk_start' e garantindo 'chunk_index'/'chunk_total'.
    """
    out: List[Document] = []
    for d in docs:
        parts = split_with_positions(d.page_content, CHUNK_SIZE, CHUNK_OVERLAP)
        total = len(parts)
        for idx, (chunk_text, start_idx) in enumerate(parts):
            md = dict(d.metadata)
            md["chunk_index"] = idx
            md["chunk_total"] = total
            md["chunk_start"] = start_idx
            out.append(Document(page_content=chunk_text, metadata=md))
    return out


def ensure_collection(client: QdrantClient, collection: str, dim: int, distance: Distance, recreate: bool) -> None:
    """
    Garante que a coleção exista com a dimensão e distância corretas.
    - Se recreate=True: deleta (se existir) e cria do zero (DROPA dados!).
    - Senão: cria se não existir; se existir, valida dimensão/distância.
    """
    exists = client.collection_exists(collection)
    if recreate:
        if exists:
            print(f"🧹 Deletando coleção '{collection}' (reset total).")
            client.delete_collection(collection_name=collection)
        print(f"🆕 Criando coleção '{collection}' com size={dim}, distance={distance.name}.")
        client.create_collection(
            collection_name=collection,
            vectors_config=VectorParams(size=dim, distance=distance),
        )
        return

    if not exists:
        print(f"🆕 Criando coleção '{collection}' com size={dim}, distance={distance.name}.")
        client.create_collection(
            collection_name=collection,
            vectors_config=VectorParams(size=dim, distance=distance),
        )
        return

    # Validar coleção existente
    info = client.get_collection(collection)
    current_size = None
    current_distance = None
    try:
        current_size = info.config.params.vectors.size  
        current_distance = info.config.params.vectors.distance  
    except Exception:
        pass

    if current_size is None or current_distance is None:
        print("⚠️  Não consegui ler size/distance da coleção existente. "
              "Se der erro de dimensão no upsert, defina QDRANT_RECREATE_COLLECTION=true e rode novamente.")
        return

    if int(current_size) != int(dim):
        raise RuntimeError(
            f"Dimensão divergente da coleção '{collection}': atual={current_size} vs esperado={dim}. "
            "Defina QDRANT_RECREATE_COLLECTION=true no .env para recriar a coleção."
        )
    if str(current_distance).upper() != distance.name:
        print(f"⚠️  Distance divergente (atual={current_distance}, esperado={distance.name}). "
              "Recomenda-se recriar a coleção para alinhar.")
    else:
        print(f"✅ Coleção '{collection}' existente com dimensão {current_size} e distância {current_distance}.")


def stable_uuid_from(s: str) -> str:
    """Gera UUID v5 determinístico a partir de uma string base (idempotente)."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, s))


def main():
    if not OPENAI_API_KEY:
        raise SystemExit("❌ Falta OPENAI_API_KEY no ../.env")

    expected_dim = get_expected_dim(OPENAI_EMBED_MODEL)
    if expected_dim is None:
        print(f"⚠️  Dimensão do modelo '{OPENAI_EMBED_MODEL}' desconhecida neste script.")
        print("    Ajuste o dicionário EMBED_DIMS_BY_MODEL ou defina um modelo conhecido.")
    else:
        print(f"🧠 Modelo de embedding: {OPENAI_EMBED_MODEL} (dim={expected_dim})")

    if not INPUT_JSONL.exists():
        raise SystemExit(f"❌ Arquivo não encontrado: {INPUT_JSONL}. Rode a etapa que gera esse JSONL primeiro.")

    print("📥 Lendo JSONL:", INPUT_JSONL)
    records = load_jsonl(INPUT_JSONL)
    print(f"  → {len(records)} mensagens carregadas.")

    print("🧱 Montando Documents com metadados...")
    base_docs = build_documents(records)
    print(f"  → {len(base_docs)} documents (1 por mensagem com texto).")

    print(f"✂️  Fazendo chunking (size={CHUNK_SIZE}, overlap={CHUNK_OVERLAP})...")
    chunked_docs = chunk_documents(base_docs)
    print(f"  → {len(chunked_docs)} chunks.")

    print("🧠 Preparando embeddings OpenAI...")
    embeddings = OpenAIEmbeddings(
        api_key=OPENAI_API_KEY,
        model=OPENAI_EMBED_MODEL,
    )

    distance_enum = parse_distance(QDRANT_DISTANCE)
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    if expected_dim is not None:
        ensure_collection(
            client=client,
            collection=QDRANT_COLLECTION,
            dim=expected_dim,
            distance=distance_enum,
            recreate=QDRANT_RECREATE,
        )
    else:
        print("⚠️  Pulando ensure_collection porque a dimensão esperada é desconhecida.")

    # Ingestão: usar URL (a documentação usa client porem com client esse negocio so deu errado)
    qdrant_url = f"http://{QDRANT_HOST}:{QDRANT_PORT}"
    print(f"🗄️ Conectando ao Qdrant via URL: {qdrant_url}, coleção '{QDRANT_COLLECTION}'...")
    print("⬆️  Enviando embeddings + payloads para o Qdrant...")

    # IDs estáveis por chunk: UUID v5 de "<doc_id>:<chunk_index>"
    point_ids: List[str] = []
    for d in chunked_docs:
        doc_id = d.metadata.get("doc_id") or d.metadata.get("message_id") or d.metadata.get("thread_id") or "doc"
        idx = d.metadata.get("chunk_index", 0)
        raw = f"{doc_id}:{idx}"
        point_ids.append(stable_uuid_from(raw))

    # from_documents fará o upsert dos pontos; gravamos conteúdo sob CONTENT_PAYLOAD_KEY="text"
    _ = Qdrant.from_documents(
        documents=chunked_docs,
        embedding=embeddings,
        url=qdrant_url,
        collection_name=QDRANT_COLLECTION,
        prefer_grpc=False,
        ids=point_ids,
        content_payload_key=CONTENT_PAYLOAD_KEY,  # grava o conteúdo como payload["text"]
    )

    # Checagem final
    info = client.get_collection(QDRANT_COLLECTION)
    print("✅ Ingestão concluída. Collection status:", getattr(info, "status", "OK"))
    print("ℹ️  Dica: acesse http://localhost:6333/collections para inspecionar as coleções.")
    print(f"ℹ️  Payload content key usada: '{CONTENT_PAYLOAD_KEY}' (compatível com seu qa.py).")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Falha na ingestão.")
        print("   Detalhes:", repr(e))
        raise
