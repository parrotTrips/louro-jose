#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
qa.py
-----
RAG no Qdrant com auto-detecção de dimensão (384 vs 1536) e resposta via LLM (OpenRouter).

Uso:
  python3 qa.py "Qual a política de cancelamento do Radisson Barra?"
  python3 qa.py "Quais tarifas incluem café?" --k 6 --mmr --show-sources

.env (../.env):
  QDRANT_URL=http://localhost:6333          # ou HOST/PORT
  QDRANT_HOST=localhost
  QDRANT_PORT=6333
  QDRANT_API_KEY=
  QDRANT_COLLECTION=emails_quotes
  QDRANT_CONTENT_KEY=text
  QDRANT_LABEL_FILTER=QUOTES

  # Embeddings (auto):
  # - 1536 -> OpenAIEmbeddings (precisa OPENAI_API_KEY)
  # - 384  -> FastEmbedEmbeddings (local)
  OPENAI_API_KEY=...

  # LLM via OpenRouter:
  OPENROUTER_API_KEY=...
  OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
  OPENROUTER_MODEL=openai/gpt-4o-mini
"""

from __future__ import annotations
import os
import sys
import argparse
from typing import Any, Dict, List, Optional

# carrega ../.env
sys.path.append("..")
from dotenv import load_dotenv
load_dotenv("../.env")

from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore
from langchain_community.embeddings import FastEmbedEmbeddings
from langchain_openai import OpenAIEmbeddings, ChatOpenAI

# =========================
# Config
# =========================
QDRANT_URL        = os.getenv("QDRANT_URL", "").strip()
QDRANT_HOST       = os.getenv("QDRANT_HOST", "localhost").strip()
QDRANT_PORT       = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_API_KEY    = os.getenv("QDRANT_API_KEY", "").strip()
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "emails_quotes").strip()
CONTENT_KEY       = os.getenv("QDRANT_CONTENT_KEY", "text").strip()
LABEL_FILTER_VAL  = os.getenv("QDRANT_LABEL_FILTER", "QUOTES").strip()

OPENAI_API_KEY    = os.getenv("OPENAI_API_KEY", "").strip()

OPENROUTER_API_KEY  = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").strip()
OPENROUTER_MODEL    = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini").strip()

# Modelos default
FASTEMBED_MODEL   = os.getenv("FASTEMBED_MODEL", "BAAI/bge-small-en-v1.5").strip()  # 384d
OPENAI_EMB_MODEL  = os.getenv("OPENAI_EMB_MODEL", "text-embedding-3-small").strip() # 1536d

PROMPT = """Você é um assistente que responde usando EXCLUSIVAMENTE o contexto abaixo (trechos de e-mails de fornecedores).
Se a resposta não estiver no contexto, diga claramente que não encontrou.
Se houver políticas (cancelamento, no-show, prazos), resuma e mencione números/datas.

Pergunta:
{question}

=== CONTEXTO ===
{context}
================

Responda em português, de forma objetiva.
"""

def make_qdrant_client() -> QdrantClient:
    if QDRANT_URL:
        return QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY or None)
    return QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, api_key=QDRANT_API_KEY or None)

def detect_vector_dim(client: QdrantClient, collection: str) -> int:
    """
    Lê a config da coleção e retorna o tamanho do vetor (suporta named vectors e vetor único).
    """
    info = client.get_collection(collection_name=collection)
    # vetor único
    try:
        size = info.config.params.vectors.size  # type: ignore[attr-defined]
        if size:
            return int(size)
    except Exception:
        pass
    # named vectors
    try:
        vectors_map = info.config.params.vectors  # type: ignore[attr-defined]
        if isinstance(vectors_map, dict):
            for _name, cfg in vectors_map.items():
                size = getattr(cfg, "size", None) or (cfg.get("size") if isinstance(cfg, dict) else None)
                if size:
                    return int(size)
    except Exception:
        pass
    # fallback
    return 1536

def build_embeddings_for_dim(dim: int):
    """
    1536 -> OpenAIEmbeddings (precisa OPENAI_API_KEY)
    384  -> FastEmbedEmbeddings
    """
    if dim >= 1000:
        if not OPENAI_API_KEY:
            raise SystemExit(
                "❌ A coleção exige 1536 dims (OpenAIEmbeddings), mas OPENAI_API_KEY não está configurada.\n"
                "   Defina OPENAI_API_KEY no ../.env ou recrie a coleção com FastEmbed (384d)."
            )
        return OpenAIEmbeddings(api_key=OPENAI_API_KEY, model=OPENAI_EMB_MODEL)
    else:
        return FastEmbedEmbeddings(model_name=FASTEMBED_MODEL)

def build_vectorstore() -> QdrantVectorStore:
    client = make_qdrant_client()
    dim = detect_vector_dim(client, QDRANT_COLLECTION)
    embedding_fn = build_embeddings_for_dim(dim)
    return QdrantVectorStore(
        client=client,
        collection_name=QDRANT_COLLECTION,
        embedding=embedding_fn,          # <<< aqui é 'embedding' (singular)
        content_payload_key=CONTENT_KEY,
    )

def make_filter(label_value: Optional[str]) -> Optional[Dict[str, Any]]:
    if not label_value:
        return None
    return {"must": [{"key": "label", "match": {"value": label_value}}]}

def format_context(docs: List[Any]) -> str:
    blocos: List[str] = []
    for i, d in enumerate(docs, 1):
        m = d.metadata or {}
        head = f"[{i}] {m.get('subject','(sem assunto)')} — {m.get('sender','?')} — {m.get('timestamp','')}"
        text = (d.page_content or "").strip()
        if len(text) > 1400:
            text = text[:1400] + " ..."
        blocos.append(f"{head}\n{text}")
    return "\n\n".join(blocos) if blocos else "(vazio)"

def ask(question: str, k: int = 5, use_mmr: bool = False, show_sources: bool = False) -> None:
    if not OPENROUTER_API_KEY:
        raise SystemExit("❌ Falta OPENROUTER_API_KEY no ../.env")

    vectorstore = build_vectorstore()

    search_kwargs: Dict[str, Any] = {"k": k}
    f = make_filter(LABEL_FILTER_VAL)
    if f:
        search_kwargs["filter"] = f

    retriever = vectorstore.as_retriever(
        search_type="mmr" if use_mmr else "similarity",
        search_kwargs=search_kwargs,
    )

    docs = retriever.invoke(question)  # API nova
    context = format_context(docs)

    llm = ChatOpenAI(
        api_key=OPENROUTER_API_KEY,
        base_url=OPENROUTER_BASE_URL,
        model=OPENROUTER_MODEL,
        temperature=0.2,
    )

    prompt = PROMPT.format(question=question, context=context)
    resp = llm.invoke(prompt)

    # === saída pedida: só a resposta ===
    print(resp.content.strip())

    if show_sources and docs:
        print("\n--- Fontes ---")
        for i, d in enumerate(docs, 1):
            m = d.metadata or {}
            print(f"[{i}] thread_id={m.get('thread_id')}  message_id={m.get('message_id')}")
            print(f"    subject={m.get('subject')}")
            print(f"    sender={m.get('sender')}  timestamp={m.get('timestamp')}")
        print("--------------")

def main():
    p = argparse.ArgumentParser(description="RAG no Qdrant + LLM via OpenRouter")
    p.add_argument("question", help="Pergunta")
    p.add_argument("--k", type=int, default=5, help="Top-k (default 5)")
    p.add_argument("--mmr", action="store_true", help="Usa MMR (diversidade)")
    p.add_argument("--show-sources", action="store_true", help="Mostra fontes (depuração)")
    args = p.parse_args()

    ask(
        question=args.question,
        k=args.k,
        use_mmr=args.mmr,
        show_sources=args.show_sources,
    )

if __name__ == "__main__":
    main()
