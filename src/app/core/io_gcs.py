"""
io_gcs.py — Leitura/Gravação de JSON no Google Cloud Storage (GCS) de forma simples e clara.

O QUE ESTE MÓDULO FAZ
---------------------
1) Fornece funções para montar caminhos padronizados no bucket:
   - state/last_history_id.json
   - state/processed_messages.json
   - raw/<threadId>/<messageId>.json
   - parsed/<threadId>/<messageId>__<sha1short>.json

2) Oferece operações básicas no GCS:
   - save_json_to_gcs(bucket, path, data): grava um dicionário como JSON (UTF-8)
   - load_json_from_gcs(bucket, path): lê JSON e retorna um dicionário
   - object_exists_in_gcs(bucket, path): verifica se o objeto existe

3) Implementa um retry simples para erros temporários do GCS (429/503/timeout).

COMO USAR (resumo)
------------------
- Configure suas credenciais locais (uma vez):
    gcloud auth application-default login

- Depois, no seu código:
    from app.core.io_gcs import (
        path_state_last_history_id, save_json_to_gcs, load_json_from_gcs
    )

    bucket = "meu-bucket" (Vou usar do dotenv aqui)
    p = path_state_last_history_id()
    save_json_to_gcs(bucket, p, {"last_history_id": 0})
    data = load_json_from_gcs(bucket, p)
    print(data)

OBS: Aqui não colocamos o nome do projeto: o GCS usa as credenciais padrão (ADC).
No Cloud Run, isso já vem configurado pelo serviço.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict

from google.cloud import storage
from google.api_core import exceptions as gcs_err
from app.core.config import load_config

# ==============================
# 1) CONSTRUÇÃO DE CAMINHOS
# ==============================

def path_state_last_history_id() -> str:
    """Caminho fixo do arquivo que guarda o último historyId lido do Gmail."""
    return "state/last_history_id.json"

def path_state_processed_messages() -> str:
    """Caminho fixo do arquivo que guarda o set/lista de messageIds já processados."""
    return "state/processed_messages.json"

def path_raw(thread_id: str, message_id: str) -> str:
    """
    Onde salvamos a mensagem bruta do Gmail (um JSON por messageId).
    Ex.: raw/1748d8a93be/183a45ce145e1a3b.json
    """
    return f"raw/{thread_id}/{message_id}.json"

def path_parsed(thread_id: str, message_id: str, sha1short: str) -> str:
    """
    Onde salvamos o resultado parseado (cotações).
    O sha1short torna o nome idempotente (sempre igual para o mesmo conteúdo lógico).
    Ex.: parsed/1748d8a93be/183a45...__a1b2c3d4.json
    """
    return f"parsed/{thread_id}/{message_id}__{sha1short}.json"


# ==============================
# 2) CLIENTE GCS
# ==============================

def _get_storage_client() -> storage.Client:
    """
    Cria um cliente do GCS usando as Credenciais Padrão de Aplicativo (ADC).
    Local: rode `gcloud auth application-default login` antes.
    Cloud Run: o serviço já injeta as credenciais.
    """
    return storage.Client()


# ==============================
# 3) RETRY SIMPLES
# ==============================

def _with_simple_retry(fn, op_name: str, attempts: int = 5, base_sleep: float = 0.5):
    """
    Executa `fn()` com retries em erros temporários (429/503/timeout).
    - attempts: quantas tentativas no total
    - base_sleep: intervalo inicial entre tentativas (aumenta exponencialmente)
    """
    sleep = base_sleep
    for i in range(1, attempts + 1):
        try:
            return fn()
        except (gcs_err.TooManyRequests, gcs_err.ServiceUnavailable, gcs_err.DeadlineExceeded) as e:
            # Erros temporários: tentar novamente, com backoff
            if i == attempts:
                raise RuntimeError(
                    f"Falha ao {op_name} no GCS após {attempts} tentativas. "
                    "Tente novamente em instantes."
                ) from e
            time.sleep(sleep)
            sleep = min(sleep * 2, 5.0)  # backoff até 5s máx
        except Exception as e:
            # Outros erros: falhar imediatamente, com mensagem clara
            raise RuntimeError(f"Erro ao {op_name} no GCS: {e}") from e


# ==============================
# 4) OPERAÇÕES BÁSICAS COM JSON
# ==============================

def _validate_bucket_and_path(bucket_name: str, blob_path: str) -> None:
    """
    Valida entradas comuns:
    - bucket não pode conter 'gs://' nem barras
    - path não pode terminar com '/'
    """
    if not bucket_name or "/" in bucket_name or bucket_name.startswith("gs://"):
        raise ValueError(
            "Bucket inválido. Use SOMENTE o nome do bucket, sem 'gs://' e sem '/'. "
            "Ex.: parrots-pipeline-data"
        )
    if not blob_path or blob_path.endswith("/"):
        raise ValueError(
            "Caminho de objeto inválido. Forneça um caminho completo para um arquivo JSON. "
            "Ex.: state/last_history_id.json"
        )

def save_json_to_gcs(bucket_name: str, blob_path: str, data: Dict[str, Any]) -> None:
    """
    Grava um dicionário como JSON (UTF-8) no GCS, com content_type correto.
    Cria o objeto ou o sobrescreve.
    """
    _validate_bucket_and_path(bucket_name, blob_path)

    def _op():
        client = _get_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        # Serializa o dicionário para JSON bonito e codifica em UTF-8
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

        # Envia para o GCS
        blob.upload_from_string(
            payload,
            content_type="application/json; charset=utf-8",
        )

    _with_simple_retry(_op, op_name=f"gravar '{blob_path}'")

def load_json_from_gcs(bucket_name: str, blob_path: str) -> Dict[str, Any]:
    """
    Lê um JSON do GCS e retorna um dicionário.
    Erros comuns:
    - Objeto não existe -> mensagem clara.
    - JSON malformado -> mensagem clara.
    """
    _validate_bucket_and_path(bucket_name, blob_path)

    def _op():
        client = _get_storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        if not blob.exists():
            raise FileNotFoundError(f"Objeto não encontrado no GCS: {blob_path}")

        text = blob.download_as_text(encoding="utf-8")
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Conteúdo em '{blob_path}' não é um JSON válido: {e}"
            ) from e

    return _with_simple_retry(_op, op_name=f"ler '{blob_path}'")

def object_exists_in_gcs(bucket_name: str, blob_path: str) -> bool:
    """
    Retorna True se o objeto existir no GCS; caso contrário, False.
    """
    _validate_bucket_and_path(bucket_name, blob_path)

    def _op():
        client = _get_storage_client()
        bucket = client.bucket(bucket_name)
        return bucket.blob(blob_path).exists()

    return _with_simple_retry(_op, op_name=f"verificar existência de '{blob_path}'")


# =============================
# 5) PEQUENO "SMOKE TEST" REAL
# =============================

if __name__ == "__main__":
    
    print("✅ Teste REAL no GCS usando .env")
    cfg = load_config()
    bucket = cfg.gcs_bucket
    p = path_state_last_history_id()

    save_json_to_gcs(bucket, p, {"last_history_id": 0})
    print("Gravado em:", bucket, p)
    print("Existe?", object_exists_in_gcs(bucket, p))
    print("Lido  :", load_json_from_gcs(bucket, p))
