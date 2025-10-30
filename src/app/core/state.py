"""
state.py — Gerencia o estado do pipeline no Google Cloud Storage (GCS).

Arquivos no bucket:
- state/last_history_id.json        -> {"last_history_id": 0}
- state/processed_messages.json     -> {"message_ids": ["id1", "id2", ...]}

Uso típico:
    ensure_state_initialized(bucket)
    cur = get_last_history_id(bucket)
    set_last_history_id(bucket, 123)
    if not has_processed(bucket, "MSG123"):
        # processa...
        add_processed(bucket, "MSG123")
"""

from __future__ import annotations
from typing import List, Union

from app.core.io_gcs import (
    path_state_last_history_id,
    path_state_processed_messages,
    save_json_to_gcs,
    load_json_from_gcs,
    object_exists_in_gcs,
)

# Valor inicial enquanto não usamos a History API do Gmail.
DEFAULT_LAST_HISTORY_ID: Union[int, str] = 0

# Limite de segurança para evitar crescimento infinito do arquivo.
MAX_PROCESSED_IDS = 50_000


class StateError(RuntimeError):
    """Erros amigáveis relacionados ao estado do pipeline."""


def ensure_state_initialized(bucket: str) -> None:
    """
    Garante que os dois arquivos de estado existam no GCS.
    Se não existirem, cria com valores padrão seguros.
    """
    # last_history_id.json
    last_path = path_state_last_history_id()
    if not object_exists_in_gcs(bucket, last_path):
        save_json_to_gcs(bucket, last_path, {"last_history_id": DEFAULT_LAST_HISTORY_ID})

    # processed_messages.json
    proc_path = path_state_processed_messages()
    if not object_exists_in_gcs(bucket, proc_path):
        save_json_to_gcs(bucket, proc_path, {"message_ids": []})


def get_last_history_id(bucket: str) -> Union[int, str]:
    """
    Lê e retorna o last_history_id.
    Levanta StateError se o arquivo estiver ausente/malformado.
    """
    try:
        data = load_json_from_gcs(bucket, path_state_last_history_id())
    except FileNotFoundError:
        raise StateError(
            "Arquivo 'state/last_history_id.json' ausente. "
            "Chame ensure_state_initialized(bucket) antes."
        )
    value = data.get("last_history_id", None)
    if value is None:
        raise StateError("Campo 'last_history_id' ausente em state/last_history_id.json.")
    return value


def set_last_history_id(bucket: str, new_value: Union[int, str]) -> None:
    """
    Atualiza o last_history_id no GCS (idempotente: sobrescreve o valor).
    """
    try:
        save_json_to_gcs(bucket, path_state_last_history_id(), {"last_history_id": new_value})
    except Exception as e:
        raise StateError(f"Falha ao salvar last_history_id: {e}")


def _load_processed(bucket: str) -> List[str]:
    """
    Helper interno: retorna a lista normalizada (sem duplicatas) de message_ids processados.
    """
    try:
        data = load_json_from_gcs(bucket, path_state_processed_messages())
    except FileNotFoundError:
        raise StateError(
            "Arquivo 'state/processed_messages.json' ausente. "
            "Chame ensure_state_initialized(bucket) antes."
        )
    ids = data.get("message_ids", None)
    if not isinstance(ids, list):
        raise StateError("Campo 'message_ids' ausente/inválido em processed_messages.json.")

    # Normaliza: remove duplicatas preservando ordem
    seen = set()
    out: List[str] = []
    for mid in ids:
        if isinstance(mid, str) and mid and mid not in seen:
            seen.add(mid)
            out.append(mid)
    return out


def _save_processed(bucket: str, ids: List[str]) -> None:
    """
    Helper interno: salva a lista de IDs processados.
    """
    try:
        save_json_to_gcs(bucket, path_state_processed_messages(), {"message_ids": ids})
    except Exception as e:
        raise StateError(f"Falha ao salvar processed_messages: {e}")


def has_processed(bucket: str, message_id: str) -> bool:
    """
    True se 'message_id' já está marcado como processado.
    """
    if not message_id:
        raise StateError("message_id vazio.")
    return message_id in _load_processed(bucket)


def add_processed(bucket: str, message_id: str) -> None:
    """
    Marca um message_id como processado (idempotente).
    Mantém no máximo MAX_PROCESSED_IDS itens (descarta os mais antigos se exceder).
    """
    if not message_id:
        raise StateError("message_id vazio.")

    ids = _load_processed(bucket)
    if message_id in ids:
        return  # já marcado

    ids.append(message_id)

    # Cap de segurança
    if len(ids) > MAX_PROCESSED_IDS:
        ids = ids[-MAX_PROCESSED_IDS:]

    _save_processed(bucket, ids)
