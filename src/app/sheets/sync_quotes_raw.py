# src/app/sheets/sync_quotes_raw.py
from __future__ import annotations

import io
import json
from typing import Dict, List, Iterable

import google.auth
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

from app.core.config import get_settings
from app.core import io_gcs

# ============================================================
# Cabeçalho oficial (17 colunas) + coluna técnica `_key`
# (precisa ser IGUAL ao header que você escreveu na aba)
# ============================================================

SHEET_TAB = "quotes_raw"

FULL_HEADER: List[str] = [
    "Timestamp",
    "Fornecedor",
    "Assunto",
    "Nome do hotel",
    "Cidade",
    "Check-in",
    "Check-out",
    "Número de quartos",
    "Descrição dos Quartos",
    "Categoria do quarto",
    "Preço (num)",
    "Configuração do quarto",
    "Tarifa NET ou comissionada?",
    "Taxa? Ex.: 5% de ISS",
    "Serviços incluso? Explicação: existem hotéis que consideram a tarifa de serviço já incluso e outros não.",
    "Política de pagamento",
    "Política de cancelamento",
    "_key",
]

# Aliases de campos que vêm do normalizador
FIELD_ALIASES: Dict[str, str] = {
    "_source_subject": "Assunto",
}

# ============================================================
# Google Sheets client (usa ADC / service account do env)
# ============================================================

def _build_sheets_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds, _ = google.auth.default(scopes=scopes)
    if hasattr(creds, "expired") and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("sheets", "v4", credentials=creds)

def _ensure_tab_and_header(svc, sheet_id: str) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    tab_titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if SHEET_TAB not in tab_titles:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": SHEET_TAB}}}]},
        ).execute()

    # escreve/garante o header
    end_col = chr(ord("A") + len(FULL_HEADER) - 1)
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{SHEET_TAB}!A1:{end_col}1",
        valueInputOption="RAW",
        body={"values": [FULL_HEADER]},
    ).execute()

def _load_existing_keys_from_sheet(svc, sheet_id: str) -> set:
    # lê a coluna _key a partir da linha 2
    key_col_idx = FULL_HEADER.index("_key")  # zero-based
    col_letter = chr(ord("A") + key_col_idx)
    res = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f"{SHEET_TAB}!{col_letter}2:{col_letter}",
    ).execute()
    values = res.get("values", [])
    return {row[0] for row in values if row and row[0]}

def _append_rows(svc, sheet_id: str, rows: List[List]) -> None:
    if not rows:
        return
    svc.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"{SHEET_TAB}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()

# ============================================================
# Carregar dados normalizados do GCS (parsed_fixed/)
# ============================================================

def _iter_parsed_fixed(bucket: str) -> Iterable[Dict]:
    """
    Itera por todos os arquivos parsed_fixed/*.json no GCS.
    Cada arquivo é uma LISTA de registros (dicts).
    """
    for name in io_gcs.iter_objects(bucket, "parsed_fixed/"):
        if not name.endswith(".json"):
            continue
        try:
            data = io_gcs.load_json_from_gcs(bucket, name)
            if isinstance(data, list):
                for rec in data:
                    if isinstance(rec, dict):
                        rec["_obj"] = name  # debug
                        yield rec
        except Exception as e:
            yield {"_error": f"erro lendo {name}: {e}"}

# ============================================================
# Montagem das linhas conforme FULL_HEADER
# ============================================================

def _row_from_record(rec: Dict) -> List:
    # aplica aliases (ex.: _source_subject → Assunto)
    for src, dst in FIELD_ALIASES.items():
        if src in rec and dst not in rec:
            rec[dst] = rec[src]

    # monta a linha na ordem exata do FULL_HEADER; faltantes ficam ""
    row = []
    for col in FULL_HEADER:
        row.append(rec.get(col, ""))
    return row

# ============================================================
# run() — núcleo
# ============================================================

def run() -> Dict:
    """
    Lê todos os registros em parsed_fixed/*.json (GCS), normaliza colunas
    para o FULL_HEADER, deduplica por `_key`, grava um snapshot JSON no GCS
    e faz append apenas do que não está na planilha.
    """
    s = get_settings()
    bucket = s.gcs_bucket
    sheet_id = s.sheet_id

    svc = _build_sheets_client()
    _ensure_tab_and_header(svc, sheet_id)
    existing_keys = _load_existing_keys_from_sheet(svc, sheet_id)

    parsed_seen = 0
    rows_built = 0
    appended = 0
    errors = 0
    to_append: List[List] = []

    # também manter um snapshot consolidado (JSON, não JSONL) no GCS
    snapshot: List[Dict] = []

    for rec in _iter_parsed_fixed(bucket):
        if "_error" in rec:
            errors += 1
            continue
        parsed_seen += 1

        key = rec.get("_key")
        if not key:
            errors += 1
            continue

        # snapshot completo
        snapshot.append(rec)

        # se já está na planilha, pula
        if key in existing_keys:
            continue

        row = _row_from_record(rec)
        to_append.append(row)
        rows_built += 1

    # append em lote
    _append_rows(svc, sheet_id, to_append)
    appended = len(to_append)

    # grava snapshot no GCS (JSON)
    try:
        io_gcs.save_json_to_gcs(bucket, "tables/quotes_raw.json", snapshot)
    except Exception:
        errors += 1

    summary = {
        "parsed_seen": parsed_seen,
        "rows_built": rows_built,
        "json_records": len(snapshot),
        "sheet_existing_keys": len(existing_keys),
        "sheet_appended": appended,
        "sheet_skipped": parsed_seen - appended,
        "errors": errors,
    }
    print(json.dumps(summary, ensure_ascii=False))
    return summary
