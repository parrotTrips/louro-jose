# src/app/sheets/sync_quotes_raw.py
from __future__ import annotations

import io
import json
from typing import Dict, Any, List, Tuple, Set

import os
from google.cloud import storage
from google.oauth2.service_account import Credentials as SACredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.core.io_gcs import load_json_from_gcs
from app.core.config import get_settings

# =========================================
# Cabeçalho de negócio (visível no Sheets)
# =========================================
HEADERS_VISIBLE = [
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
]

# Coluna técnica para idempotência (pode ocultar no Sheets)
TECH_KEY_COL = "_key"

# Cabeçalho real no Sheets = visível + técnica
SHEET_HEADERS = HEADERS_VISIBLE + [TECH_KEY_COL]

# Caminho do dataset no GCS (JSONL deduplicado)
TABLE_JSONL_PATH = "tables/quotes_raw.jsonl"

SHEET_TAB_NAME = "quotes_raw"
SHEETS_SA_JSON = os.getenv("SHEETS_SA_JSON", "credentials/sheets-parrots.json")


# =========================================
# GCS helpers
# =========================================
def _list_parsed_objects(bucket: str) -> List[str]:
    client = storage.Client()
    bkt = client.bucket(bucket)
    return [b.name for b in client.list_blobs(bkt, prefix="parsed/") if b.name.endswith(".json")]

def _load_parsed(bucket: str, blob_path: str) -> Dict[str, Any]:
    return load_json_from_gcs(bucket, blob_path)

def _rows_from_parsed(obj: Dict[str, Any]) -> Tuple[str, List[Any]]:
    """
    Constrói (key, row) para o Sheets a partir de um objeto parsed/.
    - key = message_id__sha1short  (usada p/ idempotência)
    - row segue a ordem de HEADERS_VISIBLE + [_key]
    """
    msg_id = obj.get("message_id", "")
    hshort = obj.get("_sha1short", "")
    key = f"{msg_id}__{hshort}"

    # Mapeamento mínimo agora; os demais campos ficam vazios para o parser “rico” preencher no futuro
    timestamp = obj.get("date", "")
    fornecedor = obj.get("from", "")
    assunto = obj.get("subject", "")
    descricao_quartos = obj.get("plain_text", "")

    row_visible = [
        timestamp,               # Timestamp
        fornecedor,              # Fornecedor
        assunto,                 # Assunto
        "",                      # Nome do hotel
        "",                      # Cidade
        "",                      # Check-in
        "",                      # Check-out
        "",                      # Número de quartos
        descricao_quartos,       # Descrição dos Quartos (mínimo didático)
        "",                      # Categoria do quarto
        "",                      # Preço (num)
        "",                      # Configuração do quarto
        "",                      # Tarifa NET ou comissionada?
        "",                      # Taxa? Ex.: 5% de ISS
        "",                      # Serviços incluso? ...
        "",                      # Política de pagamento
        "",                      # Política de cancelamento
    ]

    row = row_visible + [key]
    return key, row

def _write_jsonl_dedup(bucket: str, rows: List[List[Any]]) -> int:
    """Escreve JSONL deduplicado com base em _key."""
    # Dedup por _key (última coluna)
    seen: Set[str] = set()
    out_dicts: List[Dict[str, Any]] = []
    for r in rows:
        k = str(r[-1])
        if k in seen:
            continue
        seen.add(k)
        # produzir dict com campos visíveis + _key
        d = dict(zip(SHEET_HEADERS, r))
        out_dicts.append(d)

    client = storage.Client()
    bkt = client.bucket(bucket)
    blob = bkt.blob(TABLE_JSONL_PATH)

    buf = io.StringIO()
    for d in out_dicts:
        buf.write(json.dumps(d, ensure_ascii=False))
        buf.write("\n")
    blob.upload_from_string(buf.getvalue(), content_type="application/jsonl; charset=utf-8")
    return len(out_dicts)


# =========================================
# Sheets helpers
# =========================================
def _build_sheets_service():
    """
    Usa OAuth do usuário (InstalledAppFlow) com escopo de Sheets.
    Salva token em .tokens/sheets_token.json
    """
    import os
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    CLIENT_SECRETS = os.getenv("SHEETS_CLIENT_SECRETS", "credentials/real-credentials-parrots-gmail.json")
    TOKEN_PATH = os.getenv("SHEETS_TOKEN_FILE", ".tokens/sheets_token.json")

    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS, SCOPES)
            creds = flow.run_local_server(port=0)
            with open(TOKEN_PATH, "w") as f:
                f.write(creds.to_json())

    return build("sheets", "v4", credentials=creds, cache_discovery=False)




def _ensure_tab_and_headers(service, spreadsheet_id: str, tab_name: str, headers: List[str]) -> None:
    sheets = service.spreadsheets()
    meta = sheets.get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    for sh in meta.get("sheets", []):
        title = sh.get("properties", {}).get("title")
        if title == tab_name:
            sheet_id = sh.get("properties", {}).get("sheetId")
            break

    if sheet_id is None:
        requests = [{"addSheet": {"properties": {"title": tab_name, "gridProperties": {"frozenRowCount": 1}}}}]
        sheets.batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()
        sheets.values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()
    else:
        resp = sheets.values().get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!A1:ZZ1").execute()
        v = resp.get("values", [])
        if not v or not v[0]:
            sheets.values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!A1",
                valueInputOption="RAW",
                body={"values": [headers]},
            ).execute()

def _read_existing_keys(service, spreadsheet_id: str, tab_name: str) -> Set[str]:
    """
    Lê a coluna técnica _key (última coluna). Se a aba ainda não tiver _key,
    volta conjunto vazio.
    """
    sheets = service.spreadsheets()
    # Descobre quantas colunas existem (para localizar _key no final)
    header = sheets.values().get(spreadsheetId=spreadsheet_id, range=f"{tab_name}!1:1").execute()
    cols = header.get("values", [[]])[0]
    if not cols:
        return set()
    key_col_idx = None
    for i, name in enumerate(cols, start=1):
        if name == TECH_KEY_COL:
            key_col_idx = i
            break
    if key_col_idx is None:
        # não há coluna _key ainda
        return set()

    col_letter = _number_to_column_letter(key_col_idx)
    resp = sheets.values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!{col_letter}2:{col_letter}",
        majorDimension="COLUMNS",
    ).execute()
    arr = resp.get("values", [])
    if not arr:
        return set()
    return set(v for v in arr[0] if isinstance(v, str) and v.strip())

def _append_rows(service, spreadsheet_id: str, tab_name: str, rows: List[List[Any]]) -> int:
    if not rows:
        return 0
    sheets = service.spreadsheets()
    resp = sheets.values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()
    return int(resp.get("updates", {}).get("updatedRows", 0))

def _number_to_column_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# =========================================
# Função principal
# =========================================
def sync_quotes_raw(
    bucket: str,
    sheet_id: str,
    tab_name: str = SHEET_TAB_NAME,
    limit: int | None = None,
) -> Dict[str, int]:
    """
    Sincroniza parsed/ → GCS(JSONL) + Sheets(aba quotes_raw).
    - JSONL: deduplicado, sobrescrito (idempotente).
    - Sheets: cria aba/cabeçalho se necessário; lê _key existentes; appenda só novas.
    """
    summary = {
        "parsed_seen": 0,
        "rows_built": 0,
        "jsonl_records": 0,
        "sheet_existing_keys": 0,
        "sheet_appended": 0,
        "sheet_skipped": 0,
        "errors": 0,
    }

    # 1) Coleta parsed/
    keys = _list_parsed_objects(bucket)
    keys = [k for k in keys if k.endswith(".json")]
    if isinstance(limit, int) and limit > 0:
        keys = keys[:limit]
    summary["parsed_seen"] = len(keys)

    # 2) Constrói linhas
    rows: List[List[Any]] = []
    for blob_path in keys:
        try:
            obj = _load_parsed(bucket, blob_path)
            _, row = _rows_from_parsed(obj)
            rows.append(row)
        except Exception:
            summary["errors"] += 1
    summary["rows_built"] = len(rows)

    # 3) Escreve JSONL deduplicado no GCS
    summary["jsonl_records"] = _write_jsonl_dedup(bucket, rows)

    # 4) Sheets
    try:
        service = _build_sheets_service()
        _ensure_tab_and_headers(service, sheet_id, tab_name, SHEET_HEADERS)
        existing = _read_existing_keys(service, sheet_id, tab_name)
        summary["sheet_existing_keys"] = len(existing)

        to_append = [r for r in rows if str(r[-1]) not in existing]
        summary["sheet_skipped"] = len(rows) - len(to_append)

        appended = _append_rows(service, sheet_id, tab_name, to_append)
        summary["sheet_appended"] = appended
    except HttpError as e:
        print(f"✗ SHEETS ERROR: {e}")
        summary["errors"] += 1

    return summary


# Execução direta
if __name__ == "__main__":
    settings = get_settings()
    s = sync_quotes_raw(
        bucket=settings.gcs_bucket,
        sheet_id=settings.sheet_id,
        tab_name=SHEET_TAB_NAME,
        limit=None,
    )
    print(json.dumps(s, ensure_ascii=False, indent=2))
