from __future__ import annotations

import json
from typing import Optional
import gspread


def _get_service_account_email(credentials_path: str) -> Optional[str]:
    try:
        with open(credentials_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("client_email")
    except Exception:
        return None


def get_client(credentials_path: str) -> gspread.Client:
    """
    Autentica com uma service account e retorna um cliente gspread.
    """
    return gspread.service_account(filename=credentials_path)


def open_spreadsheet_by_id(sheet_id: str, credentials_path: str) -> gspread.Spreadsheet:
    """
    Abre uma planilha pelo ID (spreadsheetId).
    Levanta erros explícitos se não conseguir abrir (ex.: falta de permissão).
    """
    gc = get_client(credentials_path)
    try:
        return gc.open_by_key(sheet_id)
    except gspread.SpreadsheetNotFound as ex:
        svc_email = _get_service_account_email(credentials_path) or "(service account não lida do JSON)"
        raise RuntimeError(
            "Não foi possível abrir a planilha pelo ID. "
            "Verifique se o ID está correto e se a planilha está compartilhada com o service account: "
            f"{svc_email}"
        ) from ex


def open_worksheet(spreadsheet: gspread.Spreadsheet, worksheet_name: str) -> gspread.Worksheet:
    """
    Abre uma aba (worksheet) pelo nome.
    """
    try:
        return spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound as ex:
        raise RuntimeError(
            f"A aba '{worksheet_name}' não foi encontrada na planilha '{spreadsheet.title}'."
        ) from ex


def get_first_row(ws: gspread.Worksheet) -> list[str]:
    """
    Retorna a primeira linha da aba (lista de strings). Se vazia, retorna [].
    """
    return ws.row_values(1)