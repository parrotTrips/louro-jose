#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
save_quotes_to_csv.py
---------------------
Varre:
  - complete_data/
  - incomplete_data/

1) Normaliza cada registro no esquema canônico HEADER_FIELDS.
2) Salva CSVs:
     - outputs/complete_data.csv
     - outputs/incomplete_data.csv
3) Faz append de TUDO na aba "quotes" da mesma planilha.
   (inclusive registros de incomplete_data, com campos ausentes como "")

Requisitos:
- .env com SHEET_ID
- Credencial em ../credentials/sheets-parrots.json
"""

import os
import sys
import json
import csv
from typing import List, Dict, Any
from dotenv import load_dotenv

# caminhos e env
sys.path.append("..")
load_dotenv("../.env")

from modules.headers import HEADER_FIELDS
from modules.login_sheets import open_spreadsheet_by_id, open_worksheet, get_first_row

# Pastas e arquivos
COMPLETE_DIR = "complete_data"
INCOMPLETE_DIR = "incomplete_data"
OUTPUTS_DIR = "outputs"

CSV_COMPLETE = os.path.join(OUTPUTS_DIR, "complete_data.csv")
CSV_INCOMPLETE = os.path.join(OUTPUTS_DIR, "incomplete_data.csv")

CREDENTIALS_PATH = "../credentials/sheets-parrots.json"
WORKSHEET_ALL = "quotes"  # única aba de destino


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _dict_to_row(d: Dict[str, Any]) -> List[str]:
    """Converte o dict para a lista na ordem exata do cabeçalho canônico."""
    out: List[str] = []
    for k in HEADER_FIELDS:
        v = d.get(k, "")
        if v is None:
            v = ""
        out.append(str(v))
    return out


def _normalize_obj_to_header(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Garante todas as colunas no esquema canônico, ignorando extras."""
    return {k: obj.get(k, "") if obj.get(k, "") is not None else "" for k in HEADER_FIELDS}


def _collect_dicts_from_folder(folder: str) -> List[Dict[str, Any]]:
    """Lê todos os .json da pasta e retorna lista de dicts normalizados ao HEADER_FIELDS."""
    if not os.path.isdir(folder):
        print(f"⚠️  Pasta não encontrada (ignorando): {folder}")
        return []

    files = sorted(
        [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".json")]
    )
    rows: List[Dict[str, Any]] = []
    for fpath in files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"⚠️  Falha ao ler '{fpath}': {e}")
            continue

        # Suporta arquivo ser um objeto único ou uma lista de objetos
        if isinstance(data, dict):
            rows.append(_normalize_obj_to_header(data))
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    rows.append(_normalize_obj_to_header(item))
        # outros tipos são ignorados

    return rows


def _write_csv(path: str, dict_rows: List[Dict[str, Any]]) -> None:
    """Escreve CSV canônico com HEADER_FIELDS."""
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER_FIELDS)
        for d in dict_rows:
            writer.writerow(_dict_to_row(d))
    print(f"💾 CSV salvo: {path} ({len(dict_rows)} linha(s))")


def _append_in_chunks(ws, rows_to_append: List[List[str]], chunk_size: int = 200):
    """Evita lotar a API enviando em lotes."""
    total = len(rows_to_append)
    if total == 0:
        return
    for i in range(0, total, chunk_size):
        chunk = rows_to_append[i:i + chunk_size]
        ws.append_rows(chunk, value_input_option="USER_ENTERED")
        print(f"➡️  Enviado(s) {min(i + len(chunk), total)}/{total} linha(s)...")


def _append_to_sheet(sh, worksheet_name: str, dict_rows: List[Dict[str, Any]]):
    """Garante cabeçalho e faz append das linhas na aba especificada."""
    ws = open_worksheet(sh, worksheet_name)

    # 1) Conferir/definir cabeçalho (corrigindo DeprecationWarning com kwargs nomeados)
    first_row = get_first_row(ws) or []
    if not first_row:
        ws.update(range_name="A1", values=[HEADER_FIELDS])
        print(f"🧭 Cabeçalho criado na planilha com HEADER_FIELDS (aba '{worksheet_name}').")
    elif [c.strip() for c in first_row[:len(HEADER_FIELDS)]] != HEADER_FIELDS:
        print(f"⚠️ Aviso: o cabeçalho atual na aba '{worksheet_name}' não bate 100% com HEADER_FIELDS.")
        print("   Cabeçalho na planilha:", first_row)
        print("   HEADER_FIELDS esperado:", HEADER_FIELDS)
        print("   Vou continuar e fazer append mesmo assim.")

    # 2) Converter para linhas e enviar em batch
    rows_to_append = [_dict_to_row(d) for d in dict_rows]
    _append_in_chunks(ws, rows_to_append, chunk_size=200)
    print(f"✅ Inseridas {len(rows_to_append)} linha(s) na aba '{worksheet_name}'.")


def main():
    SHEET_ID = os.getenv("SHEET_ID", "").strip()
    if not SHEET_ID:
        raise SystemExit("⛔ SHEET_ID não definido no .env")

    # 1) Coletar dados das pastas
    complete_rows = _collect_dicts_from_folder(COMPLETE_DIR)
    incomplete_rows = _collect_dicts_from_folder(INCOMPLETE_DIR)

    if not complete_rows and not incomplete_rows:
        print("⛔ Nenhum dado encontrado em 'complete_data/' ou 'incomplete_data/'.")
        return

    # 2) Salvar CSVs separados (audit/local)
    if complete_rows:
        _write_csv(CSV_COMPLETE, complete_rows)
    else:
        print("ℹ️  Sem dados para 'complete_data/' — CSV não gerado.")

    if incomplete_rows:
        _write_csv(CSV_INCOMPLETE, incomplete_rows)
    else:
        print("ℹ️  Sem dados para 'incomplete_data/' — CSV não gerado.")

    # 3) Abrir planilha
    sh = open_spreadsheet_by_id(SHEET_ID, CREDENTIALS_PATH)

    # 4) Append TUDO na mesma aba 'quotes'
    all_rows = complete_rows + incomplete_rows
    _append_to_sheet(sh, WORKSHEET_ALL, all_rows)


if __name__ == "__main__":
    main()
