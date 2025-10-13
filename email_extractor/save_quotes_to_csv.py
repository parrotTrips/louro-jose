#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
save_quotes_to_sheets.py (preenche e depois limpa gaps por deleção)
-------------------------------------------------------------------
- Lê JSONs de:
    - complete_data/
    - incomplete_data/
- Normaliza conforme modules.headers.HEADER_FIELDS
- Escreve na planilha (aba 'quotes') e, em seguida,
  remove linhas em branco na faixa A2..A{clean-limit}.

Como configurar o limite de limpeza (linhas inspecionadas a partir do topo):
- Via CLI:   --clean-limit 200     (padrão 200)
- Via .env:  CLEAN_LIMIT=200

Requisitos:
- .env com SHEET_ID (e opcional CLEAN_LIMIT)
- Credencial em ../credentials/sheets-parrots.json
"""

import os
import sys
import json
import argparse
from typing import List, Dict, Any, Tuple
from dotenv import load_dotenv

# importar utilitários do projeto
sys.path.append("..")
load_dotenv("../.env")

from modules.headers import HEADER_FIELDS
from modules.login_sheets import open_spreadsheet_by_id, open_worksheet, get_first_row

# Pastas
COMPLETE_DIR = "complete_data"
INCOMPLETE_DIR = "incomplete_data"

# Sheets
CREDENTIALS_PATH = "../credentials/sheets-parrots.json"
WORKSHEET_ALL = "quotes"


# ----------------- utilidades básicas -----------------

def _normalize_obj_to_header(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Mantém somente HEADER_FIELDS e garante '' para ausentes/None."""
    return {k: ("" if obj.get(k, "") is None else obj.get(k, "")) for k in HEADER_FIELDS}


def _dict_to_row(d: Dict[str, Any]) -> List[str]:
    """Valores na ordem exata de HEADER_FIELDS (sempre str/'' )."""
    return [str(d.get(k, "")) if d.get(k, "") is not None else "" for k in HEADER_FIELDS]


def _collect_from_folder(folder: str) -> List[Dict[str, Any]]:
    """Lê todos os .json (objeto único ou lista) e normaliza."""
    rows: List[Dict[str, Any]] = []
    if not os.path.isdir(folder):
        return rows

    files = sorted(
        f for f in (os.path.join(folder, x) for x in os.listdir(folder))
        if os.path.isfile(f) and f.lower().endswith(".json")
    )
    for fpath in files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"⚠️  Erro lendo '{fpath}': {e}")
            continue

        if isinstance(data, dict):
            rows.append(_normalize_obj_to_header(data))
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    rows.append(_normalize_obj_to_header(item))
    return rows


# ----------------- escrita na planilha -----------------

def _ensure_header(ws) -> None:
    """Garante cabeçalho == HEADER_FIELDS na linha 1."""
    first_row = get_first_row(ws) or []
    if not first_row:
        ws.update(range_name="A1", values=[HEADER_FIELDS])
        print(f"🧭 Cabeçalho criado na aba '{WORKSHEET_ALL}'.")
        return

    normalized_sheet = [c.strip() for c in first_row[:len(HEADER_FIELDS)]]
    if normalized_sheet != HEADER_FIELDS:
        ws.update(range_name="A1", values=[HEADER_FIELDS])
        print(f"ℹ️  Cabeçalho da aba '{WORKSHEET_ALL}' foi atualizado para HEADER_FIELDS atual.")


def _col_letter(idx: int) -> str:
    """1->A, 2->B, ..."""
    s = ""
    while idx > 0:
        idx, r = divmod(idx - 1, 26)
        s = chr(65 + r) + s
    return s


def _detect_first_gap(ws, ncols: int) -> Tuple[int, int]:
    """
    Detecta o primeiro buraco interno (todas as células vazias de A..ncols).
    Retorna (linha_inicio_gap, linha_primeira_usada_abaixo).
    - Se não houver gap, retorna (len(vals)+1, 0): escrever no final.
    """
    vals = ws.get_all_values()
    if not vals:
        return 2, 0
    if len(vals) == 1:
        return 2, 0

    start_gap, next_used = 0, 0
    for i in range(1, len(vals)):  # começa da linha 2 (index 1)
        row = vals[i] if i < len(vals) else []
        padded = (row + [""] * ncols)[:ncols]
        is_empty = all((cell or "").strip() == "" for cell in padded)
        if is_empty and start_gap == 0:
            start_gap = i + 1  # número da linha
        if (not is_empty) and start_gap != 0:
            next_used = i + 1
            break

    if start_gap == 0:
        return len(vals) + 1, 0
    return start_gap, next_used  # se next_used==0, gap vai até o fim


def _update_block(ws, start_row: int, rows: List[List[str]]):
    if not rows:
        return
    end_col = _col_letter(len(HEADER_FIELDS))
    r1 = start_row
    r2 = start_row + len(rows) - 1
    ws.update(range_name=f"A{r1}:{end_col}{r2}", values=rows, value_input_option="USER_ENTERED")


def _append_without_gaps(ws, dict_rows: List[Dict[str, Any]], chunk_size: int = 200):
    """
    Estratégia existente: tenta preencher o primeiro gap interno; se sobrar, continua no final.
    (Mantida a pedido; a limpeza de linhas vazias virá DEPOIS desta escrita.)
    """
    if not dict_rows:
        return

    _ensure_header(ws)

    ncols = len(HEADER_FIELDS)
    rows = [_dict_to_row(d) for d in dict_rows]

    start_gap, next_used = _detect_first_gap(ws, ncols)

    # Sem gap interno (ou gap até o fim) → escreve tudo a partir de start_gap
    if next_used == 0:
        total, sent = len(rows), 0
        while sent < total:
            chunk = rows[sent:sent + chunk_size]
            _update_block(ws, start_gap + sent, chunk)
            sent += len(chunk)
            print(f"➡️  Enviado(s) {sent}/{total} linha(s)...")
        return

    # Gap limitado: [start_gap .. next_used-1]
    gap_size = max(0, next_used - start_gap)
    first_part = rows[:gap_size]
    rest_part = rows[gap_size:]

    # 1) Preenche o gap
    total_gap, sent_gap = len(first_part), 0
    while sent_gap < total_gap:
        chunk = first_part[sent_gap:sent_gap + chunk_size]
        _update_block(ws, start_gap + sent_gap, chunk)
        sent_gap += len(chunk)
        print(f"➡️  (gap) Enviado(s) {sent_gap}/{total_gap} linha(s)...")

    # 2) O restante vai para o final atual
    if rest_part:
        vals_after = ws.get_all_values()
        start_tail = len(vals_after) + 1
        total_tail, sent_tail = len(rest_part), 0
        while sent_tail < total_tail:
            chunk = rest_part[sent_tail:sent_tail + chunk_size]
            _update_block(ws, start_tail + sent_tail, chunk)
            sent_tail += len(chunk)
            print(f"➡️  (final) Enviado(s) {sent_tail}/{total_tail} linha(s)...")


def _append_all_to_sheet(sheet, worksheet_name: str, dict_rows: List[Dict[str, Any]]):
    """Abre aba, garante cabeçalho e grava todas as linhas."""
    ws = open_worksheet(sheet, worksheet_name)
    _append_without_gaps(ws, dict_rows, chunk_size=200)
    print(f"✅ Inseridas {len(dict_rows)} linha(s) na aba '{worksheet_name}'.")
    return ws


# ----------------- limpeza de linhas vazias -----------------

def _pad_trim(row: List[str], ncols: int) -> List[str]:
    return (row + [""] * ncols)[:ncols]

def _is_row_empty(row: List[str]) -> bool:
    return all((c or "").strip() == "" for c in row)

def _col_range(ncols: int) -> str:
    return f"A:{_col_letter(ncols)}"

def _group_consecutive(nums: List[int]) -> List[Tuple[int, int]]:
    """Transforma [5,6,7, 10,11] -> [(5,7), (10,11)] para deletar em blocos."""
    if not nums:
        return []
    nums = sorted(nums)
    ranges = []
    start = prev = nums[0]
    for n in nums[1:]:
        if n == prev + 1:
            prev = n
            continue
        ranges.append((start, prev))
        start = prev = n
    ranges.append((start, prev))
    return ranges

def remove_blank_rows(ws, scan_limit: int, ncols: int) -> int:
    """
    Varre da linha 2 até 'scan_limit' (inclusive) e remove linhas 100% vazias
    (todas as colunas A..ncols em branco). Retorna quantidade de linhas removidas.

    Observação: deleta em ordem reversa por blocos para evitar alteração de índices.
    """
    scan_limit = max(2, int(scan_limit))
    end_col = _col_letter(ncols)
    # Range explícito preserva células vazias como "" no retorno
    data = ws.get(f"A2:{end_col}{scan_limit}", value_render_option="FORMATTED_VALUE")
    # data é uma matriz com (scan_limit-1) linhas; se a sheet for menor, gspread
    # geralmente retorna linhas faltantes como [] -> tratamos como vazias.

    empty_rows_abs: List[int] = []
    for i in range(2, scan_limit + 1):
        idx = i - 2  # índice dentro de 'data'
        row = data[idx] if idx < len(data) else []
        row = _pad_trim(row, ncols)
        if _is_row_empty(row):
            empty_rows_abs.append(i)

    if not empty_rows_abs:
        print(f"🧼 Sem linhas 100% vazias entre A2 e A{scan_limit}.")
        return 0

    # Deletar em blocos (menos chamadas à API)
    ranges = _group_consecutive(empty_rows_abs)
    deleted = 0
    for start, end in reversed(ranges):
        try:
            ws.delete_rows(start, end)
            deleted += (end - start + 1)
            print(f"🗑️  Removidas linhas {start}–{end}.")
        except Exception as e:
            print(f"⚠️  Falha ao remover linhas {start}–{end}: {e}")

    print(f"✅ Limpeza concluída: {deleted} linha(s) vazia(s) removida(s) no topo (até {scan_limit}).")
    return deleted


# ----------------- main -----------------

def main():
    parser = argparse.ArgumentParser(description="Envia dados ao Google Sheets e limpa linhas vazias no topo.")
    parser.add_argument("--clean-limit", type=int, default=int(os.getenv("CLEAN_LIMIT", "200")),
                        help="Máximo de linhas a inspecionar a partir do topo (A2..A{N}) para remoção de linhas totalmente vazias. Padrão=200.")
    args = parser.parse_args()

    SHEET_ID = os.getenv("SHEET_ID", "").strip()
    if not SHEET_ID:
        raise SystemExit("⛔ SHEET_ID não definido no .env")

    all_rows: List[Dict[str, Any]] = []
    all_rows.extend(_collect_from_folder(COMPLETE_DIR))
    all_rows.extend(_collect_from_folder(INCOMPLETE_DIR))

    if not all_rows:
        print("⛔ Nada para enviar: nenhuma linha encontrada em 'complete_data/' ou 'incomplete_data/'.")
        return

    sh = open_spreadsheet_by_id(SHEET_ID, CREDENTIALS_PATH)
    ws = _append_all_to_sheet(sh, WORKSHEET_ALL, all_rows)

    # limpeza de linhas em branco no topo (A2..A{clean-limit})
    ncols = len(HEADER_FIELDS)
    remove_blank_rows(ws, scan_limit=args.clean_limit, ncols=ncols)


if __name__ == "__main__":
    main()
