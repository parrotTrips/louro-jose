import os
import json
from typing import List, Dict

from dotenv import load_dotenv
from utils.headers import HEADER_FIELDS
from utils.login_sheets import open_spreadsheet_by_id, open_worksheet, get_first_row

IN_JSONL = "outputs/quotes_extracted.jsonl"
CREDENTIALS_PATH = "credentials/sheets-parrots.json"
WORKSHEET_NAME = "quotes"


def _dict_to_row(d: Dict[str, str]) -> List[str]:
    """Converte o dict para a lista na ordem exata do cabeçalho."""
    return [d.get(k, "") for k in HEADER_FIELDS]


def _load_jsonl(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        raise SystemExit(f"⛔ Arquivo não encontrado: {path}. Rode antes: llm_extract_quotes.py")

    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                print(f"⚠️ Linha {ln} inválida no JSONL: {e}")
                continue
            # garante todas as colunas
            rows.append({k: obj.get(k, "") for k in HEADER_FIELDS})
    return rows


def _append_in_chunks(ws, rows_to_append: List[List[str]], chunk_size: int = 200):
    """Evita lotar a API enviando em lotes."""
    total = len(rows_to_append)
    if total == 0:
        return
    for i in range(0, total, chunk_size):
        chunk = rows_to_append[i:i + chunk_size]
        ws.append_rows(chunk, value_input_option="USER_ENTERED")
        print(f"➡️  Enviado(s) {min(i + len(chunk), total)}/{total} linha(s)...")


def main():
    load_dotenv()
    SHEET_ID = os.getenv("SHEET_ID", "").strip()
    if not SHEET_ID:
        raise SystemExit("⛔ SHEET_ID não definido no .env")

    # 1) Carregar dados do JSONL
    dict_rows = _load_jsonl(IN_JSONL)
    if not dict_rows:
        print("⛔ Nenhuma linha para salvar (JSONL vazio).")
        return

    # 2) Abrir planilha/aba
    sh = open_spreadsheet_by_id(SHEET_ID, CREDENTIALS_PATH)
    ws = open_worksheet(sh, WORKSHEET_NAME)

    # 3) Conferir cabeçalho existente (só avisa; não altera)
    first_row = get_first_row(ws) or []
    if first_row and [c.strip() for c in first_row[:len(HEADER_FIELDS)]] != HEADER_FIELDS:
        print("⚠️ Aviso: o cabeçalho atual na planilha não bate 100% com HEADER_FIELDS.")
        print("   Cabeçalho na planilha:", first_row)
        print("   HEADER_FIELDS esperado:", HEADER_FIELDS)
        print("   Vou continuar e fazer append mesmo assim.")

    # 4) Converter para linhas e enviar em batch
    rows_to_append = [_dict_to_row(d) for d in dict_rows]
    _append_in_chunks(ws, rows_to_append, chunk_size=200)

    print(f"✅ Inseridas {len(rows_to_append)} linha(s) na aba '{WORKSHEET_NAME}'.")


if __name__ == "__main__":
    main()
