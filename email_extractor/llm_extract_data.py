#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
llm_extract_data.py (versão multi-cotações)
-------------------------------------------
Lê arquivos de `raw_messages/`, consulta um LLM (OpenRouter) e extrai **uma ou mais cotações**
por arquivo — uma para **cada combinação distinta de tipo/configuração de quarto e preço**.

Saídas:
  - complete_data/: 1+ JSONs completos por arquivo de entrada (todos os HEADER_FIELDS preenchidos)
  - incomplete_data/: 1+ JSONs incompletos (lista _missing_fields) OU erros de parsing/LLM
  - extracted_data.jsonl: agregado com **uma linha por cotação**

Uso:
  python3 llm_extract_data.py
  python3 llm_extract_data.py --raw_dir raw_messages --out_complete complete_data --out_incomplete incomplete_data \
      --model openai/gpt-4o --max_files 500

Requisitos:
  - pip install python-dotenv openai==1.*
  - Definir OPENROUTER_API_KEY no ambiente ou .env
"""

from __future__ import annotations
import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Union

from dotenv import load_dotenv

# === Config de pastas padrão ===
DEFAULT_RAW_DIR = "raw_messages"
DEFAULT_COMPLETE_DIR = "complete_data"
DEFAULT_INCOMPLETE_DIR = "incomplete_data"
DEFAULT_JSONL_AGG = "extracted_data.jsonl"

# === Campos a serem extraídos (por cotação) ===
HEADER_FIELDS: List[str] = [
    "Timestamp",
    "Fornecedor",
    "Assunto",
    "Nome do hotel",
    "Cidade",
    "Check-in",
    "Check-out",
    "Número de quartos",
    "Tipo de quarto",
    "Tipo de quarto (normalizado)",
    "Preço (num)",
    "Qual configuração do quarto (twin, double)",
    "Tarifa NET ou comissionada?",
    "Taxa? Ex.: 5% de ISS",
    "Serviços incluso? Explicação: existem hotéis que consideram a tarifa de serviço já incluso e outros não.",
    "Política de pagamento",
    "Política de cancelamento",
    "Email do fornecedor",
    "Email do remetente (top-level)",
]

# === Prompt do LLM ===
SYSTEM_PROMPT = (
    "Você extrai **cotações de hotel** de e-mails.\n"
    "Sempre responda com **apenas um JSON** válido.\n"
    "Cada combinação distinta de tipo/configuração de quarto e preço deve virar **um objeto separado**.\n"
    "Se algum campo não existir, use string vazia \"\" (exceto `Preço (num)`, que deve ser número ou \"\")."
)

USER_PROMPT_TEMPLATE = """Extraia as cotações do conteúdo abaixo.\n\n\
Regras obrigatórias:\n\
- Saída deve ser **um único JSON** no formato **lista de objetos** (array).\n\
- **Uma cotação por combinação distinta** de tipo/configuração de quarto e preço.\n\
- Use **exatamente** estes nomes de chaves em **cada objeto**:\n{fields_json}\n\
- Datas podem manter o formato encontrado. Não invente valores.\n\
- `Preço (num)` deve ser numérico (ponto decimal) quando houver; caso contrário, use \"\".\n\
- `Email do remetente (top-level)` é o e-mail do **primeiro** cabeçalho \"From:\" no topo do corpo.\n\
- `Email do fornecedor` é o e-mail do hotel/fornecedor (geralmente não `parrottrips.com`).\n\
- **Responda apenas com o JSON array**, sem markdown e sem texto extra.\n\n\
Conteúdo do e-mail/thread (texto/JSON bruto):\n----------------\n{email_text}\n----------------\n\n\
Exemplo de **formato da resposta** (apenas formato, valores fictícios):\n[\n  {{\n    \"Timestamp\": \"2025-08-08T12:41:12-03:00\",\n    \"Fornecedor\": \"Hotel X <reservas@hotelx.com>\",\n    \"Assunto\": \"Parrot Trips | Cidade | Hotel X | Reveillon\",\n    \"Nome do hotel\": \"Hotel X\",\n    \"Cidade\": \"Cidade\",\n    \"Check-in\": \"21/11/2025\",\n    \"Check-out\": \"24/11/2025\",\n    \"Número de quartos\": \"10\",\n    \"Tipo de quarto\": \"Standard DBL\",\n    \"Tipo de quarto (normalizado)\": \"Duplo\",\n    \"Preço (num)\": 900.0,\n    \"Qual configuração do quarto (twin, double)\": \"double\",\n    \"Tarifa NET ou comissionada?\": \"NET\",\n    \"Taxa? Ex.: 5% de ISS\": \"5% ISS\",\n    \"Serviços incluso? Explicação: existem hotéis que consideram a tarifa de serviço já incluso e outros não.\": \"café incluído\",\n    \"Política de pagamento\": \"50% antecipado\",\n    \"Política de cancelamento\": \"até 7 dias\",\n    \"Email do fornecedor\": \"reservas@hotelx.com\",\n    \"Email do remetente (top-level)\": \"becker@parrottrips.com\"\n  }},\n  {{ /* outra configuração/preço */ }}\n]\n"""

# === Utilidades ===

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _load_if_json(txt: str) -> Optional[dict]:
    try:
        return json.loads(txt)
    except Exception:
        return None


def read_text_any(path: Path) -> str:
    """Lê como texto, mas se for JSON retorna uma versão pretty (melhor para o LLM)."""
    try:
        txt = path.read_text(encoding="utf-8", errors="ignore")
        obj = _load_if_json(txt)
        if obj is not None:
            return json.dumps(obj, ensure_ascii=False, indent=2)
        return txt
    except Exception as e:
        return f"<<ERRO AO LER ARQUIVO: {e}>>"


def extract_body_from_rawtext(raw_text: str) -> str:
    obj = _load_if_json(raw_text)
    if isinstance(obj, dict):
        if isinstance(obj.get("body"), str):
            return obj["body"]
        md = obj.get("metadata")
        if isinstance(md, dict) and isinstance(md.get("body"), str):
            return md["body"]
    return raw_text


EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", re.UNICODE)


def extract_top_from_email(body_text: str) -> str:
    head = body_text[:3000]
    for line in head.splitlines():
        if line.strip().lower().startswith("from:"):
            m = EMAIL_REGEX.search(line)
            if m:
                return m.group(0).strip()
    m = EMAIL_REGEX.search(head)
    return m.group(0).strip() if m else ""


def extract_supplier_email_heuristic(body_text: str) -> str:
    ignore_domains = {
        "parrottrips.com", "facebook.com", "instagram.com", "linkedin.com",
        "gmail.com", "googlemail.com",
    }
    candidates_priority: List[str] = []
    candidates_regular: List[str] = []
    for line in body_text.splitlines():
        emails = EMAIL_REGEX.findall(line)
        if not emails:
            continue
        for em in emails:
            domain = em.split("@")[-1].lower()
            if domain in ignore_domains:
                continue
            if line.strip().lower().startswith(("from:", "to:")):
                candidates_priority.append(em)
            else:
                candidates_regular.append(em)
    if candidates_priority:
        return candidates_priority[0]
    if candidates_regular:
        return candidates_regular[0]
    return ""


def coerce_price(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return ""
    if s.count(",") == 1 and s.count(".") > 1:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return ""


def sanitize_json_only(s: str) -> str:
    start = s.find("[")
    end = s.rfind("]")
    if start == -1 or end == -1 or end < start:
        # fallback: tenta objeto simples (alguns modelos enviam objeto único)
        start_obj = s.find("{")
        end_obj = s.rfind("}")
        if start_obj != -1 and end_obj != -1 and end_obj >= start_obj:
            return s[start_obj : end_obj + 1]
        return s
    return s[start : end + 1]


def complete_check(record: Dict[str, Any], required_fields: List[str]) -> Tuple[bool, List[str]]:
    missing = []
    for k in required_fields:
        if k not in record:
            missing.append(k)
        else:
            v = record[k]
            if v is None:
                missing.append(k)
            elif isinstance(v, str) and v.strip() == "":
                missing.append(k)
    return (len(missing) == 0, missing)


def load_env() -> None:
    load_dotenv()


# === OpenRouter (SDK OpenAI) ===

def make_client():
    from openai import OpenAI
    base_url = "https://openrouter.ai/api/v1"
    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Defina OPENROUTER_API_KEY no ambiente ou .env")
    client = OpenAI(base_url=base_url, api_key=api_key)
    return client


def call_llm(client, model: str, http_referer: str | None, x_title: str | None, email_text: str) -> str:
    extra_headers = {}
    if http_referer:
        extra_headers["HTTP-Referer"] = http_referer
    if x_title:
        extra_headers["X-Title"] = x_title

    user_prompt = USER_PROMPT_TEMPLATE.format(
        fields_json=json.dumps(HEADER_FIELDS, ensure_ascii=False, indent=2),
        email_text=email_text[:100000],
    )

    max_retries = 6
    base_delay = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            completion = client.chat.completions.create(
                extra_headers=extra_headers if extra_headers else None,
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
            )
            return completion.choices[0].message.content or ""
        except Exception as e:
            if attempt < max_retries:
                sleep_s = base_delay * (2 ** (attempt - 1))
                print(f"⚠️  LLM erro (tentativa {attempt}/{max_retries}): {e}. Retentando em {sleep_s:.1f}s...")
                time.sleep(sleep_s)
                continue
            raise


def parse_llm_to_list(text: str) -> List[Dict[str, Any]]:
    """Converte a resposta do LLM para **lista de objetos**.
    Aceita: array JSON direto; objeto único; objeto com chave \"Cotações\".
    """
    cleaned = sanitize_json_only(text).strip()
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.IGNORECASE | re.DOTALL)

    try:
        obj: Union[List[Any], Dict[str, Any]] = json.loads(cleaned)
    except Exception as e:
        raise ValueError(f"JSON parse fail: {e}")

    if isinstance(obj, list):
        # filtra apenas dicts
        return [x for x in obj if isinstance(x, dict)]

    if isinstance(obj, dict):
        if isinstance(obj.get("Cotações"), list):
            return [x for x in obj["Cotações"] if isinstance(x, dict)]
        # objeto único — embrulha em lista
        return [obj]

    return []


# === Pipeline por arquivo ===

def enrich_and_validate_quote(quote: Dict[str, Any], body_text: str) -> Dict[str, Any]:
    # Garante chaves e normaliza preço
    for field in HEADER_FIELDS:
        if field not in quote:
            quote[field] = ""
    quote["Preço (num)"] = coerce_price(quote.get("Preço (num)"))

    # Heurísticas para e-mails
    if not str(quote.get("Email do remetente (top-level)", "")).strip():
        top_from = extract_top_from_email(body_text)
        if top_from:
            quote["Email do remetente (top-level)"] = top_from

    if not str(quote.get("Email do fornecedor", "")).strip():
        supplier = extract_supplier_email_heuristic(body_text)
        if supplier:
            quote["Email do fornecedor"] = supplier

    return quote


def process_file(
    client,
    model: str,
    http_referer: str | None,
    x_title: str | None,
    path: Path,
    out_complete: Path,
    out_incomplete: Path,
) -> List[Dict[str, Any]]:
    raw_text_pretty = read_text_any(path)
    body_text = extract_body_from_rawtext(raw_text_pretty)

    # === Chamada ao LLM ===
    llm_text = call_llm(client, model, http_referer, x_title, raw_text_pretty)

    meta_base: Dict[str, Any] = {
        "_source_raw": str(path),
        "_llm_model": model,
    }

    # === Parsing p/ lista de cotações ===
    try:
        quotes = parse_llm_to_list(llm_text)
    except Exception as e:
        payload = [{
            **meta_base,
            "_error": f"JSON parse fail: {e}",
            "_llm_raw_response": llm_text[:2000],
        }]
        out_path = out_incomplete / (path.stem + "__parsed_error.json")
        out_path.write_text(json.dumps(payload[0], ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    # Se o modelo não retornou nada útil, registre um vazio
    if not quotes:
        err = {**meta_base, "_error": "EMPTY_RESULT_FROM_LLM"}
        (out_incomplete / (path.stem + "__empty_result.json")).write_text(
            json.dumps(err, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return [err]

    results: List[Dict[str, Any]] = []

    # === Enriquecimento, validação e gravação 1:1 por cotação ===
    for idx, q in enumerate(quotes, start=1):
        q = enrich_and_validate_quote(q, body_text)
        is_complete, missing = complete_check(q, HEADER_FIELDS)
        out_obj = {**meta_base, **q}
        if not is_complete:
            out_obj["_missing_fields"] = missing

        # Decide pasta e nomeia com índice
        if is_complete:
            out_path = out_complete / f"{path.stem}__extracted_{idx:02d}.json"
        else:
            out_path = out_incomplete / f"{path.stem}__extracted_incomplete_{idx:02d}.json"

        out_path.write_text(json.dumps(out_obj, ensure_ascii=False, indent=2), encoding="utf-8")
        results.append(out_obj)

    return results


def main():
    load_env()

    parser = argparse.ArgumentParser(description="Extrai **múltiplas** cotações por arquivo via OpenRouter LLM.")
    parser.add_argument("--raw_dir", default=DEFAULT_RAW_DIR, help="Diretório com arquivos brutos (dump_threads).")
    parser.add_argument("--out_complete", default=DEFAULT_COMPLETE_DIR, help="Diretório para JSONs completos.")
    parser.add_argument("--out_incomplete", default=DEFAULT_INCOMPLETE_DIR, help="Diretório para JSONs incompletos/erros.")
    parser.add_argument("--jsonl_out", default=DEFAULT_JSONL_AGG, help="Arquivo agregado JSONL (raiz do projeto).")
    parser.add_argument("--model", default=os.getenv("OPENROUTER_MODEL", "openai/gpt-4o"), help="Modelo OpenRouter (ex.: openai/gpt-4o).")
    parser.add_argument("--http_referer", default=os.getenv("OPENROUTER_HTTP_REFERER", "").strip(), help="HTTP-Referer (ranking OpenRouter).")
    parser.add_argument("--x_title", default=os.getenv("OPENROUTER_X_TITLE", "").strip(), help="X-Title (ranking OpenRouter).")
    parser.add_argument("--max_files", type=int, default=0, help="Limite opcional de arquivos para processar (0 = todos).")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_complete = Path(args.out_complete)
    out_incomplete = Path(args.out_incomplete)
    jsonl_out = Path(args.jsonl_out)

    if not raw_dir.exists():
        print(f"❌ Diretório não encontrado: {raw_dir}")
        sys.exit(1)

    ensure_dir(out_complete)
    ensure_dir(out_incomplete)

    client = make_client()

    files = sorted([p for p in raw_dir.glob("**/*") if p.is_file() and not p.name.startswith(".")])
    if args.max_files > 0:
        files = files[: args.max_files]

    if not files:
        print("⚠️  Nenhum arquivo encontrado em raw_messages/.")
        sys.exit(0)

    print(f"🧠 Extração via LLM em {len(files)} arquivo(s) de {raw_dir}/ — múltiplas cotações por arquivo habilitadas")

    aggregated: List[Dict[str, Any]] = []
    ok_quotes, bad_quotes = 0, 0

    for i, f in enumerate(files, 1):
        print(f"[{i}/{len(files)}] → {f.name}")
        try:
            out_list = process_file(
                client=client,
                model=args.model,
                http_referer=args.http_referer or None,
                x_title=args.x_title or None,
                path=f,
                out_complete=out_complete,
                out_incomplete=out_incomplete,
            )
            # agrega **cada** cotação separadamente
            for row in out_list:
                aggregated.append(row)
                if ("_missing_fields" in row) or ("_error" in row):
                    bad_quotes += 1
                else:
                    ok_quotes += 1
        except Exception as e:
            err_obj = {
                "_source_raw": str(f),
                "_llm_model": args.model,
                "_error": f"PROCESS_FAIL: {e}",
            }
            (out_incomplete / (f.stem + "__process_error.json")).write_text(
                json.dumps(err_obj, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            aggregated.append(err_obj)
            bad_quotes += 1

    # Salva agregado (uma linha por cotação)
    try:
        with jsonl_out.open("w", encoding="utf-8") as fp:
            for row in aggregated:
                fp.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"\n📦 Agregado salvo em: {jsonl_out}")
    except Exception as e:
        print(f"⚠️  Falha ao salvar JSONL agregado ({jsonl_out}): {e}")

    print(f"\n✅ Cotações completas: {ok_quotes} | ⚠️ Cotações incompletas/erros: {bad_quotes} | Total de cotações: {ok_quotes + bad_quotes}")


if __name__ == "__main__":
    main()
