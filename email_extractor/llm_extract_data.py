#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
llm_extract_data.py
-------------------
Lê arquivos de `raw_messages/`, extrai campos via LLM (OpenRouter) e salva:
  - complete_data/: JSONs completos (todos os HEADER_FIELDS presentes e não vazios)
  - incomplete_data/: JSONs incompletos (lista _missing_fields) OU erros de parsing/LLM

Também gera um agregado `extracted_data.jsonl` na raiz do projeto (conveniente para análises).

Uso:
  python3 llm_extract_data.py
  python3 llm_extract_data.py --raw_dir raw_messages --out_complete complete_data --out_incomplete incomplete_data \
      --model openai/gpt-4o --max_files 500

Requisitos:
  - pip install python-dotenv openai==1.* (SDK compatível com OpenRouter)
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
from typing import Any, Dict, List, Tuple, Optional

from dotenv import load_dotenv

# === Config de pastas padrão ===
DEFAULT_RAW_DIR = "raw_messages"
DEFAULT_COMPLETE_DIR = "complete_data"
DEFAULT_INCOMPLETE_DIR = "incomplete_data"
DEFAULT_JSONL_AGG = "extracted_data.jsonl"

# === Campos a serem extraídos (na ordem fornecida por você) ===
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
    # >>> Novos campos solicitados <<<
    "Email do fornecedor",
    "Email do remetente (top-level)",
]

# === Prompt do LLM ===
SYSTEM_PROMPT = (
    "Você é um assistente que extrai dados estruturados de e-mails de cotações de hotéis. "
    "Retorne **apenas** um JSON único, sem comentários ou texto extra."
)

USER_PROMPT_TEMPLATE = """Extraia os seguintes campos do conteúdo abaixo. Se um campo não existir, deixe como string vazia "".

Campos exigidos (use exatamente esses nomes de chave):
{fields_json}

Conteúdo do e-mail/thread (texto/JSON bruto):
----------------
{email_text}
----------------

Regras:
- Responda **apenas** com um objeto JSON único, sem markdown, sem explicações.
- Mantenha os nomes das chaves exatamente como fornecidos.
- “Preço (num)” deve ser número (pode usar ponto como separador decimal). Caso não haja, deixe "".
- Datas podem permanecer no formato encontrado; não invente valores.
- “Email do remetente (top-level)” é o e-mail que aparece no PRIMEIRO cabeçalho "From:" do topo do corpo.
- “Email do fornecedor” é o e-mail do hotel/fornecedor (normalmente um domínio diferente de parrottrips.com). Se houver vários, priorize o principal vinculado às tarifas da hospedagem.
"""

# === Utilidades ===
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _load_if_json(txt: str) -> Optional[dict]:
    try:
        return json.loads(txt)
    except Exception:
        return None

def read_text_any(path: Path) -> str:
    """
    Lê o conteúdo do arquivo como texto. Tenta:
    - Se for JSON, retorna pretty string do próprio JSON (útil para LLM)
    - Caso contrário, lê como texto bruto
    """
    try:
        txt = path.read_text(encoding="utf-8", errors="ignore")
        obj = _load_if_json(txt)
        if obj is not None:
            return json.dumps(obj, ensure_ascii=False, indent=2)
        return txt
    except Exception as e:
        return f"<<ERRO AO LER ARQUIVO: {e}>>"

def extract_body_from_rawtext(raw_text: str) -> str:
    """
    Tenta localizar um campo 'body' dentro de um JSON embutido no raw_text (se houver).
    Caso contrário, retorna o raw_text original.
    """
    obj = _load_if_json(raw_text)
    if isinstance(obj, dict):
        # Tenta caminhos comuns para body
        # Ex.: { "metadata": { "body": "..." } } ou { "body": "..." }
        if "body" in obj and isinstance(obj["body"], str):
            return obj["body"]
        md = obj.get("metadata")
        if isinstance(md, dict) and isinstance(md.get("body"), str):
            return md["body"]
    return raw_text

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", re.UNICODE)

def extract_top_from_email(body_text: str) -> str:
    """
    Pega o primeiro e-mail que apareça numa linha iniciada por 'From:' no topo do corpo.
    Ex.: 'From: Nome <email@dominio.com>'
    """
    # Considera apenas o início do corpo (primeiros ~3 blocos 'Forwarded message' inclusive)
    head = body_text[:3000]
    # Procura linhas com 'From:'
    for line in head.splitlines():
        if line.strip().lower().startswith("from:"):
            m = EMAIL_REGEX.search(line)
            if m:
                return m.group(0).strip()
    # fallback: primeiro e-mail do texto
    m = EMAIL_REGEX.search(head)
    return m.group(0).strip() if m else ""

def extract_supplier_email_heuristic(body_text: str) -> str:
    """
    Heurística simples para e-mail do fornecedor:
      - Procura por e-mails cujo domínio NÃO seja 'parrottrips.com'
      - Ignora domínios de redes sociais comuns.
      - Prioriza os que aparecem em linhas com 'From:' ou 'To:'.
    """
    ignore_domains = {
        "parrottrips.com", "facebook.com", "instagram.com", "linkedin.com",
        "gmail.com", "googlemail.com"
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
    """
    Converte “Preço (num)” para float se possível,
    mantendo string vazia se não for viável.
    """
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return ""
    # normalização simples de vírgula/ponto
    if s.count(",") == 1 and s.count(".") > 1:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return ""

def sanitize_json_only(s: str) -> str:
    """
    O modelo pode retornar texto extra. Pegamos o primeiro '{' e o último '}'.
    """
    start = s.find("{")
    end = s.rfind("}")
    if start == -1 or end == -1 or end < start:
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
    """
    Chama o LLM com retries exponenciais.
    Retorna o conteúdo bruto (string) da resposta do modelo.
    """
    extra_headers = {}
    if http_referer:
        extra_headers["HTTP-Referer"] = http_referer
    if x_title:
        extra_headers["X-Title"] = x_title

    user_prompt = USER_PROMPT_TEMPLATE.format(
        fields_json=json.dumps(HEADER_FIELDS, ensure_ascii=False, indent=2),
        email_text=email_text[:100000]  # proteção para inputs gigantes
    )

    max_retries = 6
    base_delay = 2.0  # segundos
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

def parse_llm_json(text: str) -> Dict[str, Any]:
    text = sanitize_json_only(text).strip()
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE | re.DOTALL)
    return json.loads(text)

# === Pipeline ===
def process_file(
    client,
    model: str,
    http_referer: str | None,
    x_title: str | None,
    path: Path,
    out_complete: Path,
    out_incomplete: Path,
) -> Dict[str, Any]:
    raw_text_pretty = read_text_any(path)
    body_text = extract_body_from_rawtext(raw_text_pretty)

    llm_text = call_llm(client, model, http_referer, x_title, raw_text_pretty)
    result: Dict[str, Any]
    meta: Dict[str, Any] = {
        "_source_raw": str(path),
        "_llm_model": model,
    }

    try:
        result = parse_llm_json(llm_text)
    except Exception as e:
        payload = {
            **meta,
            "_error": f"JSON parse fail: {e}",
            "_llm_raw_response": llm_text[:2000],
        }
        out_path = out_incomplete / (path.stem + "__parsed_error.json")
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    # Garante todas as chaves no objeto final
    for field in HEADER_FIELDS:
        if field not in result:
            result[field] = ""

    # Coerção de preço
    result["Preço (num)"] = coerce_price(result.get("Preço (num)"))

    # ===== Fallbacks / heurísticas específicas para os novos campos =====
    # 1) Email do remetente (top-level)
    if not result.get("Email do remetente (top-level)", "").strip():
        top_from = extract_top_from_email(body_text)
        if top_from:
            result["Email do remetente (top-level)"] = top_from

    # 2) Email do fornecedor
    if not result.get("Email do fornecedor", "").strip():
        supplier = extract_supplier_email_heuristic(body_text)
        if supplier:
            result["Email do fornecedor"] = supplier

    # Avalia completude
    is_complete, missing = complete_check(result, HEADER_FIELDS)
    out_obj = {**meta, **result}
    if not is_complete:
        out_obj["_missing_fields"] = missing

    # Decide pasta
    if is_complete:
        out_path = out_complete / (path.stem + "__extracted.json")
    else:
        out_path = out_incomplete / (path.stem + "__extracted_incomplete.json")

    out_path.write_text(json.dumps(out_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_obj

def main():
    load_env()

    parser = argparse.ArgumentParser(description="Extrai dados de threads em raw_messages via OpenRouter LLM.")
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

    print(f"🧠 Extração via LLM em {len(files)} arquivo(s) de {raw_dir}/")
    aggregated = []
    ok, bad = 0, 0

    for i, f in enumerate(files, 1):
        print(f"[{i}/{len(files)}] → {f.name}")
        try:
            out_obj = process_file(
                client=client,
                model=args.model,
                http_referer=args.http_referer or None,
                x_title=args.x_title or None,
                path=f,
                out_complete=out_complete,
                out_incomplete=out_incomplete,
            )
            aggregated.append(out_obj)
            if "_missing_fields" in out_obj or "_error" in out_obj:
                bad += 1
            else:
                ok += 1
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
            bad += 1

    try:
        with jsonl_out.open("w", encoding="utf-8") as fp:
            for row in aggregated:
                fp.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"\n📦 Agregado salvo em: {jsonl_out}")
    except Exception as e:
        print(f"⚠️  Falha ao salvar JSONL agregado ({jsonl_out}): {e}")

    print(f"\n✅ Completos: {ok} | ⚠️ Incompletos/erros: {bad} | Total: {len(files)}")


if __name__ == "__main__":
    main()
