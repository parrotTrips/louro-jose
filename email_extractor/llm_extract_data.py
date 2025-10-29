#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
llm_extract_data.py (versão multi-cotações, sem aggregated JSONL)
-----------------------------------------------------------------
- Troca/Padronização de campos:
    "Tipo de quarto (normalizado)"                -> "Categoria do quarto"
    "Qual configuração do quarto (twin, double)"  -> "Configuração do quarto"
    "Descrição de Valores" / "Descrição de Quartos" / "Descrição do Quarto"
                                                   -> "Descrição dos Quartos"
      (foco em tipos/configurações/capacidades/observações — não em preços)

Lê arquivos de `raw_messages/`, consulta um LLM (OpenRouter) e extrai **uma ou mais cotações**
por arquivo — uma para **cada combinação distinta de categoria/configuração de quarto e preço**.

Saídas por cotação:
  - complete_data/: JSON completo (todos os HEADER_FIELDS)
  - incomplete_data/: JSON incompleto (com _missing_fields) ou erros de parsing

Uso:
  python3 llm_extract_data.py
  python3 llm_extract_data.py --raw_dir raw_messages --out_complete complete_data --out_incomplete incomplete_data \
      --model openai/gpt-4o --max_files 500 --log_level INFO --log_file logs/extract.log

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
import unicodedata
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Union

from dotenv import load_dotenv
from modules.headers import HEADER_FIELDS

# === logging ===
LOGGER_NAME = "llm_extract"
logger = logging.getLogger(LOGGER_NAME)

def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(lvl)
    logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(lvl)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(lvl)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

# === Config de pastas padrão ===
DEFAULT_RAW_DIR = "raw_messages"
DEFAULT_COMPLETE_DIR = "complete_data"
DEFAULT_INCOMPLETE_DIR = "incomplete_data"

# === Prompt do LLM (ATUALIZADO) ===
SYSTEM_PROMPT = (
    "Você extrai **cotações de hotel** de e-mails.\n"
    "Sempre responda com **apenas um JSON** válido.\n"
    "Cada combinação distinta de **categoria/configuração de quarto e preço** deve virar **um objeto separado**.\n"
    "Se algum campo não existir, use string vazia \"\" (exceto `Preço (num)`, que deve ser número ou \"\").\n"
    "\n"
    "Definições:\n"
    "- **Categoria do quarto**: a classe comercial do quarto (p.ex.: standard, luxo, superior, deluxe, premium, master).\n"
    "- **Configuração do quarto**: arranjo de leitos/ocupação (p.ex.: twin/duas de solteiro, double/uma de casal, "
    "  1 casal + 1 solteiro, 3 solteiros, triplo, quádruplo, king, queen). Capture números e tipos de camas quando houver.\n"
    "\n"
    "Campo **Descrição dos Quartos** (obrigatório e **específico da cotação**):\n"
    "- Deve conter **apenas a descrição referente à categoria/configuração daquela cotação** (uma linha/bullet curto),\n"
    "  **não** copie o bloco inteiro que lista todas as categorias.\n"
    "- Se houver um bloco com várias categorias, selecione **somente** o trecho da categoria correspondente;\n"
    "  se houver sublinhas por configuração (SGL/DBL, twin/double, triplo), escolha a sublinha correta.\n"
    "- Se não houver trecho específico, **sintetize** curto a partir dos campos (ex.: `Standard: SGL/DBL`).\n"
    "- **Não inclua preços**. Remova símbolos e números de preço se vierem misturados.\n"
    "- Inclua apenas observações **intrínsecas ao quarto** (vista, metragem, tipo/qtde de camas, capacidade, berço/cama extra);\n"
    "  não repita itens gerais como café da manhã, taxas, política de pagamento/cancelamento.\n"
)

USER_PROMPT_TEMPLATE = """Extraia as cotações do conteúdo abaixo.

Regras obrigatórias:
- Saída deve ser **um único JSON** no formato **lista de objetos** (array).
- **Uma cotação por combinação distinta** de **categoria/configuração de quarto e preço**.
- Use **exatamente** estes nomes de chaves em **cada objeto**:
{fields_json}
- Datas podem manter o formato encontrado. Não invente valores.
- `Preço (num)` deve ser numérico (ponto decimal) quando houver; caso contrário, use "".
- `Email do remetente (top-level)` é o e-mail do **primeiro** cabeçalho "From:" no topo do corpo.
- `Email do fornecedor` é o e-mail do hotel/fornecedor (geralmente não `parrottrips.com`).
- **Responda apenas com o JSON array**, sem markdown e sem texto extra.

Instruções específicas para **Descrição dos Quartos** (**coluna I**, preencher sempre e de forma **específica**):
- Quando houver bloco com múltiplas categorias, selecione **somente** a linha/trecho da **categoria** e, se aplicável, da **configuração** correspondentes àquela cotação.
- Se não houver linha específica, **sintetize** curto a partir de categoria/configuração: ex. `Standard: SGL/DBL`.
- **Não** inclua preços nem itens gerais (café da manhã, taxas, políticas).

Exemplo de **formato da resposta** (apenas formato, valores fictícios):
[
  {{
    "Timestamp": "2025-08-08T12:41:12-03:00",
    "Fornecedor": "Hotel X <reservas@hotelx.com>",
    "Assunto": "Parrot Trips | Cidade | Hotel X | Reveillon",
    "Nome do hotel": "Hotel X",
    "Cidade": "Cidade",
    "Check-in": "21/11/2025",
    "Check-out": "24/11/2025",
    "Número de quartos": "10",
    "Descrição dos Quartos": "Standard: SGL/DBL; ~25 m²; vista cidade.",
    "Categoria do quarto": "Standard",
    "Preço (num)": 900.0,
    "Configuração do quarto": "SGL/DBL",
    "Tarifa NET ou comissionada?": "NET",
    "Taxa? Ex.: 5% de ISS": "5% ISS",
    "Serviços incluso? Explicação: existem hotéis que consideram a tarifa de serviço já incluso e outros não.": "café incluído",
    "Política de pagamento": "50% antecipado",
    "Política de cancelamento": "até 7 dias",
    "Email do fornecedor": "reservas@hotelx.com",
    "Email do remetente (top-level)": "becker@parrottrips.com"
  }}
]

Conteúdo do e-mail/thread (texto/JSON bruto):
----------------
{email_text}
----------------
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
                logger.warning(f"LLM erro (tentativa {attempt}/{max_retries}): {e}. Retentando em {sleep_s:.1f}s...")
                time.sleep(sleep_s)
                continue
            logger.exception("Falha definitiva ao chamar o LLM")
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
        return [x for x in obj if isinstance(x, dict)]

    if isinstance(obj, dict):
        if isinstance(obj.get("Cotações"), list):
            return [x for x in obj["Cotações"] if isinstance(x, dict)]
        return [obj]

    return []

# === Helpers de pós-processamento ===
_PRICE_TOKEN_RE = re.compile(
    r"(R\$\s?\d[\d\.\,]*|\$\s?\d[\d\.\,]*|\b\d{1,3}(\.\d{3})*(,\d+)?\b)",
    re.IGNORECASE,
)

HOTEL_WIDE_TERMS = (
    "café da manhã", "cafe da manha",
    "taxa", "iss", "serviço", "servico",
    "política de pagamento", "politica de pagamento",
    "política de cancelamento", "politica de cancelamento",
    "pré pagamento", "pre pagamento", "pré-pagamento", "pre-pagamento",
    "no show", "faturamento", "boleto", "pix", "cartão", "cartao", "depósito", "deposito"
)

def strip_price_tokens(text: str) -> str:
    """Remove tokens de preço de descrições."""
    if not isinstance(text, str) or not text.strip():
        return text
    cleaned = _PRICE_TOKEN_RE.sub("", text)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    cleaned = "\n".join(line.rstrip() for line in cleaned.splitlines())
    return cleaned.strip()

def _norm(s: str) -> str:
    """Normaliza para comparação (lower, sem acento)."""
    s = s or ""
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s

def _line_split_chunks(text: str) -> List[str]:
    """Separa bloco em linhas/itens curtos: por quebras de linha e bullets."""
    if not text:
        return []
    parts = re.split(r"(?:\n|\r|\r\n|^)\s*[•\-–]\s*|[\r\n]+", text)
    parts = [p.strip(" \t;,.") for p in parts if p and p.strip()]
    joined: List[str] = []
    buf = ""
    for p in parts:
        if not buf:
            buf = p
        else:
            if (not re.search(r"[.;:]$", buf)) and re.match(r"^[a-zà-ú0-9]", _norm(p)):
                buf = f"{buf} {p}"
            else:
                joined.append(buf.strip())
                buf = p
    if buf:
        joined.append(buf.strip())
    return joined

def _config_keywords(cfg: str) -> List[str]:
    n = _norm(cfg)
    keys: set[str] = set()
    if not n:
        return []
    if "sgl" in n or "single" in n or "individual" in n:
        keys.update(["sgl", "single", "individual", "single/individual"])
    if "dbl" in n or "duplo" in n or "double" in n or "casal" in n:
        keys.update(["dbl", "duplo", "double", "casal", "s/d"])
    if "twin" in n or "duas camas" in n or "2 twin" in n or "solteiro" in n:
        keys.update(["twin", "2 twin", "duas camas", "solteiro", "2 solteiro", "duas de solteiro"])
    if "trip" in n or "tripl" in n or "3" in n:
        keys.update(["triplo", "triple", "3", "trp"])
    if "quad" in n or "quadru" in n or "4" in n:
        keys.update(["quadruplo", "quadruple", "4", "qdp"])
    if "king" in n:
        keys.update(["king"])
    if "queen" in n:
        keys.update(["queen"])
    if "casal" in n:
        keys.update(["casal"])
    return sorted(keys)

def _category_match_score(line: str, categoria: str) -> int:
    nline = _norm(line); ncat = _norm(categoria); score = 0
    if ncat and ncat in nline:
        score += 2
    aliases = {
        "standard": ["std", "standard"],
        "superior": ["superior"],
        "luxo": ["luxo", "deluxe", "lux"],
        "deluxe": ["deluxe", "luxo"],
        "classic": ["classic"],
        "master": ["master"],
        "premium": ["premium"],
    }
    for k, vals in aliases.items():
        if k in ncat and any(val in nline for val in vals):
            score += 1
    if re.match(r"^(categoria|apto\.?|apartamento|standard|superior|luxo|deluxe|classic)\b", nline):
        score += 1
    return score

def _config_match_score(line: str, cfg: str) -> int:
    nline = _norm(line); keys = _config_keywords(cfg)
    if not keys:
        return 0
    score = 0
    for k in keys:
        if k in nline:
            score += 1
    if any(w in nline for w in ["single", "individual"]):
        score += 1 if any(w in _norm(cfg) for w in ["single", "individual", "sgl"]) else 0
    if any(w in nline for w in ["duplo", "double", "casal"]):
        score += 1 if any(w in _norm(cfg) for w in ["duplo", "double", "casal", "dbl"]) else 0
    if "twin" in nline:
        score += 1 if "twin" in _norm(cfg) else 0
    if "triplo" in nline or "triple" in nline:
        score += 1 if any(w in _norm(cfg) for w in ["trip", "tripl"]) else 0
    if "quadru" in nline or "4" in nline:
        score += 1 if any(w in _norm(cfg) for w in ["quadru", "4"]) else 0
    return score

def _remove_hotel_wide_info(s: str) -> str:
    n = _norm(s)
    for term in HOTEL_WIDE_TERMS:
        if term in n:
            sentences = re.split(r"(?<=[.!?])\s+|\s*;\s*|\s*\|\s*", s)
            keep = [t for t in sentences if _norm(t).find(term) == -1]
            s = "; ".join([t.strip() for t in keep if t.strip()])
            n = _norm(s)
    return s.strip()

def refine_description_for_quote(desc_block: str, categoria: str, cfg: str) -> str:
    if not desc_block:
        return ""
    desc_block = strip_price_tokens(desc_block)
    desc_block = _remove_hotel_wide_info(desc_block)
    lines = _line_split_chunks(desc_block)
    if not lines:
        return ""
    scored: List[Tuple[int, int, str]] = []
    for ln in lines:
        if not ln.strip():
            continue
        cat_score = _category_match_score(ln, categoria)
        cfg_score = _config_match_score(ln, cfg)
        total = cat_score * 3 + cfg_score
        if total > 0:
            scored.append((total, -len(ln), ln))
    if scored:
        scored.sort(reverse=True)
        return scored[0][2].strip()
    only_cat: List[Tuple[int, int, str]] = []
    for ln in lines:
        cat_score = _category_match_score(ln, categoria)
        if cat_score > 0:
            only_cat.append((cat_score, -len(ln), ln))
    if only_cat:
        only_cat.sort(reverse=True)
        return only_cat[0][2].strip()
    cat = (categoria or "").strip(); c = (cfg or "").strip()
    if cat and c: return f"{cat}: {c}"
    if cat: return f"{cat}"
    if c: return f"{c}"
    return ""

def synthesize_room_description(quote: Dict[str, Any]) -> str:
    cat = str(quote.get("Categoria do quarto", "") or "").strip()
    cfg = str(quote.get("Configuração do quarto", "") or "").strip()
    if cat and cfg: return f"{cat}: {cfg}"
    if cat: return f"{cat}"
    if cfg: return f"{cfg}"
    return ""

# === Pipeline por arquivo ===
def enrich_and_validate_quote(quote: Dict[str, Any], body_text: str) -> Dict[str, Any]:
    for field in HEADER_FIELDS:
        if field not in quote:
            quote[field] = ""
    quote["Preço (num)"] = coerce_price(quote.get("Preço (num)"))
    if not str(quote.get("Email do remetente (top-level)", "")).strip():
        top_from = extract_top_from_email(body_text)
        if top_from:
            quote["Email do remetente (top-level)"] = top_from
    if not str(quote.get("Email do fornecedor", "")).strip():
        supplier = extract_supplier_email_heuristic(body_text)
        if supplier:
            quote["Email do fornecedor"] = supplier
    raw_desc = str(quote.get("Descrição dos Quartos", "") or "")
    desc = strip_price_tokens(raw_desc)
    desc_refined = refine_description_for_quote(
        desc_block=desc,
        categoria=str(quote.get("Categoria do quarto", "") or ""),
        cfg=str(quote.get("Configuração do quarto", "") or "")
    ).strip()
    if not desc_refined:
        desc_refined = synthesize_room_description(quote)
    quote["Descrição dos Quartos"] = desc_refined
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
    body_text = extract_body_from_rawtext(raw_text_pretty)  # fix typo

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
        logger.error(f"[{path.name}] Erro de parsing do JSON do LLM: {e}")
        return payload

    if not quotes:
        err = {**meta_base, "_error": "EMPTY_RESULT_FROM_LLM"}
        (out_incomplete / (path.stem + "__empty_result.json")).write_text(
            json.dumps(err, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.warning(f"[{path.name}] Resposta vazia do LLM.")
        return [err]

    results: List[Dict[str, Any]] = []

    for idx, q in enumerate(quotes, start=1):
        q = enrich_and_validate_quote(q, body_text)
        is_complete, missing = complete_check(q, HEADER_FIELDS)
        out_obj = {**meta_base, **q}
        if not is_complete:
            out_obj["_missing_fields"] = missing

        if is_complete:
            out_path = out_complete / f"{path.stem}__extracted_{idx:02d}.json"
            logger.info(f"[{path.name}] Cotação {idx:02d}: COMPLETA → {out_path.name}")
        else:
            out_path = out_incomplete / f"{path.stem}__extracted_incomplete_{idx:02d}.json"
            logger.info(f"[{path.name}] Cotação {idx:02d}: INCOMPLETA (faltam: {', '.join(missing)}) → {out_path.name}")

        out_path.write_text(json.dumps(out_obj, ensure_ascii=False, indent=2), encoding="utf-8")
        results.append(out_obj)

    return results

def main():
    load_env()

    parser = argparse.ArgumentParser(
        description="Extrai **múltiplas** cotações por arquivo via OpenRouter LLM (campos atualizados, descrição específica por cotação)."
    )
    parser.add_argument("--raw_dir", default=DEFAULT_RAW_DIR, help="Diretório com arquivos brutos (dump_threads).")
    parser.add_argument("--out_complete", default=DEFAULT_COMPLETE_DIR, help="Diretório para JSONs completos.")
    parser.add_argument("--out_incomplete", default=DEFAULT_INCOMPLETE_DIR, help="Diretório para JSONs incompletos/erros.")
    parser.add_argument("--model", default=os.getenv("OPENROUTER_MODEL", "openai/gpt-4o"), help="Modelo OpenRouter (ex.: openai/gpt-4o).")
    parser.add_argument("--http_referer", default=os.getenv("OPENROUTER_HTTP_REFERER", "").strip(), help="HTTP-Referer (ranking OpenRouter).")
    parser.add_argument("--x_title", default=os.getenv("OPENROUTER_X_TITLE", "").strip(), help="X-Title (ranking OpenRouter).")
    parser.add_argument("--max_files", type=int, default=0, help="Limite opcional de arquivos para processar (0 = todos).")
    parser.add_argument("--log_level", default=os.getenv("LOG_LEVEL", "INFO"), help="Nível de log (DEBUG, INFO, WARNING, ERROR).")
    parser.add_argument("--log_file", default=os.getenv("LOG_FILE", ""), help="Caminho opcional para salvar o log em arquivo.")
    args = parser.parse_args()

    setup_logging(args.log_level, args.log_file if args.log_file else None)

    raw_dir = Path(args.raw_dir)
    out_complete = Path(args.out_complete)
    out_incomplete = Path(args.out_incomplete)

    if not raw_dir.exists():
        logger.error(f"Diretório não encontrado: {raw_dir}")
        sys.exit(1)

    ensure_dir(out_complete)
    ensure_dir(out_incomplete)

    try:
        client = make_client()
    except Exception as e:
        logger.exception("Falha criando cliente OpenRouter")
        sys.exit(2)

    files = sorted([p for p in raw_dir.glob("**/*") if p.is_file() and not p.name.startswith(".")])
    if args.max_files > 0:
        files = files[: args.max_files]

    if not files:
        logger.warning("Nenhum arquivo encontrado em raw_messages/.")
        sys.exit(0)

    logger.info(f"🧠 Processando {len(files)} arquivo(s) de {raw_dir}/ — múltiplas cotações por arquivo (campos novos)")

    ok_quotes, bad_quotes, total_files = 0, 0, len(files)

    for i, f in enumerate(files, 1):
        logger.info(f"[{i}/{total_files}] → {f.name}")
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
            for row in out_list:
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
            logger.exception(f"[{f.name}] Falha no processamento do arquivo")
            bad_quotes += 1

    logger.info(
        f"✅ Cotações completas: {ok_quotes} | ⚠️ Incompletas/erros: {bad_quotes} | Total de cotações: {ok_quotes + bad_quotes}"
    )

if __name__ == "__main__":
    main()
