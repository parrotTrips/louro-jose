"""
- Troca/Padronização de campos:
    "Tipo de quarto (normalizado)"                -> "Categoria do quarto"
    "Qual configuração do quarto (twin, double)"  -> "Configuração do quarto"
    "Descrição de Valores" / "Descrição de Quartos" / "Descrição do Quarto"
                                                   -> "Descrição dos Quartos"
      (foco em tipos/configurações/capacidades/observações — não em preços)

Lê arquivos de `raw_messages/`, consulta um LLM (OpenRouter) e extrai **uma ou mais cotações**
por arquivo — uma para **cada combinação distinta de categoria/configuração de quarto e preço**.

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
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Union

from dotenv import load_dotenv

# === Config de pastas padrão ===
DEFAULT_RAW_DIR = "raw_messages"
DEFAULT_COMPLETE_DIR = "complete_data"
DEFAULT_INCOMPLETE_DIR = "incomplete_data"
DEFAULT_JSONL_AGG = "extracted_data.jsonl"

# === Campos a serem extraídos (por cotação) — ATUALIZADOS ===
HEADER_FIELDS: List[str] = [
    "Timestamp",
    "Fornecedor",
    "Assunto",
    "Nome do hotel",
    "Cidade",
    "Check-in",
    "Check-out",
    "Número de quartos",
    "Descrição dos Quartos",                      # texto específico da cotação (categoria/config/observações do quarto) — sem preços
    "Categoria do quarto",
    "Preço (num)",
    "Configuração do quarto",
    "Tarifa NET ou comissionada?",
    "Taxa? Ex.: 5% de ISS",
    "Serviços incluso? Explicação: existem hotéis que consideram a tarifa de serviço já incluso e outros não.",
    "Política de pagamento",
    "Política de cancelamento",
    "Email do fornecedor",
    "Email do remetente (top-level)",
]

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
    # BR: "1.234,56" | US: "1,234.56" | simples: "1234,56" or "1234.56"
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
        # fallback: tenta objeto simples
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
        return [x for x in obj if isinstance(x, dict)]

    if isinstance(obj, dict):
        if isinstance(obj.get("Cotações"), list):
            return [x for x in obj["Cotações"] if isinstance(x, dict)]
        return [obj]

    return []


# === Compatibilidade retroativa de chaves antigas -> novas ===

OLD_TO_NEW_KEYS = {
    "Tipo de quarto (normalizado)": "Categoria do quarto",
    "Qual configuração do quarto (twin, double)": "Configuração do quarto",
    "Descrição de Valores": "Descrição dos Quartos",
    "Descrição de Quartos": "Descrição dos Quartos",
    "Descrição do Quarto": "Descrição dos Quartos",
}

def normalize_key_aliases(d: Dict[str, Any]) -> Dict[str, Any]:
    if not d:
        return d
    out = dict(d)
    for old_k, new_k in OLD_TO_NEW_KEYS.items():
        if old_k in out and new_k not in out:
            out[new_k] = out.pop(old_k)
    return out


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
    """Separa bloco em linhas/cartos curtos: por quebras de linha e bullets."""
    if not text:
        return []
    # quebra em bullets '•' ou hífens ou quebras
    parts = re.split(r"(?:\n|\r|\r\n|^)\s*[•\-–]\s*|[\r\n]+", text)
    parts = [p.strip(" \t;,.") for p in parts if p and p.strip()]
    # juntar linhas muito curtas que provavelmente foram quebradas no meio
    joined: List[str] = []
    buf = ""
    for p in parts:
        if not buf:
            buf = p
        else:
            # Heurística: se terminou sem ponto e a próxima começa minúscula, pode ser continuação
            if (not re.search(r"[.;:]$", buf)) and re.match(r"^[a-zà-ú0-9]", _norm(p)):
                buf = f"{buf} {p}"
            else:
                joined.append(buf.strip())
                buf = p
    if buf:
        joined.append(buf.strip())
    return joined

def _config_keywords(cfg: str) -> List[str]:
    """Extrai palavras-chave de configuração e seus sinônimos comuns."""
    n = _norm(cfg)
    keys: set[str] = set()
    if not n:
        return []
    # básicos
    if "sgl" in n or "single" in n or "individual" in n:
        keys.update(["sgl", "single", "individual", "single/individual"])
    if "dbl" in n or "duplo" in n or "double" in n or "casal" in n:
        keys.update(["dbl", "duplo", "double", "casal", "s/d"])  # s/d às vezes aparece como abreviação
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
    nline = _norm(line)
    ncat = _norm(categoria)
    score = 0
    # match direto do nome da categoria
    if ncat and ncat in nline:
        score += 2
    # reforços por aliases comuns
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
        if k in ncat:
            if any(val in nline for val in vals):
                score += 1
    # pistas de que é cabeçalho de categoria
    if re.match(r"^(categoria|apto\.?|apartamento|standard|superior|luxo|deluxe|classic)\b", nline):
        score += 1
    return score

def _config_match_score(line: str, cfg: str) -> int:
    nline = _norm(line)
    keys = _config_keywords(cfg)
    if not keys:
        return 0
    score = 0
    for k in keys:
        if k in nline:
            score += 1
    # padrões de ocupação
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
            # remove a sentença inteira contendo o termo
            sentences = re.split(r"(?<=[.!?])\s+|\s*;\s*|\s*\|\s*", s)
            keep = [t for t in sentences if _norm(t).find(term) == -1]
            s = "; ".join([t.strip() for t in keep if t.strip()])
            n = _norm(s)
    return s.strip()

def refine_description_for_quote(desc_block: str, categoria: str, cfg: str) -> str:
    """
    Recebe um bloco (às vezes com todas as categorias) e devolve apenas
    a linha/trecho mais relevante para a categoria/config da cotação.
    Se nada combinar, sintetiza a partir de categoria+config.
    """
    if not desc_block:
        return ""

    # 1) limpar preços e infos de hotel-wide
    desc_block = strip_price_tokens(desc_block)
    desc_block = _remove_hotel_wide_info(desc_block)

    # 2) separar em linhas/itens
    lines = _line_split_chunks(desc_block)
    if not lines:
        return ""

    # 3) pontuar linhas por categoria/config
    scored: List[Tuple[int, int, str]] = []  # (score_total, -len(line), line)
    for ln in lines:
        if not ln.strip():
            continue
        cat_score = _category_match_score(ln, categoria)
        cfg_score = _config_match_score(ln, cfg)
        total = cat_score * 3 + cfg_score  # dar mais peso para categoria
        if total > 0:
            scored.append((total, -len(ln), ln))

    if scored:
        scored.sort(reverse=True)
        best = scored[0][2].strip()
        return best

    # 4) fallback: se nenhuma linha casou, tentar uma linha com categoria apenas
    only_cat: List[Tuple[int, int, str]] = []
    for ln in lines:
        cat_score = _category_match_score(ln, categoria)
        if cat_score > 0:
            only_cat.append((cat_score, -len(ln), ln))
    if only_cat:
        only_cat.sort(reverse=True)
        return only_cat[0][2].strip()

    # 5) fallback final: sintetizar curto
    cat = categoria.strip()
    c = cfg.strip()
    if cat and c:
        return f"{cat}: {c}"
    if cat:
        return f"{cat}"
    if c:
        return f"{c}"
    return ""


def synthesize_room_description(quote: Dict[str, Any]) -> str:
    """Se o LLM não preencheu 'Descrição dos Quartos' ou não casou nada,
    sintetiza uma mínima usando campos já extraídos (categoria/config)."""
    cat = str(quote.get("Categoria do quarto", "") or "").strip()
    cfg = str(quote.get("Configuração do quarto", "") or "").strip()
    if cat and cfg:
        return f"{cat}: {cfg}"
    if cat:
        return f"{cat}"
    if cfg:
        return f"{cfg}"
    return ""


# === Pipeline por arquivo ===

def enrich_and_validate_quote(quote: Dict[str, Any], body_text: str) -> Dict[str, Any]:
    # Normaliza possíveis chaves antigas para as novas
    quote = normalize_key_aliases(quote)

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

    # Preenchimento/limpeza de "Descrição dos Quartos"
    raw_desc = str(quote.get("Descrição dos Quartos", "") or "")
    desc = strip_price_tokens(raw_desc)

    # NOVO: refinar para a linha específica da categoria/config
    desc_refined = refine_description_for_quote(
        desc_block=desc,
        categoria=str(quote.get("Categoria do quarto", "") or ""),
        cfg=str(quote.get("Configuração do quarto", "") or "")
    ).strip()

    if not desc_refined:
        # fallback de síntese curta
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
    body_text = extract_body_from_rawtext(raw_text_prety := raw_text_pretty)  # mantém raw para debug

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

    parser = argparse.ArgumentParser(description="Extrai **múltiplas** cotações por arquivo via OpenRouter LLM (campos atualizados, descrição específica por cotação).")
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

    print(f"🧠 Extração via LLM em {len(files)} arquivo(s) de {raw_dir}/ — múltiplas cotações por arquivo habilitadas (campos novos)")

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
