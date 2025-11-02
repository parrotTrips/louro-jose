# src/app/parser/llm_extract.py
from __future__ import annotations
import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Union

import requests  # pip install requests
from dotenv import load_dotenv  # pip install python-dotenv

# Carrega variáveis do .env (não sobrescreve o que já estiver no ambiente)
load_dotenv(override=False)

# Campos esperados (alinhe com a sua planilha "quotes_raw")
HEADER_FIELDS: List[str] = [
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
    "Configuração do quarto",
    "Preço (num)",
    "Tarifa NET ou comissionada?",
    "Taxa? Ex.: 5% de ISS",
    "Serviços incluso? Explicação: existem hotéis que consideram a tarifa de serviço já incluso e outros não.",
    "Política de pagamento",
    "Política de cancelamento",
    "Email do fornecedor",
    "Email do remetente (top-level)",
]

SYSTEM_PROMPT = (
    "Você extrai **cotações de hotel** de e-mails/threads.\n"
    "Responda com **apenas um JSON** válido (array de objetos).\n"
    "Cada combinação distinta de **categoria/configuração de quarto e preço** vira **um objeto**.\n"
    "Se um campo não existir, retorne string vazia \"\" (exceto `Preço (num)`: número ou \"\").\n"
    "‘Descrição dos Quartos’ deve ser curta e específica daquela cotação (sem preços/políticas gerais).\n"
)

USER_PROMPT_TEMPLATE = """Extraia as cotações do conteúdo abaixo.

Regras:
- Saída: **um único JSON** no formato **lista (array) de objetos**.
- **Uma cotação por combinação** de **categoria/configuração e preço**.
- Use **exatamente** estas chaves em **cada objeto**:
{fields_json}
- Não invente valores. Use "" quando ausente. Em `Preço (num)`, use número ou "".
- Datas podem manter o formato encontrado.
- `Email do remetente (top-level)`: e-mail do primeiro cabeçalho From do topo.
- `Email do fornecedor`: e-mail do hotel/fornecedor (não @parrottrips.com quando possível).
- Responda **apenas com o JSON array**, sem markdown.

Conteúdo (contexto e corpo):
----------------
From: {email_from}
Subject: {subject}
Date: {date}

{body}
----------------
"""

def _sanitize_json_only(s: str) -> str:
    start = s.find("["); end = s.rfind("]")
    if start != -1 and end != -1 and end >= start:
        return s[start:end+1]
    start = s.find("{"); end = s.rfind("}")
    if start != -1 and end != -1 and end >= start:
        return s[start:end+1]
    return s

def _parse_llm_json(text: str) -> List[Dict[str, Any]]:
    cleaned = _sanitize_json_only(text).strip()
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.I | re.S)
    obj: Union[List[Any], Dict[str, Any]] = json.loads(cleaned)
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        return [obj]
    return []

def _coerce_price(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return ""
    # trata "1.234,56" vs "1,234.56"
    s = s.replace(".", "").replace(",", ".") if s.count(",") == 1 and s.count(".") > 1 else s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return ""

def _align_fields(q: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: (q.get(k, "") if q.get(k, "") is not None else "") for k in HEADER_FIELDS}
    out["Preço (num)"] = _coerce_price(out.get("Preço (num)"))
    return out

def _openrouter_headers() -> Dict[str, str]:
    key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not key:
        raise RuntimeError("Defina OPENROUTER_API_KEY no ambiente/.env")
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    # headers opcionais de ranking do OpenRouter
    http_referer = os.getenv("OPENROUTER_HTTP_REFERER", "").strip()
    x_title = os.getenv("OPENROUTER_X_TITLE", "").strip()
    if http_referer:
        headers["HTTP-Referer"] = http_referer
    if x_title:
        headers["X-Title"] = x_title
    return headers

def _post_openrouter(payload: Dict[str, Any]) -> Dict[str, Any]:
    base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/")
    url = f"{base_url}/chat/completions"
    headers = _openrouter_headers()

    last_err: Optional[Exception] = None
    for i in range(6):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code >= 400:
                try:
                    err = resp.json()
                except Exception:
                    err = {"error": resp.text}
                raise RuntimeError(f"OpenRouter HTTP {resp.status_code}: {err}")
            return resp.json()
        except Exception as e:
            last_err = e
            if i < 5:
                time.sleep(2.0 * (2 ** i))
            else:
                raise
    raise RuntimeError(f"Falha ao chamar OpenRouter: {last_err}")

def extract_quotes_from_context(
    subject: str,
    email_from: str,
    date: str,
    body: str,
    model: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Retorna lista de cotações já alinhadas a HEADER_FIELDS, chamando o OpenRouter via HTTP.
    Lê OPENROUTER_API_KEY / OPENROUTER_MODEL / OPENROUTER_BASE_URL do ambiente (.env).
    """
    model = model or os.getenv("OPENROUTER_MODEL", "openai/gpt-4o")
    user_prompt = USER_PROMPT_TEMPLATE.format(
        fields_json=json.dumps(HEADER_FIELDS, ensure_ascii=False, indent=2),
        email_from=email_from or "",
        subject=subject or "",
        date=date or "",
        body=(body or "")[:100000],
    )

    payload = {
        "model": model,
        "temperature": 0.0,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    }

    data = _post_openrouter(payload)
    try:
        txt = data["choices"][0]["message"]["content"]
    except Exception:
        txt = json.dumps(data, ensure_ascii=False)

    parsed = _parse_llm_json(txt or "")
    if not parsed:
        return []
    return [_align_fields(q) for q in parsed]
