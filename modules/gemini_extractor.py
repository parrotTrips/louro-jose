import os
import re
import json
from typing import Dict, List, Union
from dotenv import load_dotenv
import google.generativeai as genai

try:
    from .headers import HEADER_FIELDS as TARGET_FIELDS
except Exception:
    from modules.headers import HEADER_FIELDS as TARGET_FIELDS

load_dotenv()
_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
if not _GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY não definido no .env")

genai.configure(api_key=_GEMINI_API_KEY)
_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME", "gemini-1.5-flash").strip()

_SYSTEM_INSTRUCTIONS = (
    "Você extrai informações de cotações hoteleiras a partir de e-mails. "
    "Retorne ESTRITAMENTE:\n"
    "- Um OBJETO JSON com as chaves exatas, quando houver apenas um tipo de quarto; OU\n"
    "- Um ARRAY de OBJETOS JSON (um por tipo de quarto) quando houver múltiplos tipos.\n"
    "NUNCA inclua texto fora do JSON/ARRAY JSON. NUNCA inclua chaves não listadas.\n"
    "Se um campo não existir, use a string vazia \"\".\n"
    "O campo 'Fornecedor' deve ser o REMETENTE (nome e/ou e-mail de quem enviou)."
)

_USER_TEMPLATE = """Contexto:
O e-mail abaixo pode conter encaminhamentos e histórico. Foque na(s) mensagem(ns) em que o HOTEL/POUSADA apresenta a COTAÇÃO (preços, políticas etc.). Ignore assinaturas, redes sociais e conversas anteriores sem dados de cotação.

Campos (CHAVES EXATAS; um item por tipo de quarto):
{campos}

Metadados:
- Timestamp: {ts}
- Assunto: {subject}
- Remetente (quem enviou) → Fornecedor: {sender}

Corpo (limpo):
---
{body}
---

FORMATO DE RESPOSTA:
- Se houver APENAS UM tipo de quarto, retorne APENAS UM OBJETO JSON.
- Se houver MÚLTIPLOS tipos de quarto, retorne UM ARRAY JSON; cada item é um OBJETO com as MESMAS chaves.

Regras de preenchimento:
- "Fornecedor": SEMPRE preencher com o REMETENTE (não use destinatário).
- "Check-in" e "Check-out": usar ISO "AAAA-MM-DD" quando possível. Se houver apenas uma data, usar em "Check-in" e deixar "Check-out" como "".
- "Número de quartos": apenas dígitos (ex.: "7") referentes àquele item/tipo.
- "Tipo de quarto": denominação comercial conforme o e-mail (ex.: "Duplo luxo").
- "Tipo de quarto (normalizado)": versão em minúsculas, removendo termos como "apto/ap./apartamento", "quarto(s)", pontuação redundante e espaços extras (ex.: "duplo luxo").
- "Preço (num)": SOMENTE o número com ponto e duas casas (ex.: "508.20"); sem "R$", sem milhares.
- Demais campos: preencher quando existirem; caso contrário, "".
"""

# ----------------- Helpers -----------------

def _extract_json_block(text: str) -> str:
    """Extrai o bloco JSON (objeto ou array) de uma resposta possivelmente com rodeios/markdown."""
    t = (text or "").strip()
    m = re.search(r"```json\s*([\s\S]*?)\s*```", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    a0, a1 = t.find("["), t.rfind("]")
    if a0 != -1 and a1 != -1 and a1 > a0:
        return t[a0:a1+1].strip()

    o0, o1 = t.find("{"), t.rfind("}")
    if o0 != -1 and o1 != -1 and o1 > o0:
        return t[o0:o1+1].strip()

    return t

def _force_json_to_list(text: str) -> List[Dict[str, str]]:
    """Converte a resposta em lista de objetos (se vier objeto único, embrulha em lista)."""
    block = _extract_json_block(text)
    data = json.loads(block)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        items = []
        for it in data:
            if isinstance(it, dict):
                items.append(it)
        return items
    return []

def _strip_forwarding_noise(s: str) -> str:
    if not isinstance(s, str):
        return ""
    t = s.replace("\r\n", "\n").replace("\r", "\n")
    # blocos de encaminhamento
    t = re.sub(r"(?m)^[- ]{5,} Forwarded message [- ]{5,}\n.*?(?=\n\n|\Z)", "", t, flags=re.IGNORECASE|re.DOTALL)
    # cabeçalhos repetidos
    t = re.sub(r"(?m)^(From|De|To|Para|Subject|Assunto|Date|Data):.*$", "", t)
    # urls/assinaturas
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"(?mi)^--\s*$.*?(?=\n\S|\Z)", "", t, flags=re.DOTALL)
    # heurística: se encontrar âncoras típicas de cotação, corta a partir dali
    anchor = re.search(r"(?i)(valores sobre nossas diárias|acomodações disponíveis|diária inclui|nossos valores)", t)
    if anchor:
        t = t[anchor.start():]
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t

def _guess_hotel_city_from_subject(subject: str) -> tuple[str, str]:
    hotel, city = "", ""
    parts = [p.strip() for p in (subject or "").split("|")]
    for p in parts:
        if re.search(r"(?i)\b(hotel|pousada|resort)\b", p):
            hotel = p
    for p in parts:
        if p and not re.search(r"(?i)\b(hotel|pousada|resort|parrot trips|reveillon|cotação|cota[oõ])\b", p):
            city = p
            break
    return hotel, city

def _ensure_all_fields_dict() -> Dict[str, str]:
    return {k: "" for k in TARGET_FIELDS}

def _normalize_label(label: str) -> str:
    if not label:
        return ""
    s = str(label).lower()
    s = re.sub(r"\b(apto|apartamento|ap\.?)\b", "", s)
    s = s.replace("quarto", "").replace("quartos", "")
    s = s.replace(" - ", " ")
    s = re.sub(r"[:\-–—]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _only_digits_str(s) -> str:
    if s is None:
        return ""
    m = re.search(r"\d+", str(s))
    return m.group(0) if m else ""

def _parse_brl_price_to_float_string(val) -> str:
    """Aceita 'R$ 1.234,56' ou '1234.56' e devolve string '1234.56' (2 casas)."""
    if val is None:
        return ""
    s = str(val).strip()
    if s == "":
        return ""
    s = re.sub(r"[^\d\.,]", "", s)
    if s == "":
        return ""
    if "," in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return f"{float(s):.2f}"
    except Exception:
        return ""

def _post_coerce_item(item: Dict[str, str], meta_ts: str, meta_sender: str, meta_subject: str) -> Dict[str, str]:
    """Pós-processamento para garantir aderência ao esquema canônico."""
    coerced = _ensure_all_fields_dict()
    for k in TARGET_FIELDS:
        if k in item and item[k] is not None:
            coerced[k] = str(item[k])

    if not coerced["Timestamp"]:
        coerced["Timestamp"] = meta_ts or ""
    if not coerced["Fornecedor"]:
        coerced["Fornecedor"] = meta_sender or ""
    if not coerced["Assunto"]:
        coerced["Assunto"] = meta_subject or ""

    if not coerced["Nome do hotel"] or len(coerced["Nome do hotel"]) < 3:
        hotel, city_guess = _guess_hotel_city_from_subject(coerced["Assunto"])
        if hotel and not coerced["Nome do hotel"]:
            coerced["Nome do hotel"] = hotel
        if city_guess and not coerced["Cidade"]:
            coerced["Cidade"] = city_guess

    coerced["Número de quartos"] = _only_digits_str(coerced.get("Número de quartos", ""))

    if not coerced.get("Tipo de quarto (normalizado)"):
        coerced["Tipo de quarto (normalizado)"] = _normalize_label(coerced.get("Tipo de quarto", ""))

    coerced["Preço (num)"] = _parse_brl_price_to_float_string(coerced.get("Preço (num)", ""))

    return coerced


def extract_fields(email_record: Dict) -> List[Dict[str, str]]:
    """
    Extrai itens normalizados (um por tipo de quarto).
    Retorna lista de dicionários nas chaves TARGET_FIELDS.
    """
    # Metadados mínimos
    ts = str(email_record.get("timestamp", "")) or str(email_record.get("date", "")) or ""
    subject = email_record.get("subject", "") or email_record.get("assunto", "") or ""
    sender = email_record.get("from", "") or email_record.get("remetente", "") or ""
    raw_body = email_record.get("body", "") or email_record.get("texto", "") or email_record.get("content", "") or ""
    body = _strip_forwarding_noise(raw_body)

    # LLM
    model = genai.GenerativeModel(_MODEL_NAME, system_instruction=_SYSTEM_INSTRUCTIONS)
    user_prompt = _USER_TEMPLATE.format(
        campos="\n".join(f"- {c}" for c in TARGET_FIELDS),
        ts=ts, subject=subject, sender=sender, body=body
    )

    items: List[Dict[str, str]] = []
    try:
        resp = model.generate_content(user_prompt)

        # extrai texto de forma robusta (resp.text ou candidates/parts)
        text = (getattr(resp, "text", None) or "").strip()
        if not text:
            try:
                for cand in getattr(resp, "candidates", []) or []:
                    content = getattr(cand, "content", None)
                    parts = getattr(content, "parts", []) if content else []
                    for p in parts:
                        ptxt = getattr(p, "text", None)
                        if ptxt and ptxt.strip():
                            text = ptxt.strip()
                            break
                    if text:
                        break
            except Exception:
                pass

        if text:
            parsed_list = _force_json_to_list(text)
            # pós-coerção/garantia de esquema
            for it in parsed_list:
                items.append(_post_coerce_item(it, ts, sender, subject))

    except Exception:
        # se der erro no LLM, ainda retornamos um item mínimo com metadados
        fail_min = _ensure_all_fields_dict()
        fail_min["Timestamp"] = ts
        fail_min["Fornecedor"] = sender
        fail_min["Assunto"] = subject
        items.append(fail_min)

    # se o LLM retornou nada válido, devolve ao menos uma linha com metadados
    if not items:
        min_row = _ensure_all_fields_dict()
        min_row["Timestamp"] = ts
        min_row["Fornecedor"] = sender
        min_row["Assunto"] = subject
        items = [min_row]

    return items
