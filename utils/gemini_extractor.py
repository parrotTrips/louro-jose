import os
import re
import json
from typing import Dict, List, Optional
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()
_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
if not _GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY não definido no .env")

genai.configure(api_key=_GEMINI_API_KEY)
_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME", "gemini-1.5-flash").strip()

TARGET_FIELDS = [
    "Timestamp",
    "Destinatário",
    "Assunto",
    "Nome do hotel",
    "Cidade",
    "Número de quartos disponível",
    "Qual configuração do quarto (twin, double)",
    "Qual tipo de quarto (standard, luxo, superior…)",
    "Preço por tipo de quarto",
    "Tarifa NET ou comissionada?",
    "Taxa? Ex.: 5% de ISS",
    "Serviços incluso? Explicação: existem hotéis que consideram a tarifa de serviço já incluso e outros não.",
    "Data da hospedagem",
    "Política de pagamento",
    "Política de cancelamento",
]

_SYSTEM_INSTRUCTIONS = (
    "Você extrai informações de cotações hoteleiras a partir de encadeamentos de e-mail. "
    "Retorne ESTRITAMENTE um JSON com as chaves exatas fornecidas. "
    "Quando a informação não existir, use a string vazia \"\". "
    "Nunca inclua comentários, texto extra ou arrays fora do JSON."
)

_USER_TEMPLATE = """Contexto:
O e-mail abaixo pode conter encaminhamentos e histórico. Foque na(s) mensagem(ns) em que o HOTEL/POUSADA apresenta a COTAÇÃO (preços, políticas etc.). Ignore assinaturas, redes sociais e conversas anteriores que não tragam dados de cotação.

Campos a extrair (chaves EXATAS):
{campos}

Metadados:
- Timestamp: {ts}
- Destinatário: {to}
- Assunto: {subject}
- Remetente: {sender}

Corpo (limpo):
---
{body}
---

Regras de formatação:
- Retorne APENAS um objeto JSON.
- Se um campo for múltiplo (ex.: vários tipos de quarto/preços), concatene em uma única string separada por '; '.
  Ex.: "Standard: R$ 379,80; Luxo: R$ 508,20; Suíte Luxo: R$ 654,00"
- "Tarifa NET ou comissionada?": detectar termos como "NET", "comissionada" (se não claro, retornar "").
- "Taxa?": capturar menções como "5% ISS", "10% taxa de serviço" etc. Se houver várias, concatene.
- "Serviços incluso?": mencionar itens como "café da manhã", "estacionamento", etc., se explícitos.
- "Data da hospedagem": preferir ISO (AAAA-MM-DD) ou intervalo (AAAA-MM-DD a AAAA-MM-DD).
- "Número de quartos disponível": se o e-mail listar várias categorias, colocar um resumo textual (ex.: "07 duplo luxo; 06 duplo standard; ..."), ou "" se não existir.
"""

# ----------------- Helpers de parsing/limpeza -----------------

def _force_json(text: str) -> Dict:
    text = (text or "").strip()
    m = re.search(r"```json\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
    if m:
        text = m.group(1).strip()
    b0, b1 = text.find("{"), text.rfind("}")
    if b0 != -1 and b1 != -1 and b1 > b0:
        text = text[b0:b1+1]
    return json.loads(text)

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
    """Heurística simples baseada no padrão '... | Cidade | Hotel Nome | ...'"""
    hotel, city = "", ""
    parts = [p.strip() for p in (subject or "").split("|")]
    for p in parts:
        if re.search(r"(?i)\b(hotel|pousada|resort)\b", p):
            hotel = p
    # cidade: preferimos um token que não seja 'Parrot Trips', 'Reveillon', que não contenha 'hotel/pousada'
    for p in parts:
        if p and not re.search(r"(?i)\b(hotel|pousada|resort|parrot trips|reveillon|cotação|cota[oõ])\b", p):
            city = p
            break
    return hotel, city

def _ensure_all_fields_dict() -> Dict[str, str]:
    return {k: "" for k in TARGET_FIELDS}

# ----------------- Função principal -----------------

def extract_fields(email_record: Dict) -> Dict[str, str]:
    # Metadados mínimos (preenchidos deterministicamente)
    ts = str(email_record.get("timestamp", "")) or str(email_record.get("date", "")) or ""
    subject = email_record.get("subject", "") or email_record.get("assunto", "") or ""
    to_field = email_record.get("to", "") or email_record.get("recipient", "") or email_record.get("destinatario", "") or ""
    sender = email_record.get("from", "") or email_record.get("remetente", "") or ""
    raw_body = email_record.get("body", "") or email_record.get("texto", "") or email_record.get("content", "") or ""
    body = _strip_forwarding_noise(raw_body)

    # 1) Tentar o LLM
    model = genai.GenerativeModel(_MODEL_NAME, system_instruction=_SYSTEM_INSTRUCTIONS)
    user_prompt = _USER_TEMPLATE.format(
        campos="\n".join(f"- {c}" for c in TARGET_FIELDS),
        ts=ts, to=to_field, subject=subject, sender=sender, body=body
    )

    parsed = _ensure_all_fields_dict()
    try:
        resp = model.generate_content(user_prompt)
        text = (getattr(resp, "text", None) or "").strip()
        if not text:
            # fallback candidatos
            for cand in getattr(resp, "candidates", []) or []:
                parts = cand.get("content", {}).get("parts", [])
                for p in parts:
                    if "text" in p and p["text"]:
                        text = p["text"].strip()
                        if text:
                            break
                if text:
                    break
        if text:
            llm_data = _force_json(text)
            for k in TARGET_FIELDS:
                v = llm_data.get(k, "")
                parsed[k] = "" if v is None else str(v)
    except Exception:
        # mantém parsed só com "" por enquanto; completaremos abaixo
        pass

    # 2) Preencher garantidos pelos metadados (sempre prevalecem se vazios)
    if not parsed["Timestamp"]:
        parsed["Timestamp"] = ts
    if not parsed["Destinatário"]:
        parsed["Destinatário"] = to_field
    if not parsed["Assunto"]:
        parsed["Assunto"] = subject

    # 3) Heurísticas úteis de subject/body para hotel/cidade (se LLM não preencheu)
    if not parsed["Nome do hotel"] or len(parsed["Nome do hotel"]) < 3:
        hotel, city_guess = _guess_hotel_city_from_subject(subject)
        if hotel and not parsed["Nome do hotel"]:
            parsed["Nome do hotel"] = hotel
        if city_guess and not parsed["Cidade"]:
            parsed["Cidade"] = city_guess

    # 4) Se nada de cidade veio e o assunto tem algo como " | Paraty | "
    if not parsed["Cidade"]:
        # Tentativa simples: checar palavras com inicial maiúscula no subject
        m = re.search(r"\|\s*([A-ZÁÂÃÀÉÊÍÓÔÕÚÇ][\wÁÂÃÀÉÊÍÓÔÕÚÇáâãàéêíóôõúç ]{2,})\s*\|", subject)
        if m:
            parsed["Cidade"] = m.group(1).strip()

    # 5) Garantir que todas as chaves existam (e sejam str)
    for k in TARGET_FIELDS:
        v = parsed.get(k, "")
        parsed[k] = "" if v is None else str(v)

    return parsed
