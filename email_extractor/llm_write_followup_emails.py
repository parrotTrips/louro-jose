from __future__ import annotations

import os
import sys
import glob
import json
import re
import time
import unicodedata
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Optional

from dotenv import load_dotenv
import requests

sys.path.append("..")
load_dotenv("../.env")

from modules.json_utils import ensure_dir

INCOMPLETE_DIR = "incomplete_data"
DRAFTS_DIR = "draft_emails"

OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "gpt-4o-mini").strip()
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_BASE = os.getenv("OPENROUTER_BASE", "https://openrouter.ai/api/v1").strip()

DEFAULT_FROM_NAME = os.getenv("PARROT_FROM_NAME", "Equipe Parrot Trips").strip()
DEFAULT_FROM_EMAIL = os.getenv("PARROT_FROM_EMAIL", "").strip()
DEFAULT_CC = os.getenv("PARROT_DEFAULT_CC", "").strip()

# -------------------- util: normalização/regex --------------------

_EMAIL_RE = re.compile(
    r'(?:"?([^"]*)"?\s*)<([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})>|'
    r'([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})'
)

def _extract_emails(s: str) -> List[str]:
    out: List[str] = []
    for m in _EMAIL_RE.finditer(s or ""):
        email = m.group(2) or m.group(3)
        if email and "://" not in email:
            out.append(email)
    seen = set()
    uniq = []
    for e in out:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    return uniq

def _parse_name(s: str) -> str:
    m = re.search(r'^"?([^"<]+?)"?\s*<', s or "")
    return m.group(1).strip() if m else ""

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

def _slug(s: str) -> str:
    s = _norm(s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "sem-nome"

# -------------------- mapeamento de perguntas --------------------

QUESTION_TEMPLATES = {
    "Taxa": "Existe alguma taxa adicional? Exemplos: ISS (5%), taxa de serviço ou taxa de turismo. Se houver, poderia detalhar o percentual e se já está incluída no preço?",
    "Taxas": "Existe alguma taxa adicional? Exemplos: ISS (5%), taxa de serviço ou taxa de turismo. Se houver, poderia detalhar o percentual e se já está incluída no preço?",
    "Taxa? Ex.: 5% de ISS": "Há alguma taxa aplicada (ex.: ISS 5%, taxa de serviço, turismo)? Poderia confirmar os percentuais e se estão incluídos?",
    "Política de cancelamento": "Qual é a política de cancelamento para o grupo (prazos, multas e condições de reembolso)?",
    "Política de Cancelamento": "Qual é a política de cancelamento para o grupo (prazos, multas e condições de reembolso)?",
    "Forma de pagamento": "Quais são as formas de pagamento aceitas e os prazos? Há necessidade de pré-pagamento/depósito? Se sim, quando e em qual percentual?",
    "Política de pagamento": "Quais são as condições e prazos de pagamento? Há necessidade de pré-pagamento/depósito? Se sim, quando e em qual percentual?",
    "Tarifa NET ou comissionada?": "A tarifa é NET ou comissionada? Em caso de comissionada, qual o percentual?",
    "Serviços incluso?": "O que está incluído na diária? (por exemplo: café da manhã, taxas, Wi-Fi).",
    "Serviços incluso? Explicação: existem hotéis que consideram a tarifa de serviço já incluso e outros não.": "Quais serviços estão incluídos na diária (ex.: café da manhã, taxas, Wi-Fi)? A tarifa de serviço já está incluída?",
    "Preço por tipo de quarto": "Poderiam detalhar o preço por categoria/tipo de quarto (SGL/DBL/TWIN/TRIPLO) e se os valores são por apartamento ou por pessoa?",
    "Número de quartos": "Quantos quartos estão disponíveis nas datas solicitadas?",
    "Qual configuração do quarto (twin, double)": "A configuração dos quartos disponíveis pode ser twin, casal ou outra? Poderiam confirmar?",
    "Qual tipo de quarto (standard, luxo, superior…)": "Quais tipos/categorias de quarto estão disponíveis (standard, superior, luxo…)?",
    "Data de hospedagem": "Poderiam confirmar as datas de check-in e check-out para essa cotação?",
    "Check-in": "Poderiam confirmar a data de check-in?",
    "Check-out": "Poderiam confirmar a data de check-out?",
    "Validade da proposta": "Qual a validade desta cotação?",
    "Preço (num)": "Poderiam informar o preço por noite para as categorias solicitadas? Se possível, detalhe por SGL/DBL/TWIN/TRIPLO.",
    "Tipo de quarto (normalizado)": "Poderiam confirmar os tipos/categorias de quarto disponíveis (por exemplo: duplo, twin, triplo) em formato padronizado?",
}

def question_for_field(field_name: str) -> str:
    s = field_name.strip()
    if s in QUESTION_TEMPLATES:
        return QUESTION_TEMPLATES[s]
    if s.lower().startswith("serviços incluso"):
        return QUESTION_TEMPLATES["Serviços incluso?"]
    if s.lower().startswith("forma de pagamento"):
        return QUESTION_TEMPLATES["Forma de pagamento"]
    return f"Poderiam informar o campo “{field_name}”?"

# -------------------- prompt do LLM --------------------

SYSTEM_INSTRUCTIONS = (
    "Você é um assistente que redige e-mails curtos, claros e simpáticos em Português (Brasil) "
    "para solicitar informações de cotações de hospedagem que ficaram pendentes. "
    "Não invente dados; apenas peça o que estiver faltando."
)

def build_followup_prompt(context: Dict) -> str:
    supplier_name = context.get("supplier_name") or ""
    orig_subj = context.get("original_subject") or ""
    missing_questions = context.get("missing_questions") or []
    from_name = context.get("from_name") or DEFAULT_FROM_NAME
    bullets = "\n".join(f"- {q}" for q in missing_questions)
    return f"""
Escreva um e-mail profissional, curto e simpático em Português (Brasil).
Objetivo: pedir informações que ficaram pendentes em uma cotação de hospedagem.

Diretrizes:
- Tom cordial e direto; evite jargões.
- Primeiro parágrafo: agradeça a resposta/cotação e diga que, na conferência, notamos que alguns pontos ficaram em aberto.
- Em seguida, apresente uma lista em bullets com perguntas específicas (use exatamente as perguntas dadas).
- Inclua uma frase gentil pedindo retorno e, se fizer sentido, “favor responder para todos em cópia”.
- Assine como “{from_name} — Parrot Trips”.
- Não use Markdown; apenas texto simples.
- Responda em JSON com as chaves: "subject" (string) e "body" (string). Não inclua campos extras.
- Se houver “assunto original”, reutilize parte para manter o contexto, mas sem prefixos técnicos (ex.: “Re:” só se apropriado).

Dados:
- Nome do contato/fornecedor (se houver): "{supplier_name}"
- Assunto original (se houver): "{orig_subj}"

Perguntas (use exatamente estas, em bullets):
{bullets}
""".strip()

# -------------------- OpenRouter --------------------

def _openrouter_request(messages: List[Dict[str, str]],
                        model: str,
                        max_retries: int = 5,
                        timeout: int = 60) -> Dict:
    if not OPENROUTER_API_KEY:
        raise SystemExit("⛔ OPENROUTER_API_KEY não definido no .env")

    url = f"{OPENROUTER_BASE}/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://parrottrips.com",
        "X-Title": "ParrotTrips-Followups",
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.2,
        "response_format": { "type": "text" },
    }

    backoff = 1.5
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else backoff ** attempt
                time.sleep(min(delay, 15))
                continue
            raise RuntimeError(f"OpenRouter HTTP {resp.status_code}: {resp.text[:500]}")
        except requests.RequestException:
            if attempt == max_retries:
                raise
            time.sleep(min(backoff ** attempt, 15))
    raise RuntimeError("Falha ao contatar OpenRouter após várias tentativas.")

def _call_llm_followup(prompt: str) -> Dict[str, str]:
    messages = [
        {"role": "system", "content": SYSTEM_INSTRUCTIONS},
        {"role": "user", "content": prompt},
    ]
    data = _openrouter_request(messages, model=OPENROUTER_MODEL)
    text = ""
    try:
        choices = data.get("choices") or []
        if choices:
            msg = choices[0].get("message") or {}
            text = (msg.get("content") or "").strip()
    except Exception:
        text = ""

    if not text:
        return {"subject": "Informações pendentes da cotação", "body": ""}

    m = re.search(r"```json\s*([\s\S]*?)```", text, flags=re.I)
    raw = m.group(1).strip() if m else text
    o0, o1 = raw.find("{"), raw.rfind("}")
    if o0 != -1 and o1 != -1 and o1 > o0:
        raw = raw[o0:o1+1]
    try:
        data = json.loads(raw)
        subj = str(data.get("subject", "")).strip() or "Informações pendentes da cotação"
        body = str(data.get("body", "")).strip()
        return {"subject": subj, "body": body}
    except Exception:
        return {"subject": "Informações pendentes da cotação", "body": text}

# -------------------- helpers: payload -> infos --------------------

def _get_missing_fields(payload: Dict) -> List[str]:
    """
    Preferir _missing_fields. Fallback para _missing_fields_per_item (lista de listas).
    """
    if isinstance(payload.get("_missing_fields"), list):
        return [str(x) for x in payload["_missing_fields"]]
    # fallback antigo:
    missing_per_item = payload.get("_missing_fields_per_item") or []
    fields, seen = [], set()
    for lst in missing_per_item:
        for f in lst:
            if f not in seen:
                seen.add(f); fields.append(f)
    return fields

def _friendly_supplier_name(payload: Dict) -> str:
    # tenta nomes amigáveis
    for k in ("Fornecedor", "supplier_name", "supplier"):
        v = (payload.get(k) or "").strip()
        if v:
            name = _parse_name(v)
            return name or v
    # tenta from do meta
    pm = payload.get("_picked_email_meta") or {}
    name = _parse_name(pm.get("from", "")) or ""
    # se ainda vazio, tenta Nome do hotel
    if not name:
        name = (payload.get("Nome do hotel") or "").strip()
    return name

def _supplier_email(payload: Dict) -> Optional[str]:
    # Prioridade 1: campo direto
    for k in ("Email do fornecedor", "supplier_email", "email_fornecedor"):
        v = (payload.get(k) or "").strip()
        if v:
            emails = _extract_emails(v)
            if emails:
                return emails[0]

    # Prioridade 2: thread meta
    thr = payload.get("thread")
    if thr and isinstance(thr.get("emails"), list):
        for e in reversed(thr["emails"]):
            senders = _extract_emails(e.get("sender", ""))
            if senders:
                return senders[-1]

    # Prioridade 3: varredura em campos comuns
    for k in ("Fornecedor", "from", "sender", "recipient", "to", "cc", "body"):
        v = (payload.get(k) or "").strip()
        if v:
            emails = _extract_emails(v)
            if emails:
                return emails[0]
    return None

def _original_subject(payload: Dict) -> str:
    for k in ("Assunto", "subject", "_subject"):
        v = (payload.get(k) or "").strip()
        if v:
            return v
    pm = payload.get("_picked_email_meta") or {}
    return (pm.get("subject") or "").strip()

def _group_key(payload: Dict) -> Tuple[str, str]:
    """
    Chave de agrupamento:
      1) Preferir e-mail do fornecedor (domínio/pessoa específica)
      2) Fallback: (Nome do hotel + Cidade)
    """
    email = _supplier_email(payload) or ""
    if email:
        return ("email", email.lower())
    hotel = (payload.get("Nome do hotel") or "").strip()
    city = (payload.get("Cidade") or "").strip()
    if hotel or city:
        return ("hotel_city", f"{_norm(hotel)}|{_norm(city)}")
    # fallback final: assunto normalizado (raro)
    subj = _original_subject(payload)
    return ("subject", _norm(subj))

def _pick_to_email(payloads: List[Dict]) -> Optional[str]:
    emails = []
    for p in payloads:
        e = _supplier_email(p)
        if e:
            emails.append(e.lower())
    if not emails:
        return None
    most_common = Counter(emails).most_common(1)[0][0]
    return most_common

def _pick_supplier_name(payloads: List[Dict]) -> str:
    # tenta pelo nome do fornecedor/hotel mais frequente
    names = []
    for p in payloads:
        n = _friendly_supplier_name(p)
        if n:
            names.append(n)
    if names:
        return Counter(names).most_common(1)[0][0]
    # fallback: Nome do hotel
    hotels = []
    for p in payloads:
        h = (p.get("Nome do hotel") or "").strip()
        if h:
            hotels.append(h)
    if hotels:
        return Counter(hotels).most_common(1)[0][0]
    return "parceiro"

def _pick_original_subject(payloads: List[Dict]) -> str:
    # reusa um assunto representativo
    subs = []
    for p in payloads:
        s = _original_subject(p)
        if s:
            subs.append(s)
    if subs:
        return subs[0]
    return "Parrot Trips | Informações pendentes"

def _collect_missing_fields(payloads: List[Dict]) -> List[str]:
    seen = set()
    fields: List[str] = []
    for p in payloads:
        for f in _get_missing_fields(p):
            if f not in seen:
                seen.add(f)
                fields.append(f)
    return fields

# -------------------- salvar drafts --------------------

def _save_draft(base_name: str,
                to_email: Optional[str],
                cc: str,
                subject: str,
                body: str) -> Tuple[str, str]:
    ensure_dir(DRAFTS_DIR)
    json_path = os.path.join(DRAFTS_DIR, f"{base_name}_draft.json")
    txt_path = os.path.join(DRAFTS_DIR, f"{base_name}_draft.txt")
    data = {
        "to": to_email or "",
        "cc": cc or "",
        "from_name": DEFAULT_FROM_NAME,
        "from_email": DEFAULT_FROM_EMAIL,
        "subject": subject,
        "body": body,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    with open(txt_path, "w", encoding="utf-8") as f:
        if to_email:
            f.write(f"Para: {to_email}\n")
        if cc:
            f.write(f"Cc: {cc}\n")
        f.write(f"Assunto: {subject}\n\n")
        f.write(body.strip() + "\n")
    return json_path, txt_path

# -------------------- main (agrupado por hotel/fornecedor) --------------------

def main():
    if not OPENROUTER_API_KEY:
        raise SystemExit("⛔ OPENROUTER_API_KEY não definido no .env")

    files = sorted(glob.glob(os.path.join(INCOMPLETE_DIR, "*.json")))
    if not files:
        print(f"⛔ Nenhum arquivo .json encontrado em {INCOMPLETE_DIR}/")
        return

    # 1) Carrega e agrupa os payloads por grupo (fornecedor/hotel)
    groups: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
    total_payloads = 0
    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            total_payloads += 1
        except Exception as e:
            print(f"⚠️ Erro ao ler {os.path.basename(path)}: {e}")
            continue
        key = _group_key(payload)
        groups[key].append(payload)

    print(f"✉️  Gerando e-mails de follow-up para {len(groups)} grupo(s) (a partir de {total_payloads} arquivo(s) incompletos)…")

    created = 0
    for key, payloads in groups.items():
        # 2) Consolida perguntas e metadados
        missing_fields = _collect_missing_fields(payloads)
        if not missing_fields:
            # nada a perguntar neste grupo
            continue

        questions = [question_for_field(f) for f in missing_fields]
        supplier_name = _pick_supplier_name(payloads)
        orig_subject = _pick_original_subject(payloads)
        to_email = _pick_to_email(payloads)

        prompt_ctx = {
            "supplier_name": supplier_name,
            "original_subject": orig_subject,
            "missing_questions": questions,
            "from_name": DEFAULT_FROM_NAME,
        }
        reply = _call_llm_followup(build_followup_prompt(prompt_ctx))

        # 3) Subject/body padrão se LLM falhar
        subject_llm = reply.get("subject") or ""
        body_llm = (reply.get("body") or "").strip()

        # Subject sugerido se vazio: mantem o contexto e indica consolidação
        if not subject_llm:
            base_subj = orig_subject or f"Parrot Trips | {supplier_name}"
            subject_llm = f"{base_subj} — Informações pendentes (consolidado)"

        if not body_llm:
            body_llm = (
                f"Olá {supplier_name or 'time'},\n\n"
                "Tudo bem? Obrigado pelas cotações enviadas. Durante a conferência, notamos que alguns pontos ficaram pendentes:\n"
                + "".join(f"- {q}\n" for q in questions) +
                "\nPoderiam, por favor, nos confirmar essas informações? Agradecemos desde já!\n\n"
                f"{DEFAULT_FROM_NAME}\nParrot Trips"
            )

        # 4) Nome de arquivo por grupo (evita duplicar por hotel/fornecedor)
        if key[0] == "email":
            base = f"group__by_email__{_slug(key[1])}"
        elif key[0] == "hotel_city":
            base = f"group__by_hotelcity__{_slug(key[1])}"
        else:
            base = f"group__by_subject__{_slug(key[1])}"

        jpath, tpath = _save_draft(
            base_name=base,
            to_email=to_email,
            cc=DEFAULT_CC,
            subject=subject_llm,
            body=body_llm,
        )
        created += 1
        print(f"✅ Draft criado (grupo): {os.path.basename(jpath)} | {os.path.basename(tpath)}  → To: {to_email or '(vazio)'}")

    print(f"🏁 Pronto! {created} draft(s) consolidado(s) em {DRAFTS_DIR}/")

if __name__ == "__main__":
    main()
