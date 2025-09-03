from __future__ import annotations

import os
import sys
import glob
import json
import re
from typing import Dict, List, Tuple, Optional

from dotenv import load_dotenv
import google.generativeai as genai

# carregar .env da raiz e permitir imports do pacote modules
sys.path.append("..")
load_dotenv("../.env")

from modules.json_utils import ensure_dir  # noqa: E402

# Pastas
INCOMPLETE_DIR = "incomplete_data"
DRAFTS_DIR = "email_drafts"

# Env (mesmos usados antes)
GEMINI_MODEL = os.getenv("GEMINI_MODEL_NAME", "gemini-1.5-flash").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

# Env opcionais para metadados do e-mail
DEFAULT_FROM_NAME = os.getenv("PARROT_FROM_NAME", "Equipe Parrot Trips").strip()
DEFAULT_FROM_EMAIL = os.getenv("PARROT_FROM_EMAIL", "").strip()
DEFAULT_CC = os.getenv("PARROT_DEFAULT_CC", "").strip()  # ex: "ai.suppliers@parrottrips.com, compras@parrottrips.com"

# -------------------- util: parsing de e-mails --------------------

# **CORRIGIDO**: exige '@' dentro de <...> e ignora URLs
_EMAIL_RE = re.compile(r'(?:"?([^"]*)"?\s*)<([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})>|([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})')

def _extract_emails(s: str) -> List[str]:
    out: List[str] = []
    for m in _EMAIL_RE.finditer(s or ""):
        email = m.group(2) or m.group(3)
        if email and "://" not in email:
            out.append(email)
    # dedup preservando ordem
    seen = set()
    uniq = []
    for e in out:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    return uniq

def _domain(addr: str) -> str:
    m = re.search(r"@([\w\.-]+)$", addr or "")
    return m.group(1).lower() if m else ""

def _parse_name(s: str) -> str:
    m = re.search(r'^"?([^"<]+?)"?\s*<', s or "")
    return m.group(1).strip() if m else ""

# -------------------- campos → perguntas específicas --------------------

QUESTION_TEMPLATES = {
    "Taxa": "Existe alguma taxa adicional? Exemplos: ISS (5%), taxa de serviço ou taxa de turismo. Se houver, poderia detalhar o percentual e se já está incluída no preço?",
    "Taxas": "Existe alguma taxa adicional? Exemplos: ISS (5%), taxa de serviço ou taxa de turismo. Se houver, poderia detalhar o percentual e se já está incluída no preço?",
    "Taxa? Ex.: 5% de ISS": "Há alguma taxa aplicada (ex.: ISS 5%, taxa de serviço, turismo)? Poderia confirmar os percentuais e se estão incluídos?",
    "Política de cancelamento": "Qual é a política de cancelamento para o grupo (prazos, multas e condições de reembolso)?",
    "Política de Cancelamento": "Qual é a política de cancelamento para o grupo (prazos, multas e condições de reembolso)?",
    "Forma de pagamento": "Quais são as formas de pagamento aceitas e os prazos? Há necessidade de pré-pagamento/depósito? Se sim, quando e em qual percentual?",
    "Tarifa NET ou comissionada?": "A tarifa é NET ou comissionada? Em caso de comissionada, qual o percentual?",
    "Serviços incluso?": "O que está incluído na diária? (por exemplo: café da manhã, taxas, Wi-Fi).",
    "Preço por tipo de quarto": "Poderiam detalhar o preço por categoria/tipo de quarto (SGL/DBL/TWIN/TRIPLO) e se os valores são por apartamento ou por pessoa?",
    "Número de quartos": "Quantos quartos estão disponíveis nas datas solicitadas?",
    "Qual configuração do quarto (twin, double)": "A configuração dos quartos disponíveis pode ser twin, casal ou outra? Poderiam confirmar?",
    "Qual tipo de quarto (standard, luxo, superior…)": "Quais tipos/categorias de quarto estão disponíveis (standard, superior, luxo…)?",
    "Data de hospedagem": "Poderiam confirmar as datas de check-in e check-out para essa cotação?",
    "Check-in": "Poderiam confirmar a data de check-in?",
    "Check-out": "Poderiam confirmar a data de check-out?",
    "Validade da proposta": "Qual a validade desta cotação?",
}

def question_for_field(field_name: str) -> str:
    return QUESTION_TEMPLATES.get(field_name.strip(), f"Poderiam informar o campo “{field_name}”?")

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

# -------------------- chamada ao LLM --------------------

def _call_llm_followup(prompt: str) -> Dict[str, str]:
    model = genai.GenerativeModel(GEMINI_MODEL, system_instruction=SYSTEM_INSTRUCTIONS)
    resp = model.generate_content(prompt)
    text = (getattr(resp, "text", None) or "").strip()
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

# -------------------- helpers do processed --------------------

def _union_missing_fields(payload: Dict) -> List[str]:
    missing = payload.get("_missing_fields_per_item") or []
    fields: List[str] = []
    seen = set()
    for lst in missing:
        for f in lst:
            if f not in seen:
                seen.add(f); fields.append(f)
    return fields

def _friendly_supplier_name(payload: Dict) -> str:
    pm = payload.get("_picked_email_meta") or {}
    name = _parse_name(pm.get("from", "")) or ""
    if name:
        return name
    items = payload.get("_items_extracted") or []
    if items:
        return (items[0].get("Fornecedor") or "").strip()
    return ""

def _original_subject(payload: Dict) -> str:
    pm = payload.get("_picked_email_meta") or {}
    return (pm.get("subject") or "").strip()

def _guess_to_from_processed(payload: Dict) -> Tuple[Optional[str], Optional[str]]:
    """
    PRIORIDADE:
      1) _guessed_supplier.email (preenchido pelo extrator) — geralmente o sender do último e-mail.
      2) fallback: últimos remetentes/recipients externos da thread
      3) fallback: objeto 'email' (recipient/to/cc/body)
    """
    gs = payload.get("_guessed_supplier") or {}
    if isinstance(gs, dict) and gs.get("email"):
        return gs.get("email"), (gs.get("name") or None)

    thr = payload.get("thread")
    if thr and isinstance(thr.get("emails"), list) and thr["emails"]:
        emails = thr["emails"]
        for e in reversed(emails):
            senders = _extract_emails(e.get("sender", ""))
            if senders:
                cand = senders[-1]
                return cand, _parse_name(e.get("sender", "")) or None
        for e in reversed(emails):
            recips = _extract_emails(e.get("recipient", ""))
            for r in reversed(recips):
                return r, None

    em = payload.get("email")
    if isinstance(em, Dict) and em:
        recips = _extract_emails((em.get("recipient", "") or "") + "," + (em.get("to", "") or "") + "," + (em.get("cc", "") or ""))
        for cand in reversed(recips):
            return cand, _parse_name(em.get("recipient", "")) or None
        for cand in reversed(_extract_emails(em.get("body", "") or "")):
            return cand, None

    return None, None

# -------------------- persistência --------------------

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

# -------------------- main --------------------

def main():
    if not GEMINI_API_KEY:
        raise SystemExit("⛔ GEMINI_API_KEY não definido no .env")
    genai.configure(api_key=GEMINI_API_KEY)

    files = sorted(glob.glob(os.path.join(INCOMPLETE_DIR, "*_processed.json")))
    if not files:
        print(f"⛔ Nenhum arquivo *_processed.json encontrado em {INCOMPLETE_DIR}/")
        return

    print(f"✉️  Gerando e-mails de follow-up para {len(files)} thread(s) incompletas…")

    created = 0
    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        missing_fields = _union_missing_fields(payload)
        if not missing_fields:
            continue

        questions = [question_for_field(f) for f in missing_fields]
        supplier_name = _friendly_supplier_name(payload)
        orig_subject = _original_subject(payload)

        to_email, to_name = _guess_to_from_processed(payload)

        prompt_ctx = {
            "supplier_name": to_name or supplier_name,
            "original_subject": orig_subject,
            "missing_questions": questions,
            "from_name": DEFAULT_FROM_NAME,
        }
        reply = _call_llm_followup(build_followup_prompt(prompt_ctx))

        subject = reply.get("subject") or "Informações pendentes da cotação"
        body = reply.get("body") or (
            f"Olá {to_name or supplier_name},\n\n"
            "Tudo bem? Obrigado pela cotação enviada. Durante a conferência, notamos que alguns pontos ficaram pendentes:\n"
            + "".join(f"- {q}\n" for q in questions) +
            "\nPoderiam, por favor, nos confirmar essas informações? Agradecemos desde já!\n\n"
            f"{DEFAULT_FROM_NAME}\nParrot Trips"
        )

        base = os.path.splitext(os.path.basename(path))[0].replace("_processed", "")
        jpath, tpath = _save_draft(
            base_name=base,
            to_email=to_email,
            cc=DEFAULT_CC,
            subject=subject,
            body=body,
        )
        created += 1
        print(f"✅ Draft criado: {os.path.basename(jpath)} | {os.path.basename(tpath)}  → To: {to_email or '(vazio)'}")

    print(f"🏁 Pronto! {created} draft(s) gerado(s) em {DRAFTS_DIR}/")

if __name__ == "__main__":
    main()
