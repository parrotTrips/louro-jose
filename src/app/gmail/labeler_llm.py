# labeler_llm.py — Rotula THREADS inteiras como "QUOTES" usando apenas LLM (OpenRouter).
#
# Como funciona:
# - Busca threads via query (ex.: newer_than:60d in:anywhere).
# - Para cada thread, monta um "preview" textual (Subject + corpo das mensagens).
# - Envia o preview ao LLM pedindo um JSON estrito {is_quote, confidence, reason}.
# - Se is_quote == true, aplica o rótulo na THREAD inteira (gmail.modify).
#
# Requisitos:
# - pip install requests python-dateutil google-api-python-client google-auth-httplib2 google-auth-oauthlib python-dotenv
# - .env com:
#     OPENROUTER_API_KEY=...
#     OPENROUTER_MODEL=gpt-4o-mini                 (opcional; tem default)
#     OPENROUTER_BASE=https://openrouter.ai/api/v1 (opcional)
#     QUOTES_LABEL_NAME=QUOTES                     (opcional)
#     GMAIL_CLIENT_SECRETS=credentials/real-credentials-parrots-gmail.json
#     GMAIL_TOKEN_FILE_MODIFY=tokens/gmail_token_modify.json   (opcional)

from __future__ import annotations

import os
import re
import json
import time
from typing import Dict, List, Tuple, Optional

import requests
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request  # ← refresh correto

from dotenv import load_dotenv
load_dotenv()

# ======= Config / Env =======

SCOPES_MODIFY = ["https://www.googleapis.com/auth/gmail.modify"]

GMAIL_CLIENT_SECRETS = os.getenv("GMAIL_CLIENT_SECRETS", "credentials/real-credentials-parrots-gmail.json")
GMAIL_TOKEN_FILE_MODIFY = os.getenv("GMAIL_TOKEN_FILE_MODIFY", "tokens/gmail_token_modify.json")
GMAIL_USER = os.getenv("GMAIL_USER", "me")

DEFAULT_QUERY = os.getenv("QUOTES_FIND_QUERY", "newer_than:60d in:anywhere")
DEFAULT_LABEL_NAME = os.getenv("QUOTES_LABEL_NAME", "QUOTES")

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_BASE = os.getenv("OPENROUTER_BASE", "https://openrouter.ai/api/v1").strip()
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "gpt-4o-mini").strip()

# ======= Gmail Auth/Utils =======

def _ensure_dir_for(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def build_gmail_service_modify():
    """Auth com escopo 'gmail.modify' (para criar/aplicar labels)."""
    creds = None
    if os.path.exists(GMAIL_TOKEN_FILE_MODIFY):
        creds = Credentials.from_authorized_user_file(GMAIL_TOKEN_FILE_MODIFY, SCOPES_MODIFY)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())  # ← correção importante
            except Exception:
                creds = None
        if not creds:
            _ensure_dir_for(GMAIL_TOKEN_FILE_MODIFY)
            flow = InstalledAppFlow.from_client_secrets_file(GMAIL_CLIENT_SECRETS, SCOPES_MODIFY)
            creds = flow.run_local_server(port=0)
            with open(GMAIL_TOKEN_FILE_MODIFY, "w") as f:
                f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def get_or_create_label_id(service, label_name: str) -> str:
    resp = service.users().labels().list(userId=GMAIL_USER).execute()
    for lb in resp.get("labels", []):
        if lb.get("name") == label_name:
            return lb["id"]
    body = {"name": label_name, "labelListVisibility": "labelShow", "messageListVisibility": "show"}
    created = service.users().labels().create(userId=GMAIL_USER, body=body).execute()
    return created["id"]

def search_thread_ids(service, q: str, max_pages: int = 50) -> List[str]:
    out, page_token, pages = [], None, 0
    while True:
        resp = service.users().threads().list(
            userId=GMAIL_USER, q=q, pageToken=page_token, maxResults=200
        ).execute()
        out.extend([t["id"] for t in resp.get("threads", [])])
        page_token = resp.get("nextPageToken")
        pages += 1
        if not page_token or pages >= max_pages:
            break
    return out

def add_label_to_thread(service, thread_id: str, label_id: str) -> None:
    service.users().threads().modify(
        userId=GMAIL_USER, id=thread_id, body={"addLabelIds": [label_id], "removeLabelIds": []}
    ).execute()

# ======= Extrair texto das mensagens =======

def _decode_b64url(data: str) -> bytes:
    import base64 as _b64
    return _b64.urlsafe_b64decode(data.encode("utf-8"))

def _flatten_payload(payload: Dict) -> List[Tuple[str, bytes]]:
    results: List[Tuple[str, bytes]] = []
    if not payload:
        return results
    mime = payload.get("mimeType", "")
    body = payload.get("body", {})
    data = body.get("data")
    parts = payload.get("parts")
    if data:
        results.append((mime, _decode_b64url(data)))
    if parts:
        for p in parts:
            results.extend(_flatten_payload(p))
    return results

def _strip_html(html: str) -> str:
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    html = re.sub(r"(?s)<[^>]+>", " ", html)
    html = html.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return re.sub(r"\s+", " ", html).strip()

def _get_plain_text_from_message(msg: Dict) -> str:
    payload = msg.get("payload", {})
    parts = _flatten_payload(payload)
    if not parts and payload.get("body", {}).get("data"):
        parts = [(payload.get("mimeType", "text/plain"), _decode_b64url(payload["body"]["data"]))]
    texts: List[str] = []
    for mime, data_bytes in parts:
        if not data_bytes:
            continue
        try:
            raw = data_bytes.decode(errors="ignore")
            if mime.startswith("text/plain"):
                texts.append(raw)
            elif mime.startswith("text/html"):
                texts.append(_strip_html(raw))
        except Exception:
            continue
    return "\n".join(t for t in texts if t).strip()

def _get_header(msg: Dict, name: str) -> str:
    for h in msg.get("payload", {}).get("headers", []):
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""

def build_thread_preview(thread_full: Dict, max_chars: int = 9000) -> str:
    """Monta um preview concatenando Subject + trechos de corpo das mensagens."""
    chunks: List[str] = []
    for i, msg in enumerate(thread_full.get("messages", []), 1):
        subject = _get_header(msg, "Subject")
        body = _get_plain_text_from_message(msg)
        if subject or body:
            chunks.append(f"[MSG {i}] Subject: {subject}\n{body}")
    preview = "\n\n---\n\n".join(chunks).strip()
    return preview[:max_chars]

# ======= LLM (OpenRouter) =======

def classify_thread_with_llm(thread_preview: str, timeout_s: int = 60, retries: int = 3, backoff: float = 2.0) -> Dict:
    """
    Classifica via LLM. Retorna um dict:
      {"is_quote": bool, "confidence": float, "reason": str}
    Exige OPENROUTER_API_KEY. Usa response_format JSON.
    """
    if not OPENROUTER_API_KEY:
        raise RuntimeError("OPENROUTER_API_KEY ausente. Defina no .env.")

    system_prompt = (
        "Você é um classificador rigoroso. Responda SOMENTE JSON válido, sem texto extra.\n"
        'Formato: {"is_quote": true|false, "confidence": 0..1, "reason": "texto curto"}.\n'
        "Definição: 'cotação hoteleira' é conversa de e-mail que contém oferta, proposta ou confirmação de tarifas, "
        "valores, disponibilidade, categorias/configurações de quarto, políticas (cancelamento, no-show, ISS), "
        "pagamento (pix, cartão, faturamento), ou linguagem típica de reservas.\n"
        "Se estiver em dúvida, use is_quote=false (seja conservador)."
    )

    user_prompt = (
        "Classifique se a conversa abaixo é uma COTAÇÃO HOTELEIRA. Responda **apenas** JSON no formato pedido.\n\n"
        f"{thread_preview}"
    )

    url = f"{OPENROUTER_BASE}/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=timeout_s)
            r.raise_for_status()
            data = r.json()
            content = data["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            # Sanitiza campos esperados
            return {
                "is_quote": bool(parsed.get("is_quote", False)),
                "confidence": float(parsed.get("confidence", 0.0)),
                "reason": str(parsed.get("reason", ""))[:500],
            }
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff ** attempt)
            else:
                raise RuntimeError(f"Falha na chamada LLM após {retries} tentativas: {e}") from e

# ======= Pipeline principal =======

def process_threads_llm_only(
    q: str = DEFAULT_QUERY,
    label_name: str = DEFAULT_LABEL_NAME,
    hard_limit_threads: Optional[int] = None,
    sleep_between: float = 0.0,
) -> Dict[str, int]:
    """
    Percorre threads pela query, pergunta ao LLM e aplica label se is_quote==true.
    Retorna estatísticas: {"threads","messages","threads_labeled","llm_calls"}.
    """
    service = build_gmail_service_modify()
    label_id = get_or_create_label_id(service, label_name)

    stats = {"threads": 0, "messages": 0, "threads_labeled": 0, "llm_calls": 0}
    thread_ids = search_thread_ids(service, q)

    for th_id in thread_ids:
        if hard_limit_threads and stats["threads"] >= hard_limit_threads:
            break

        stats["threads"] += 1
        thread_full = service.users().threads().get(userId=GMAIL_USER, id=th_id, format="full").execute()

        # já rotulada?
        already = False
        for msg in thread_full.get("messages", []):
            if label_id in set(msg.get("labelIds", [])):
                already = True
                break
        if already:
            # ainda contamos mensagens para telemetria
            stats["messages"] += len(thread_full.get("messages", []))
            continue

        # monta preview e classifica
        preview = build_thread_preview(thread_full)
        stats["llm_calls"] += 1
        decision = classify_thread_with_llm(preview)
        if decision.get("is_quote", False):
            add_label_to_thread(service, th_id, label_id)
            stats["threads_labeled"] += 1

        # estatística de mensagens analisadas
        stats["messages"] += len(thread_full.get("messages", []))

        if sleep_between:
            time.sleep(sleep_between)

    return stats

# ======= ENTRYPOINT PROGRAMÁTICO (para a main) =======

def run(q: str, label: str = DEFAULT_LABEL_NAME, limit: int = 100, sleep: float = 0.0) -> Dict[str, int]:
    """
    Entry-point programático para uso via import na sua `main`.
    Exemplo:
        from app.gmail.labeler_llm import run as labeler_run
        stats = labeler_run(q="newer_than:30d in:anywhere -label:QUOTES", label="QUOTES", limit=200)
    """
    stats = process_threads_llm_only(
        q=q,
        label_name=label,
        hard_limit_threads=(limit or None),
        sleep_between=sleep,
    )
    return stats

# ======= CLI =======

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Rotula THREADS como QUOTES usando somente LLM (OpenRouter).")
    ap.add_argument("--q", default=DEFAULT_QUERY, help=f"Consulta Gmail (padrão: '{DEFAULT_QUERY}').")
    ap.add_argument("--label", default=DEFAULT_LABEL_NAME, help=f"Nome do rótulo (padrão: '{DEFAULT_LABEL_NAME}').")
    ap.add_argument("--limit", type=int, default=0, help="Limite de threads (0 = sem limite).")
    ap.add_argument("--sleep", type=float, default=0.0, help="Dormir N segundos entre threads (evita rate limit).")
    args = ap.parse_args()

    try:
        stats = process_threads_llm_only(
            q=args.q,
            label_name=args.label,
            hard_limit_threads=(args.limit or None),
            sleep_between=args.sleep,
        )
        # Saída 100% JSON (fácil de parsear pela sua main, se cair no fallback)
        print(json.dumps({"ok": True, "stats": stats}, ensure_ascii=False))
    except HttpError as e:
        print(json.dumps({"ok": False, "error": f"Gmail API: {e}"}, ensure_ascii=False))
    except FileNotFoundError as e:
        print(json.dumps({"ok": False, "error": f"Arquivo não encontrado: {e}"}, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"ok": False, "error": f"Erro inesperado: {e}"}, ensure_ascii=False))
