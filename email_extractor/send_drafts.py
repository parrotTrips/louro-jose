"""
Envia e-mails a partir de drafts em JSON na pasta email_drafts/.

Suporta autopreencher "to" usando o *_processed.json correspondente,
aproveitando o campo "_guessed_supplier.email" (que agora prioriza sender do último e-mail).
"""

from __future__ import annotations

import os
import sys
import glob
import json
import argparse
import base64
import re
from pathlib import Path
from typing import List, Optional

# --- bootstrap repo root on sys.path ---
ROOT = Path(__file__).resolve().parents[1]  # .../louro-jose
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from googleapiclient.errors import HttpError
from email.message import EmailMessage
from email.utils import formataddr

from modules.login_gmail import create_login  # reusa seu login

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

CREDENTIALS_PATH = os.getenv(
    "GMAIL_OAUTH_CLIENT",
    str(ROOT / "credentials" / "real-credentials-parrots-gmail.json")
)
TOKEN_DIR = os.getenv("GMAIL_TOKEN_DIR", str(ROOT / "token_files"))
TOKEN_PATH = str(Path(TOKEN_DIR) / "token_gmail_v1.json")

DRAFTS_DIR = "email_drafts"           # relativo a email_extractor/
INCOMPLETE_DIR = "incomplete_data"    # onde ficam os *_processed.json

# ----------------------- utils -----------------------

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _split_addrs(s: str) -> List[str]:
    if not s:
        return []
    parts = []
    for chunk in s.replace(";", ",").split(","):
        addr = chunk.strip()
        if addr:
            parts.append(addr)
    return parts

def _build_mime(to_addrs: List[str],
                cc_addrs: List[str],
                from_name: str,
                from_email: str,
                subject: str,
                body: str) -> EmailMessage:
    msg = EmailMessage()
    if from_email:
        msg["From"] = formataddr((from_name or "", from_email))
        msg["Reply-To"] = formataddr((from_name or "", from_email))
    else:
        msg["From"] = from_name or ""
    if to_addrs:
        msg["To"] = ", ".join(to_addrs)
    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)
    msg["Subject"] = subject or "(sem assunto)"
    msg.set_content(body or "")
    return msg

def _send_raw(service, raw_b64: str) -> dict:
    return service.users().messages().send(userId="me", body={"raw": raw_b64}).execute()

# ---------- autofill 'to' a partir do *_processed.json ----------

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
            seen.add(e); uniq.append(e)
    return uniq

def _domain(addr: str) -> str:
    m = re.search(r"@([\w\.-]+)$", addr or "")
    return m.group(1).lower() if m else ""

def _autofill_to_from_processed(draft_path: Path) -> Optional[str]:
    """
    Procura incomplete_data/<base>_processed.json correspondente ao draft.
    Preferência: _guessed_supplier.email (do extrator).
    """
    base = draft_path.name.replace("_draft.json", "")
    processed = Path(INCOMPLETE_DIR) / f"{base}_processed.json"
    if not processed.exists():
        return None
    try:
        payload = json.loads(processed.read_text(encoding="utf-8"))
    except Exception:
        return None

    gs = payload.get("_guessed_supplier") or {}
    if isinstance(gs, dict) and gs.get("email"):
        return gs.get("email")

    # Fallbacks (mantidos por garantia)
    thr = payload.get("thread")
    if thr and isinstance(thr.get("emails"), list) and thr["emails"]:
        emails = thr["emails"]
        for e in reversed(emails):
            senders = _extract_emails(e.get("sender", ""))
            if senders:
                return senders[-1]
        for e in reversed(emails):
            recips = _extract_emails(e.get("recipient", ""))
            for r in reversed(recips):
                return r

    em = payload.get("email")
    if isinstance(em, dict) and em:
        recips = _extract_emails((em.get("recipient", "") or "") + "," + (em.get("to", "") or "") + "," + (em.get("cc", "") or ""))
        for cand in reversed(recips):
            return cand
        for cand in reversed(_extract_emails(em.get("body", "") or "")):
            return cand

    return None

# ----------------------- main -----------------------

def main():
    parser = argparse.ArgumentParser(description="Envia e-mails a partir dos drafts em email_drafts/*.json")
    parser.add_argument("--dir", default=DRAFTS_DIR, help="Pasta com drafts .json (padrão: email_drafts)")
    parser.add_argument("--max", type=int, default=0, help="Limite de mensagens a enviar (0 = sem limite)")
    parser.add_argument("--simulate", action="store_true", help="Apenas pré-visualiza (não envia)")
    parser.add_argument("--autofill-to", action="store_true", help="Tentar preencher 'to' a partir de incomplete_data/*_processed.json")
    args = parser.parse_args()

    _ensure_dir(TOKEN_DIR)
    try:
        service = create_login(
            credentials_path=CREDENTIALS_PATH,
            token_path=TOKEN_PATH,
            scopes=SCOPES,
        )
    except Exception as e:
        raise SystemExit(f"⛔ Falha no login Gmail: {e}")

    files = sorted(glob.glob(str(Path(args.dir) / "*.json")))
    if not files:
        print(f"⛔ Nenhum draft .json encontrado em {args.dir}/")
        return

    sent = 0
    skipped = 0

    for path in files:
        if args.max and sent >= args.max:
            break

        p = Path(path)
        try:
            draft = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"⚠️  Ignorando {p.name}: não consegui ler JSON ({e})")
            skipped += 1
            continue

        to_addrs = _split_addrs((draft.get("to") or "").strip())
        cc_addrs = _split_addrs((draft.get("cc") or "").strip())
        from_name = (draft.get("from_name") or "").strip()
        from_email = (draft.get("from_email") or "").strip()
        subject = (draft.get("subject") or "").strip()
        body = draft.get("body") or ""

        if not to_addrs and args.autofill_to:
            guessed = _autofill_to_from_processed(p)
            if guessed:
                to_addrs = [guessed]
                print(f"🧩 Autofill 'to': {guessed}   ← {p.name}")

        if not to_addrs:
            print(f"⚠️  Sem destinatário 'to' → pulando {p.name}")
            skipped += 1
            continue

        msg = _build_mime(
            to_addrs=to_addrs,
            cc_addrs=cc_addrs,
            from_name=from_name,
            from_email=from_email,
            subject=subject,
            body=body,
        )
        raw_b64 = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

        if args.simulate:
            print(f"🔎 [SIMULATE] {p.name}")
            print(f"    To: {', '.join(to_addrs)}")
            if cc_addrs:
                print(f"    Cc: {', '.join(cc_addrs)}")
            print(f"    Subject: {subject}")
            preview = body[:160].replace("\n", " ")
            print(f"    Body preview: {preview}{'…' if len(body)>160 else ''}")
        else:
            try:
                resp = _send_raw(service, raw_b64)
                print(f"✅ Enviado {p.name} → id={resp.get('id')} thread={resp.get('threadId')}")
                sent += 1
            except HttpError as e:
                print(f"❌ Erro Gmail API ao enviar {p.name}: {e}")
                skipped += 1
            except Exception as e:
                print(f"❌ Erro inesperado ao enviar {p.name}: {e}")
                skipped += 1

    print("\n🏁 Resumo:")
    print(f"  Drafts encontrados : {len(files)}")
    print(f"  Enviados           : {sent}")
    print(f"  Ignorados/erros    : {skipped}")

if __name__ == "__main__":
    main()
