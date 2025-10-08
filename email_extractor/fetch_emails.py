"""
- Autenticar no Gmail via `modules.login_gmail.create_login`
- Varrer mensagens do label (padrão: QUOTES) usando `modules.gmail_query`
- Agregar por thread e simplificar mensagens via `simplify_message`
- Normalizar texto (fallback de HTML -> texto) e salvar 1 mensagem/linha em JSONL

Saída: data/emails_quotes.jsonl
"""

from __future__ import annotations
import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

sys.path.append("..")
from dotenv import load_dotenv
load_dotenv("../.env")

from modules.login_gmail import create_login
from modules.gmail_query import (
    find_label_id, list_messages, get_thread, simplify_message,
    build_gmail_query, unique_thread_ids
)

from bs4 import BeautifulSoup
import html2text

DEFAULT_CREDENTIALS = "../credentials/real-credentials-parrots-gmail.json"
DEFAULT_TOKEN       = "../token_files/token_gmail_v1.json"
DEFAULT_LABEL       = "QUOTES"
OUTPUT_JSONL        = Path("data") / "emails_quotes.jsonl"
OUTPUT_JSONL.parent.mkdir(parents=True, exist_ok=True)

def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    for t in soup(["script", "style"]):
        t.extract()
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.ignore_images = True
    h.body_width = 0
    return h.handle(str(soup))

def _normalize_text(text_or_html: str, is_html: bool = False) -> str:
    text = _html_to_text(text_or_html) if is_html else (text_or_html or "")
    text = (text or "").replace("\r\n", "\n")
    # remove blocos óbvios de citação (heurística leve)
    lines, out = text.splitlines(), []
    for ln in lines:
        if re.match(r"^>+", ln.strip()):  # replies quoted
            continue
        if re.search(r"On .* wrote:", ln, flags=re.IGNORECASE):
            break
        if re.search(r"^--\s*$", ln):  # início de assinatura simples
            break
        out.append(ln)
    txt = "\n".join(out)
    txt = re.sub(r"\n{3,}", "\n\n", txt).strip()
    return txt

def parse_args():
    p = argparse.ArgumentParser(description="Gera JSONL (uma mensagem por linha) do label informado.")
    p.add_argument("--label", default=DEFAULT_LABEL, help="Nome do rótulo (ex.: QUOTES)")
    p.add_argument("--q", default=None, help="Consulta Gmail adicional (ex.: 'has:attachment')")
    p.add_argument("--after", default=None, help="YYYY/MM/DD")
    p.add_argument("--before", default=None, help="YYYY/MM/DD")
    p.add_argument("--max", type=int, default=2000, help="Máximo de mensagens para varrer")
    p.add_argument("--out", default=str(OUTPUT_JSONL), help="Caminho do JSONL de saída")
    return p.parse_args()

def main():
    args = parse_args()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # === Login do Gmail usando SUA função e paths ===
    service = create_login(
        credentials_path=DEFAULT_CREDENTIALS,
        token_path=DEFAULT_TOKEN,
    )

    # === Label -> label_id (se vier) ===
    label_id = None
    if args.label:
        label_id = find_label_id(service, args.label)
        if not label_id:
            raise SystemExit(f"❌ Label '{args.label}' não encontrado.")

    # === Query Gmail (se quiser refinar) ===
    query = build_gmail_query(q=args.q, after=args.after, before=args.before)

    # === Lista mensagens, pega threads únicas ===
    msgs = list_messages(service, label_ids=[label_id] if label_id else None, query=query, max_results=args.max)
    tids = unique_thread_ids(msgs)
    print(f"🧵 Threads únicas: {len(tids)}")

    n = 0
    with out_path.open("w", encoding="utf-8") as fw:
        for i, tid in enumerate(tids, 1):
            t = get_thread(service, tid)
            messages = t.get("messages", [])

            # usa sua função pra simplificar cada mensagem
            emails = [simplify_message(m) for m in messages]
            # ordena por timestamp (se sua simplify já traz esse campo)
            emails.sort(key=lambda e: e.get("timestamp", ""))

            for em in emails:
                # Campos comuns esperados de 'simplify_message':
                #   timestamp, sender, to, cc, subject, text / html / body...
                # Como não sabemos 100% dos nomes, tentamos chaves comuns:
                raw_text = em.get("text") or em.get("body") or ""
                raw_html = em.get("html") or em.get("body_html") or ""

                if raw_text:
                    text = _normalize_text(raw_text, is_html=False)
                elif raw_html:
                    text = _normalize_text(raw_html, is_html=True)
                else:
                    text = ""

                rec = {
                    "thread_id": tid,
                    "message_id": em.get("id") or em.get("message_id"),
                    "label": args.label or "",
                    "timestamp": em.get("timestamp", ""),
                    "sender": em.get("sender", ""),
                    "to": em.get("to", ""),
                    "cc": em.get("cc", ""),
                    "subject": em.get("subject", ""),
                    "text": text,
                }
                fw.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n += 1

            if i % 20 == 0:
                print(f"  ...{i}/{len(tids)} threads processadas")

    print(f"✅ JSONL salvo: {out_path} ({n} linhas/mensagens)")

if __name__ == "__main__":
    main()
