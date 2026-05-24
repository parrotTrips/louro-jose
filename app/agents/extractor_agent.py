import json
import hashlib
import base64
import logging
import re
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.core.gcs_client import make_gcs_client
from app.core.llm_client import make_llm_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema / prompts
# ---------------------------------------------------------------------------

HEADER_FIELDS = [
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

IDENTITY_FIELDS_FOR_KEY = [
    "Nome do hotel",
    "Cidade",
    "Check-in",
    "Check-out",
    "Categoria do quarto",
    "Configuração do quarto",
    "Preço (num)",
]

FIELDS_JSON = json.dumps(HEADER_FIELDS, ensure_ascii=False, indent=2)

SYSTEM_PROMPT = (
    "Você extrai **cotações de hotel** de e-mails.\n"
    "Sempre responda com **apenas um JSON** válido.\n"
    "Cada combinação distinta de **categoria/configuração de quarto e preço** deve virar **um objeto separado**.\n"
    "Se algum campo não existir, use string vazia \"\" (exceto `Preço (num)`, que deve ser número ou \"\").\n"
    "\n"
    "Definições:\n"
    "- **Categoria do quarto**: a classe comercial do quarto (p.ex.: standard, luxo, superior, deluxe, premium, master).\n"
    "- **Configuração do quarto**: arranjo de leitos/ocupação (p.ex.: twin/duas de solteiro, double/uma de casal, "
    "  1 casal + 1 solteiro, 3 solteiros, triplo, quádruplo, king, queen).\n"
    "\n"
    "Campo **Descrição dos Quartos** (obrigatório e **específico da cotação**):\n"
    "- Deve conter **apenas a descrição referente à categoria/configuração daquela cotação**.\n"
    "- Se não houver trecho específico, **sintetize** curto a partir dos campos (ex.: `Standard: SGL/DBL`).\n"
    "- **Não inclua preços** e não repita políticas gerais, taxas, café da manhã etc.\n"
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

Instruções específicas para **Descrição dos Quartos**:
- Se houver bloco com múltiplas categorias, selecione **somente** a linha/trecho da categoria/configuração daquela cotação.
- Se não houver linha específica, **sintetize** curto a partir de categoria/configuração: ex. `Standard: SGL/DBL`.
- **Não** inclua preços nem itens gerais (café da manhã, taxas, políticas).

Trechos relevantes da thread (selecionados e limpos):
----------------
{email_text}
----------------
"""

EXTRACTOR_STATE_PATH = "state/extractor_state.json"

# ---------------------------------------------------------------------------
# Pure helper functions
# ---------------------------------------------------------------------------

def _thread_id_from_blob_name(blob_name: str) -> str:
    return blob_name.split("/")[-1].replace(".json", "")


def _make_row_key(thread_id: str, row: Dict[str, Any]) -> str:
    parts = [thread_id]
    for field in IDENTITY_FIELDS_FOR_KEY:
        parts.append(str(row.get(field, "")).strip())
    raw = "||".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _get_header(headers: List[Dict[str, str]], name: str) -> str:
    name_low = name.lower()
    for h in headers or []:
        if h.get("name", "").lower() == name_low:
            return h.get("value", "")
    return ""


def _decode_b64(data: str) -> str:
    if not data:
        return ""
    try:
        return base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _extract_body_from_payload(payload: Dict[str, Any]) -> str:
    if not payload:
        return ""

    body = payload.get("body", {}) or {}
    parts = payload.get("parts") or []

    if not parts and body.get("data"):
        return _decode_b64(body["data"])

    # Search for text/plain first
    stack = list(parts)
    while stack:
        part = stack.pop()
        p_mime = (part.get("mimeType") or "").lower()
        if p_mime == "text/plain" and part.get("body", {}).get("data"):
            return _decode_b64(part["body"]["data"])
        if p_mime.startswith("multipart/"):
            stack.extend(part.get("parts") or [])

    # Fallback to text/html
    stack = list(parts)
    while stack:
        part = stack.pop()
        p_mime = (part.get("mimeType") or "").lower()
        if p_mime == "text/html" and part.get("body", {}).get("data"):
            html = _decode_b64(part["body"]["data"])
            html = re.sub(r"(?i)<br\s*/?>", "\n", html)
            html = re.sub(r"(?i)</p>", "\n", html)
            html = re.sub(r"<[^>]+>", " ", html)
            html = re.sub(r"\s+", " ", html)
            return html.strip()
        if p_mime.startswith("multipart/"):
            stack.extend(part.get("parts") or [])

    return ""


def _strip_reply_history_and_signature(body: str) -> str:
    if not body:
        return ""

    stop_markers = [
        "mensagem encaminhada",
        "forwarded message",
        "-----mensagem original-----",
        "----mensagem original----",
    ]
    disclaimer_markers = [
        "esta mensagem é confidencial",
        "esta mensagem e confidencial",
        "pode conter informação confidencial",
        "se você não for o destinatário",
        "se voce nao for o destinatario",
    ]

    cleaned: List[str] = []
    for line in body.splitlines():
        low = line.strip().lower()
        if any(m in low for m in disclaimer_markers):
            break
        if any(m in low for m in stop_markers):
            break
        if low.startswith(">"):
            continue
        if low.startswith("em ") and "escreveu" in low:
            break
        cleaned.append(line)

    text = "\n".join(cleaned).strip()
    return re.sub(r"\n{3,}", "\n\n", text)


def _score_message(from_email: str, body: str) -> int:
    if not body:
        return -999

    text = body.lower()
    score = 0

    if from_email and "parrottrips.com" not in from_email.lower():
        score += 2

    if (
        re.search(r"r\$\s*\d", text)
        or re.search(r"\d{1,3}\.\d{3},\d{2}", text)
        or re.search(r"\d+,\d{2}", text)
    ):
        score += 3

    keywords = [
        "diária", "diaria", "noite", "hospedagem", "hotel",
        "apartamento", "quarto", "tarifa", "standard", "luxo",
        "superior", "check-in", "check in", "check-out", "check out",
        "café da manhã", "cafe da manha", "pensão", "pensao", "regime",
    ]
    score += min(sum(1 for kw in keywords if kw in text), 3)

    if len(text) < 80:
        score -= 2
    if any(p in text for p in ["obrigado", "agradecemos o contato", "à disposição", "a disposição"]):
        score -= 1

    return score


def _build_clean_thread_text(thread_data: Dict[str, Any]) -> str:
    messages = thread_data.get("messages") or []
    if not messages:
        return json.dumps(thread_data, ensure_ascii=False)

    msg_infos = []
    for idx, msg in enumerate(messages):
        payload = msg.get("payload", {}) or {}
        headers = payload.get("headers", []) or []
        from_email = _get_header(headers, "From")
        subject = _get_header(headers, "Subject")
        date = _get_header(headers, "Date")
        body_raw = _extract_body_from_payload(payload)
        body_clean = _strip_reply_history_and_signature(body_raw)
        score = _score_message(from_email, body_clean)
        msg_infos.append({
            "idx": idx, "from": from_email, "subject": subject,
            "date": date, "score": score, "body": body_clean.strip(),
        })

    top_msg = messages[0]
    top_headers = (top_msg.get("payload") or {}).get("headers", []) or []
    header_lines = [
        f"TOP-LEVEL FROM: {_get_header(top_headers, 'From')}",
        f"TOP-LEVEL SUBJECT: {_get_header(top_headers, 'Subject')}",
        f"TOP-LEVEL DATE: {_get_header(top_headers, 'Date')}",
        "",
        "Abaixo, apenas as mensagens mais relevantes (hotel / fornecedor), já limpas:",
        "",
    ]

    selected = [m for m in msg_infos if m["score"] > 0 and len(m["body"]) > 40]
    if not selected:
        non_empty = [m for m in msg_infos if m["body"]]
        if non_empty:
            selected = [sorted(non_empty, key=lambda x: x["idx"])[-1]]
        else:
            return json.dumps(thread_data, ensure_ascii=False)

    selected = sorted(selected, key=lambda m: (-m["score"], m["idx"]))[:5]

    blocks = []
    for j, m in enumerate(selected, 1):
        blocks.append("\n".join([
            f"--- MENSAGEM {j} ---",
            f"From: {m['from']}",
            f"Date: {m['date']}",
            f"Subject: {m['subject']}",
            "",
            m["body"],
            "",
        ]))

    return "\n".join(header_lines + blocks)


# ---------------------------------------------------------------------------
# ExtractorAgent
# ---------------------------------------------------------------------------

class ExtractorAgent:
    def __init__(self) -> None:
        self._state: Optional[Dict[str, Any]] = None

    def _load_state(self, gcs) -> Dict[str, Any]:
        data = gcs.download_json(EXTRACTOR_STATE_PATH)
        if data is None or not isinstance(data, dict):
            return {}
        return data

    def _is_up_to_date(self, state: Dict[str, Any], thread_id: str, last_message_id: str) -> bool:
        if not last_message_id:
            return False
        stored = (state.get(thread_id) or {}).get("last_message_id") or ""
        return stored == last_message_id

    def run(self) -> None:
        gcs = make_gcs_client()
        llm = make_llm_client()
        state = self._load_state(gcs)

        blob_names = [b for b in gcs.list_blobs("threads/") if b.endswith(".json")]
        logger.info("Encontrados %d arquivos em threads/", len(blob_names))
        logger.info("Estado atual: %d threads já processadas.", len(state))

        all_rows: List[Dict[str, Any]] = []

        for blob_name in blob_names:
            thread_id = _thread_id_from_blob_name(blob_name)
            thread_data = gcs.download_json(blob_name)
            if not thread_data:
                logger.warning("Thread %s: arquivo vazio, pulando.", thread_id)
                continue

            messages = thread_data.get("messages") or []
            if not messages:
                logger.warning("Thread %s: sem mensagens, pulando.", thread_id)
                continue

            last_message_id = (messages[-1].get("id") or "").strip()

            if self._is_up_to_date(state, thread_id, last_message_id):
                logger.debug("Thread %s sem novos emails, pulando.", thread_id)
                continue

            logger.info("Extraindo thread %s...", thread_id)

            try:
                rows = self._extract_thread(thread_id, thread_data, llm, gcs)
                logger.info("Thread %s: %d cotações extraídas.", thread_id, len(rows))
                all_rows.extend(rows)
                state[thread_id] = {
                    "processed": True,
                    "last_message_id": last_message_id,
                    "last_row_count": len(rows),
                }
            except Exception as e:
                logger.error("Erro ao extrair thread %s: %s", thread_id, e)

        gcs.upload_json(EXTRACTOR_STATE_PATH, state)
        logger.info("Extração concluída. Total de linhas novas: %d", len(all_rows))

        gcs.upload_json("tables/quotes_raw.json", all_rows)
        logger.info("tables/quotes_raw.json salvo.")

        if all_rows:
            self._update_history(gcs, all_rows)

    def _extract_thread(
        self,
        thread_id: str,
        thread_data: Dict[str, Any],
        llm,
        gcs,
    ) -> List[Dict[str, Any]]:
        email_text = _build_clean_thread_text(thread_data)
        user_prompt = USER_PROMPT_TEMPLATE.format(
            fields_json=FIELDS_JSON,
            email_text=email_text,
        )

        try:
            quotes = llm.extract_quotes(SYSTEM_PROMPT, user_prompt)
        except Exception as e:
            msg = str(e)
            if "400 Client Error" in msg:
                debug_path = f"state/extractor_llm_400_{thread_id}.json"
                try:
                    gcs.upload_json(debug_path, {
                        "thread_id": thread_id,
                        "error": msg,
                        "user_prompt_head": user_prompt[:4000],
                    })
                    logger.warning("Debug do erro 400 salvo em %s", debug_path)
                except Exception:
                    pass
            raise

        rows: List[Dict[str, Any]] = []
        for i, quote in enumerate(quotes):
            row = {field: quote.get(field, "") for field in HEADER_FIELDS}
            row["_thread_id"] = thread_id
            row["_row_index_in_thread"] = i
            row["_key"] = _make_row_key(thread_id, row)
            rows.append(row)

        return rows

    def _update_history(self, gcs, new_rows: List[Dict[str, Any]]) -> None:
        history_path = "tables/quotes_history.json"
        existing = gcs.download_json(history_path)
        if not isinstance(existing, list):
            existing = []

        by_key: Dict[str, Dict[str, Any]] = {
            row["_key"]: row for row in existing if row.get("_key")
        }
        new_count = 0
        for row in new_rows:
            key = row.get("_key")
            if key and key not in by_key:
                by_key[key] = row
                new_count += 1

        gcs.upload_json(history_path, list(by_key.values()))
        logger.info(
            "Histórico atualizado: %d novas linhas únicas, %d total.",
            new_count,
            len(by_key),
        )


def run_extractor() -> None:
    ExtractorAgent().run()
