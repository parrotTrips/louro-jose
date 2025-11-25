# app/agents/extractor_agent.py

import json
import hashlib
import base64
import re
from typing import List, Dict, Any

from google.cloud import storage

from app.core.config import settings
from app.core.gcs_client import gcs_client
from app.core.llm_client import llm_client


# Campos que queremos (colunas da tabela)
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
    "- Deve conter **apenas a descrição referente à categoria/configuração daquela cotação** (uma linha/bullet curto).\n"
    "- Se houver um bloco com várias categorias, selecione **somente** o trecho da categoria correspondente.\n"
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


# =====================================================
# Helpers para trabalhar com a estrutura do Gmail
# =====================================================

def _thread_id_from_blob_name(blob_name: str) -> str:
    # ex: "threads/1988a581579a47bf.json" -> "1988a581579a47bf"
    return blob_name.split("/")[-1].replace(".json", "")


def _make_row_key(thread_id: str, idx: int) -> str:
    # chave técnica única por linha de cotação
    raw = f"{thread_id}#{idx}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _get_header(headers: List[Dict[str, str]], name: str) -> str:
    """Pega um header pelo nome (case-insensitive)."""
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
    """
    Extrai texto do corpo a partir do payload do Gmail.
    Prioriza text/plain; se não tiver, usa text/html (removendo tags).
    """
    if not payload:
        return ""

    mime_type = payload.get("mimeType", "") or ""
    body = payload.get("body", {}) or {}
    parts = payload.get("parts") or []

    # Caso simples: sem multipart
    if not parts and body.get("data"):
        text = _decode_b64(body.get("data", ""))
        return text

    # Multipart: procura text/plain primeiro
    chosen = None
    stack = list(parts)
    while stack:
        part = stack.pop()
        p_mime = (part.get("mimeType") or "").lower()
        if p_mime == "text/plain" and part.get("body", {}).get("data"):
            chosen = _decode_b64(part["body"]["data"])
            break
        # acumula para procurar text/html depois
        if p_mime.startswith("multipart/"):
            stack.extend(part.get("parts") or [])

    if chosen:
        return chosen

    # Se não achou text/plain, tenta text/html
    stack = list(parts)
    html_text = ""
    while stack:
        part = stack.pop()
        p_mime = (part.get("mimeType") or "").lower()
        if p_mime == "text/html" and part.get("body", {}).get("data"):
            html_text = _decode_b64(part["body"]["data"])
            break
        if p_mime.startswith("multipart/"):
            stack.extend(part.get("parts") or [])

    if html_text:
        # converte HTML simples para texto
        html_text = re.sub(r"(?i)<br\s*/?>", "\n", html_text)
        html_text = re.sub(r"(?i)</p>", "\n", html_text)
        html_text = re.sub(r"<[^>]+>", " ", html_text)
        html_text = re.sub(r"\s+", " ", html_text)
        return html_text.strip()

    return ""


def _strip_reply_history_and_signature(body: str) -> str:
    """
    Remove:
    - histórico de respostas encaminhadas
    - linhas citadas (começando com '>')
    - disclaimers padrão de confidencialidade
    mantendo só o miolo útil da resposta do hotel.
    """
    if not body:
        return ""

    lines = body.splitlines()
    cleaned: List[str] = []

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

    for line in lines:
        l = line.strip()
        low = l.lower()

        if any(m in low for m in disclaimer_markers):
            break
        if any(m in low for m in stop_markers):
            break
        if low.startswith(">"):
            # histórico citado
            continue
        if low.startswith("em ") and "escreveu" in low:
            # "Em 12 de março, Fulano escreveu:"
            break

        cleaned.append(line)

    text = "\n".join(cleaned).strip()
    # comprime múltiplas linhas em branco
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def _score_message(from_email: str, body: str) -> int:
    """
    Score simples para priorizar:
    - mensagens do hotel (não parrottrips)
    - com preço e vocabulário de cotação
    - penaliza mensagens muito curtas/de agradecimento
    """
    if not body:
        return -999  # impróprio

    text = body.lower()
    score = 0

    if from_email and "parrottrips.com" not in from_email.lower():
        score += 2  # vem do hotel/fornecedor

    # presença de preço
    if re.search(r"r\$\s*\d", text) or re.search(r"\d{1,3}\.\d{3},\d{2}", text) or re.search(r"\d+,\d{2}", text):
        score += 3

    keywords = [
        "diária", "diaria", "noite", "hospedagem", "hotel",
        "apartamento", "quarto", "tarifa", "standard", "luxo",
        "superior", "check-in", "check in", "check-out", "check out",
        "café da manhã", "cafe da manha", "pensão", "pensao", "regime",
    ]
    kw_hits = sum(1 for kw in keywords if kw in text)
    score += min(kw_hits, 3)

    # penaliza mensagens muito curtas / só agradecimento
    if len(text) < 80:
        score -= 2
    if any(p in text for p in ["obrigado", "agradecemos o contato", "à disposição", "a disposição", "estamos à disposição"]):
        score -= 1

    return score


def _build_clean_thread_text(thread_data: Dict[str, Any]) -> str:
    """
    Seleciona as mensagens mais relevantes do hotel na thread,
    faz faxina e devolve um texto compacto para o LLM.
    """
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

        msg_infos.append(
            {
                "idx": idx,
                "from": from_email,
                "subject": subject,
                "date": date,
                "score": score,
                "body": body_clean.strip(),
            }
        )

    # header top-level (primeira mensagem da thread)
    top_msg = messages[0]
    top_headers = (top_msg.get("payload") or {}).get("headers", []) or []
    top_from = _get_header(top_headers, "From")
    top_subject = _get_header(top_headers, "Subject")
    top_date = _get_header(top_headers, "Date")

    header_lines = [
        f"TOP-LEVEL FROM: {top_from}",
        f"TOP-LEVEL SUBJECT: {top_subject}",
        f"TOP-LEVEL DATE: {top_date}",
        "",
        "Abaixo, apenas as mensagens mais relevantes (hotel / fornecedor), já limpas:",
        "",
    ]

    # filtra mensagens com score > 0 e corpo razoável
    selected = [m for m in msg_infos if m["score"] > 0 and len(m["body"]) > 40]

    if not selected:
        # fallback: pega só a última mensagem não vazia
        non_empty = [m for m in msg_infos if m["body"]]
        if non_empty:
            selected = [sorted(non_empty, key=lambda x: x["idx"])[-1]]
        else:
            # último fallback: devolve JSON bruto da thread
            return json.dumps(thread_data, ensure_ascii=False)

    # ordena por score desc, depois ordem cronológica
    selected = sorted(selected, key=lambda m: (-m["score"], m["idx"]))
    selected = selected[:5]  # limite de segurança

    blocks = []
    for j, m in enumerate(selected, 1):
        block = [
            f"--- MENSAGEM {j} ---",
            f"From: {m['from']}",
            f"Date: {m['date']}",
            f"Subject: {m['subject']}",
            "",
            m["body"],
            "",
        ]
        blocks.append("\n".join(block))

    return "\n".join(header_lines + blocks)


# =====================================================
# ExtractorAgent
# =====================================================

class ExtractorAgent:
    def __init__(self):
        self.bucket_name = settings.GCS_BUCKET
        self.service_account_file = settings.SERVICE_ACCOUNT_FILE
        self.state_path = "state/extractor_state.json"
        self.state: Dict[str, Dict[str, Any]] = self._load_state()

    # ---------------------------
    # Estado incremental
    # ---------------------------
    def _load_state(self) -> Dict[str, Dict[str, Any]]:
        """Carrega o estado do extrator do GCS (ou {} se não existir)."""
        data = gcs_client.download_json(self.state_path)
        if data is None:
            return {}
        if not isinstance(data, dict):
            # sanity check simples
            return {}
        return data

    def _save_state(self) -> None:
        """Persiste o estado do extrator no GCS."""
        gcs_client.upload_json(self.state_path, self.state)

    def _is_processed(self, thread_id: str) -> bool:
        """Retorna True se a thread já foi processada com sucesso."""
        info = self.state.get(thread_id)
        return bool(info and info.get("processed"))

    def _mark_processed(self, thread_id: str, row_count: int) -> None:
        """Marca uma thread como processada com sucesso."""
        self.state[thread_id] = {
            "processed": True,
            "last_row_count": row_count,
        }

    # ---------------------------
    # Listar todos os JSONs em threads/
    # ---------------------------
    def list_thread_objects(self) -> List[str]:
        client = storage.Client.from_service_account_json(self.service_account_file)
        bucket = client.bucket(self.bucket_name)

        blobs = bucket.list_blobs(prefix="threads/")
        return [b.name for b in blobs if b.name.endswith(".json")]

    # ---------------------------
    # Extrair cotações de UMA thread
    # ---------------------------
    def extract_thread(self, thread_id: str, thread_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        # agora mandamos para o LLM apenas as mensagens relevantes, já limpas
        email_text = _build_clean_thread_text(thread_data)

        user_prompt = USER_PROMPT_TEMPLATE.format(
            fields_json=FIELDS_JSON,
            email_text=email_text,
        )

        try:
            quotes = llm_client.extract_quotes(SYSTEM_PROMPT, user_prompt)
        except Exception as e:
            msg = str(e)
            # tratamento especial para 400
            if "400 Client Error" in msg:
                print(f"   ⚠️ [LLM-400] Erro 400 (Bad Request) ao chamar LLM para thread {thread_id}.")
                debug_payload = {
                    "thread_id": thread_id,
                    "error": msg,
                    # corta o prompt pra não ficar gigante no debug
                    "user_prompt_head": user_prompt[:4000],
                }
                debug_path = f"state/extractor_llm_400_{thread_id}.json"
                try:
                    gcs_client.upload_json(debug_path, debug_payload)
                    print(f"   📝 Debug do erro salvo em gs://{self.bucket_name}/{debug_path}")
                except Exception as e2:
                    print(f"   ⚠️ Falha ao salvar debug do erro 400: {e2}")
            # propaga o erro para o chamador decidir se marca estado ou não
            raise

        rows: List[Dict[str, Any]] = []
        for i, quote in enumerate(quotes):
            # garante que todas as colunas existam
            row = {field: quote.get(field, "") for field in HEADER_FIELDS}

            row["_thread_id"] = thread_id
            row["_row_index_in_thread"] = i
            row["_key"] = _make_row_key(thread_id, i)

            rows.append(row)

        return rows

    # ---------------------------
    # Fluxo principal do agente
    # ---------------------------
    def run(self) -> None:
        print("🟣 Iniciando ExtractorAgent (threads → tables/quotes_raw.json)")

        blob_names = self.list_thread_objects()
        print(f"→ Encontrados {len(blob_names)} arquivos em threads/")

        already = sum(1 for b in blob_names if self._is_processed(_thread_id_from_blob_name(b)))
        print(f"→ {already} threads já marcadas como processadas no estado; serão puladas.")

        all_rows: List[Dict[str, Any]] = []

        for blob_name in blob_names:
            thread_id = _thread_id_from_blob_name(blob_name)

            if self._is_processed(thread_id):
                print(f"⏭️  Thread {thread_id} já processada anteriormente, pulando.")
                continue

            print(f"📦 Processando thread {thread_id} ({blob_name})...")

            thread_data = gcs_client.download_json(blob_name)
            if not thread_data:
                print("   → Arquivo vazio ou não encontrado, pulando.")
                continue

            try:
                rows = self.extract_thread(thread_id, thread_data)
                print(f"   → {len(rows)} cotações extraídas.")
                all_rows.extend(rows)

                # Marca como processada, mesmo que 0 linhas (LLM decidiu que não é cotação)
                self._mark_processed(thread_id, len(rows))

            except Exception as e:
                # NÃO marca como processada em caso de erro, para tentar de novo na próxima execução
                print(f"   ⚠️ Erro ao extrair thread {thread_id}: {e}")

        # Salva estado atualizado
        self._save_state()

        print(f"✅ Extração concluída. Total de linhas novas nesta execução: {len(all_rows)}")

        # Salva tudo em um único JSON array tabular
        output_path = "tables/quotes_raw.json"
        gcs_client.upload_json(output_path, all_rows)
        print(f"📂 Tabela (parcial desta execução) salva em gs://{self.bucket_name}/{output_path}")


extractor_agent = ExtractorAgent()
