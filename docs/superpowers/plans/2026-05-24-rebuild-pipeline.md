# Louro José Pipeline Rebuild — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the Gmail → GCS → Sheets pipeline from scratch, replacing LLM-based email classification with Gmail filters, fixing all identified bugs, and ensuring correct credential handling in Cloud Run.

**Architecture:** Three sequential agents (StorageAgent, ExtractorAgent, SheetsSyncAgent) orchestrated by `main.py`, deployed as a Cloud Run Job triggered 2x/day by Cloud Scheduler. Gmail filters (created by a one-time setup script) replace LLM classification. All credentials are mounted from Secret Manager at runtime — never baked into the Docker image.

**Tech Stack:** Python 3.11, google-api-python-client, google-auth, google-auth-oauthlib, google-cloud-storage, gspread 6.x, requests, python-dotenv, pytest (dev)

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `requirements.txt` | Rewrite | Only direct deps, no unnecessary pins |
| `requirements-dev.txt` | Create | pytest only |
| `Dockerfile` | Update | No credentials in image |
| `.dockerignore` | Update | Exclude credentials/, scripts/, .env |
| `.gitignore` | Update | Exclude credentials/, .env |
| `.env.example` | Create | Template for local dev |
| `app/core/config.py` | Update | GMAIL_TOKEN_FILE + SERVICE_ACCOUNT_FILE via env var |
| `app/core/gcs_client.py` | Rewrite | Lazy init, factory function |
| `app/core/llm_client.py` | Update | Fix JSON parsing, lazy init, factory function |
| `app/core/gmail_client.py` | Update | Pagination fix, lazy init, factory function |
| `app/core/state.py` | Rewrite | No module-level singleton, injected GCS client |
| `app/agents/storage_agent.py` | Create | Gmail QUOTES threads → GCS (no LLM) |
| `app/agents/labeler_storage_agent.py` | Delete | Replaced by storage_agent.py |
| `app/agents/extractor_agent.py` | Update | Fix JSON parsing, lazy init |
| `app/agents/sheets_sync_agent.py` | Rewrite | Fix gspread API, fix attribute bug, lazy init |
| `main.py` | Update | Use StorageAgent, no module-level singletons |
| `scripts/generate_gmail_token.py` | Create | OAuth flow to generate/renew Gmail token |
| `scripts/setup_gmail_filters.py` | Create | Create Gmail filters + retroactive labeling |
| `tests/__init__.py` | Create | Test package |
| `tests/test_llm_parsing.py` | Create | Test LLM JSON response parsing |
| `tests/test_extractor_helpers.py` | Create | Test pure helper functions |

---

## Task 1: Infrastructure files

**Files:**
- Rewrite: `requirements.txt`
- Create: `requirements-dev.txt`
- Update: `Dockerfile`
- Update: `.dockerignore`
- Update: `.gitignore`
- Create: `.env.example`

- [ ] **Step 1: Rewrite requirements.txt**

```
google-api-python-client
google-auth
google-auth-httplib2
google-auth-oauthlib
google-cloud-storage
gspread
python-dotenv
requests
```

- [ ] **Step 2: Create requirements-dev.txt**

```
pytest
```

- [ ] **Step 3: Update Dockerfile**

```dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY main.py .

CMD ["python", "main.py"]
```

- [ ] **Step 4: Update .dockerignore**

```
credentials/
scripts/
env/
.env
.git
__pycache__
*.pyc
*.pyo
.DS_Store
docs/
tests/
requirements-dev.txt
```

- [ ] **Step 5: Update .gitignore**

```
credentials/
.env
env/
__pycache__/
*.pyc
*.pyo
.DS_Store
```

- [ ] **Step 6: Create .env.example**

```
GCP_PROJECT_ID=louro-jose-479223
GCS_BUCKET=parrot-agents-dev
GMAIL_LABEL=QUOTES
GMAIL_TOKEN_FILE=credentials/gmail-token.json
SERVICE_ACCOUNT_FILE=credentials/service-account.json
SHEETS_SPREADSHEET_ID=
SHEETS_QUOTE_SHEET_NAME=quotes_raw
OPENAI_API_KEY=
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_MODEL=gpt-4o
```

- [ ] **Step 7: Commit**

```bash
git add requirements.txt requirements-dev.txt Dockerfile .dockerignore .gitignore .env.example
git commit -m "chore: limpa infraestrutura — requirements, Dockerfile, gitignore"
```

---

## Task 2: config.py

**Files:**
- Update: `app/core/config.py`

- [ ] **Step 1: Rewrite app/core/config.py**

```python
import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    GCS_BUCKET = os.getenv("GCS_BUCKET")
    GMAIL_LABEL = os.getenv("GMAIL_LABEL", "QUOTES")
    GMAIL_TOKEN_FILE = os.getenv("GMAIL_TOKEN_FILE", "credentials/gmail-token.json")
    SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "credentials/service-account.json")
    SHEETS_SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")
    SHEETS_QUOTE_SHEET_NAME = os.getenv("SHEETS_QUOTE_SHEET_NAME", "quotes_raw")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")


settings = Settings()
```

- [ ] **Step 2: Commit**

```bash
git add app/core/config.py
git commit -m "fix(config): GMAIL_TOKEN_FILE e SERVICE_ACCOUNT_FILE via env var"
```

---

## Task 3: gcs_client.py

**Files:**
- Rewrite: `app/core/gcs_client.py`

- [ ] **Step 1: Rewrite app/core/gcs_client.py**

```python
import json
import logging
from typing import Any, List, Optional

from google.cloud import storage

from app.core.config import settings

logger = logging.getLogger(__name__)


class GCSClient:
    def __init__(self, service_account_file: str, bucket_name: str) -> None:
        self._service_account_file = service_account_file
        self._bucket_name = bucket_name
        self._client: Optional[storage.Client] = None

    @property
    def _bucket(self) -> storage.Bucket:
        if self._client is None:
            self._client = storage.Client.from_service_account_json(
                self._service_account_file
            )
        return self._client.bucket(self._bucket_name)

    def upload_json(self, path: str, data: Any) -> None:
        blob = self._bucket.blob(path)
        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False, indent=2),
            content_type="application/json",
        )
        logger.info("GCS upload OK → %s", path)

    def download_json(self, path: str) -> Any:
        blob = self._bucket.blob(path)
        if not blob.exists():
            return None
        return json.loads(blob.download_as_string())

    def list_blobs(self, prefix: str) -> List[str]:
        return [b.name for b in self._bucket.list_blobs(prefix=prefix)]


def make_gcs_client() -> GCSClient:
    return GCSClient(settings.SERVICE_ACCOUNT_FILE, settings.GCS_BUCKET)
```

- [ ] **Step 2: Commit**

```bash
git add app/core/gcs_client.py
git commit -m "fix(gcs): lazy init, factory function make_gcs_client"
```

---

## Task 4: llm_client.py + testes de parsing

**Files:**
- Update: `app/core/llm_client.py`
- Create: `tests/__init__.py`
- Create: `tests/test_llm_parsing.py`

- [ ] **Step 1: Criar tests/__init__.py (vazio)**

```python
```

- [ ] **Step 2: Escrever teste que vai falhar (parsing com markdown)**

Criar `tests/test_llm_parsing.py`:

```python
import pytest
from app.core.llm_client import _strip_markdown_fences


def test_strip_plain_json():
    raw = '[{"a": 1}]'
    assert _strip_markdown_fences(raw) == '[{"a": 1}]'


def test_strip_json_fences():
    raw = '```json\n[{"a": 1}]\n```'
    assert _strip_markdown_fences(raw) == '[{"a": 1}]'


def test_strip_plain_fences():
    raw = '```\n[{"a": 1}]\n```'
    assert _strip_markdown_fences(raw) == '[{"a": 1}]'


def test_strip_fences_with_spaces():
    raw = '  ```json  \n[{"a": 1}]\n```  '
    assert _strip_markdown_fences(raw) == '[{"a": 1}]'


def test_json_with_backtick_in_value():
    raw = '```json\n[{"a": "texto com ` backtick"}]\n```'
    result = _strip_markdown_fences(raw)
    assert result == '[{"a": "texto com ` backtick"}]'
```

- [ ] **Step 3: Rodar teste para confirmar que falha**

```bash
python -m pytest tests/test_llm_parsing.py -v
```

Esperado: `ImportError` ou `AttributeError` — `_strip_markdown_fences` não existe ainda.

- [ ] **Step 4: Reescrever app/core/llm_client.py com a função e os fixes**

```python
import json
import re
import logging
from typing import Any, Dict, List

import requests

from app.core.config import settings

logger = logging.getLogger(__name__)


def _strip_markdown_fences(text: str) -> str:
    text = text.strip()
    text = re.sub(r'^```(?:json)?\s*', '', text)
    text = re.sub(r'\s*```$', '', text)
    return text.strip()


class LLMClient:
    def __init__(self, api_key: str, base_url: str, model: str) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def extract_quotes(self, system_prompt: str, user_prompt: str) -> List[Dict[str, Any]]:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.0,
        }
        resp = requests.post(
            f"{self.base_url}/chat/completions",
            headers=self._headers(),
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]

        text = _strip_markdown_fences(content)

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Falha ao parsear JSON do LLM: {e}\nConteúdo: {text[:500]}"
            )

        if not isinstance(data, list):
            raise RuntimeError("A resposta do LLM não é um array JSON.")

        return data


def make_llm_client() -> LLMClient:
    return LLMClient(
        api_key=settings.OPENAI_API_KEY,
        base_url=settings.OPENAI_BASE_URL,
        model=settings.OPENAI_MODEL,
    )
```

- [ ] **Step 5: Rodar testes para confirmar que passam**

```bash
python -m pytest tests/test_llm_parsing.py -v
```

Esperado: 5 testes passando.

- [ ] **Step 6: Commit**

```bash
git add app/core/llm_client.py tests/__init__.py tests/test_llm_parsing.py
git commit -m "fix(llm): parsing robusto de markdown fences, factory make_llm_client"
```

---

## Task 5: gmail_client.py

**Files:**
- Update: `app/core/gmail_client.py`

- [ ] **Step 1: Reescrever app/core/gmail_client.py**

```python
import base64
import logging
from email import message_from_bytes
from typing import Any, Dict, List, Optional

from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.core.config import settings

logger = logging.getLogger(__name__)

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]


class GmailClient:
    def __init__(self, token_file: str) -> None:
        creds = Credentials.from_authorized_user_file(token_file, scopes=GMAIL_SCOPES)
        self.service = build("gmail", "v1", credentials=creds)

    def _execute(self, request: Any, action: str) -> Any:
        try:
            return request.execute()
        except RefreshError as e:
            logger.error(
                "Gmail token expirado/invalidado durante '%s'. "
                "Execute: python scripts/generate_gmail_token.py\n"
                "Detalhes: %s",
                action,
                e,
            )
            raise
        except HttpError as e:
            status = getattr(e, "status_code", None) or (
                e.resp.status if e.resp else None
            )
            if status in (401, 403):
                logger.error(
                    "Erro de autenticação no Gmail durante '%s' (HTTP %s).",
                    action,
                    status,
                )
            else:
                logger.exception("Erro no Gmail API durante '%s'.", action)
            raise

    # --- Labels ---

    def get_or_create_label(self, label_name: str) -> str:
        resp = self._execute(
            self.service.users().labels().list(userId="me"),
            "listar labels",
        )
        for label in resp.get("labels", []):
            if label["name"].lower() == label_name.lower():
                return label["id"]
        body = {
            "name": label_name,
            "labelListVisibility": "labelShow",
            "messageListVisibility": "show",
        }
        created = self._execute(
            self.service.users().labels().create(userId="me", body=body),
            "criar label",
        )
        return created["id"]

    # --- Threads ---

    def list_threads_with_label(self, label_id: str) -> List[Dict[str, Any]]:
        threads: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            resp = self._execute(
                self.service.users().threads().list(
                    userId="me",
                    labelIds=[label_id],
                    pageToken=page_token,
                ),
                "listar threads com label",
            )
            threads.extend(resp.get("threads", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return threads

    def get_thread(self, thread_id: str) -> Dict[str, Any]:
        return self._execute(
            self.service.users().threads().get(
                userId="me", id=thread_id, format="full"
            ),
            "obter thread",
        )

    # --- Messages ---

    def search_messages(self, query: str) -> List[Dict[str, Any]]:
        messages: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            resp = self._execute(
                self.service.users().messages().list(
                    userId="me", q=query, pageToken=page_token
                ),
                "buscar mensagens",
            )
            messages.extend(resp.get("messages", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return messages

    def add_label_to_message(self, message_id: str, label_id: str) -> None:
        self._execute(
            self.service.users().messages().modify(
                userId="me",
                id=message_id,
                body={"addLabelIds": [label_id]},
            ),
            "adicionar label",
        )


def make_gmail_client() -> GmailClient:
    return GmailClient(settings.GMAIL_TOKEN_FILE)
```

- [ ] **Step 2: Commit**

```bash
git add app/core/gmail_client.py
git commit -m "fix(gmail): paginação em list_threads_with_label, factory make_gmail_client"
```

---

## Task 6: state.py

**Files:**
- Rewrite: `app/core/state.py`

- [ ] **Step 1: Reescrever app/core/state.py**

```python
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

STATE_PATH = "state/threads_state.json"


class ThreadsState:
    def __init__(self, gcs_client: Any) -> None:
        self._gcs = gcs_client
        self._data: Dict[str, Any] = gcs_client.download_json(STATE_PATH) or {}

    def get_last_email(self, thread_id: str) -> Optional[str]:
        return self._data.get(thread_id, {}).get("last_email_id")

    def update_thread(self, thread_id: str, last_email_id: str) -> None:
        self._data[thread_id] = {"last_email_id": last_email_id}
        self._gcs.upload_json(STATE_PATH, self._data)
        logger.debug("State: thread %s → last_email_id %s", thread_id, last_email_id)
```

- [ ] **Step 2: Commit**

```bash
git add app/core/state.py
git commit -m "fix(state): remove singleton global, GCSClient injetado"
```

---

## Task 7: StorageAgent + remover labeler_storage_agent.py

**Files:**
- Create: `app/agents/storage_agent.py`
- Delete: `app/agents/labeler_storage_agent.py`

- [ ] **Step 1: Criar app/agents/storage_agent.py**

```python
import logging

from app.core.config import settings
from app.core.gmail_client import make_gmail_client
from app.core.gcs_client import make_gcs_client
from app.core.state import ThreadsState

logger = logging.getLogger(__name__)


class StorageAgent:
    def run(self) -> None:
        gmail = make_gmail_client()
        gcs = make_gcs_client()
        state = ThreadsState(gcs)

        label_id = gmail.get_or_create_label(settings.GMAIL_LABEL)
        logger.info("Label '%s' encontrado/criado (id=%s).", settings.GMAIL_LABEL, label_id)

        threads = gmail.list_threads_with_label(label_id)
        logger.info("Encontradas %d threads com '%s'.", len(threads), settings.GMAIL_LABEL)

        synced = 0
        skipped = 0

        for th in threads:
            thread_id = th["id"]
            thread_full = gmail.get_thread(thread_id)
            messages = thread_full.get("messages", [])

            if not messages:
                logger.warning("Thread %s sem mensagens, pulando.", thread_id)
                continue

            last_email_id = messages[-1]["id"]

            if state.get_last_email(thread_id) == last_email_id:
                skipped += 1
                continue

            gcs.upload_json(f"threads/{thread_id}.json", thread_full)
            state.update_thread(thread_id, last_email_id)
            synced += 1
            logger.info("Thread %s sincronizada.", thread_id)

        logger.info(
            "StorageAgent concluído: %d sincronizadas, %d sem novidades.", synced, skipped
        )


def run_storage() -> None:
    StorageAgent().run()
```

- [ ] **Step 2: Deletar labeler_storage_agent.py**

```bash
git rm app/agents/labeler_storage_agent.py
```

- [ ] **Step 3: Commit**

```bash
git add app/agents/storage_agent.py
git commit -m "feat(storage): StorageAgent sem LLM substitui LabelerStorageAgent"
```

---

## Task 8: extractor_agent.py + testes de helpers

**Files:**
- Update: `app/agents/extractor_agent.py`
- Create: `tests/test_extractor_helpers.py`

- [ ] **Step 1: Escrever testes que vão falhar**

Criar `tests/test_extractor_helpers.py`:

```python
import pytest


# --- _make_row_key ---

def test_make_row_key_is_stable():
    from app.agents.extractor_agent import _make_row_key
    row = {
        "Nome do hotel": "Hotel ABC",
        "Cidade": "São Paulo",
        "Check-in": "2024-03-10",
        "Check-out": "2024-03-12",
        "Categoria do quarto": "Standard",
        "Configuração do quarto": "Double",
        "Preço (num)": 350.0,
    }
    key1 = _make_row_key("thread123", row)
    key2 = _make_row_key("thread123", row)
    assert key1 == key2
    assert len(key1) == 40  # SHA-1 hex


def test_make_row_key_differs_by_price():
    from app.agents.extractor_agent import _make_row_key
    row_a = {"Nome do hotel": "H", "Cidade": "SP", "Check-in": "2024-03-10",
             "Check-out": "2024-03-12", "Categoria do quarto": "Std",
             "Configuração do quarto": "DBL", "Preço (num)": 300}
    row_b = {**row_a, "Preço (num)": 400}
    assert _make_row_key("t1", row_a) != _make_row_key("t1", row_b)


def test_make_row_key_differs_by_thread():
    from app.agents.extractor_agent import _make_row_key
    row = {"Nome do hotel": "H", "Cidade": "SP", "Check-in": "2024-03-10",
           "Check-out": "2024-03-12", "Categoria do quarto": "Std",
           "Configuração do quarto": "DBL", "Preço (num)": 300}
    assert _make_row_key("thread_a", row) != _make_row_key("thread_b", row)


# --- _score_message ---

def test_score_external_sender_with_price():
    from app.agents.extractor_agent import _score_message
    body = "Tarifa: R$ 350,00 por diária. Quarto standard disponível para check-in."
    score = _score_message("reservas@hotel.com.br", body)
    assert score > 0


def test_score_parrot_sender_penalized():
    from app.agents.extractor_agent import _score_message
    body = "Tarifa: R$ 350,00 por diária. Quarto standard disponível."
    score_external = _score_message("hotel@externo.com", body)
    score_parrot = _score_message("fulano@parrottrips.com", body)
    assert score_external > score_parrot


def test_score_short_thanks_message():
    from app.agents.extractor_agent import _score_message
    score = _score_message("hotel@externo.com", "Obrigado pelo contato!")
    assert score < 0


def test_score_empty_body():
    from app.agents.extractor_agent import _score_message
    assert _score_message("hotel@externo.com", "") == -999


# --- _strip_reply_history_and_signature ---

def test_strip_removes_forwarded_block():
    from app.agents.extractor_agent import _strip_reply_history_and_signature
    body = "Preço R$ 200\n\n-----Mensagem Original-----\nDe: alguém"
    result = _strip_reply_history_and_signature(body)
    assert "200" in result
    assert "Mensagem Original" not in result


def test_strip_removes_quoted_lines():
    from app.agents.extractor_agent import _strip_reply_history_and_signature
    body = "Tarifa disponível\n> Em 10 mar, alguém escreveu:\n> texto antigo"
    result = _strip_reply_history_and_signature(body)
    assert "Tarifa disponível" in result
    assert "texto antigo" not in result


def test_strip_removes_disclaimer():
    from app.agents.extractor_agent import _strip_reply_history_and_signature
    body = "Tarifa R$300\n\nEsta mensagem é confidencial e pode conter informação"
    result = _strip_reply_history_and_signature(body)
    assert "300" in result
    assert "confidencial" not in result
```

- [ ] **Step 2: Rodar para confirmar falha**

```bash
python -m pytest tests/test_extractor_helpers.py -v
```

Esperado: `ImportError` — módulo ainda não atualizado.

- [ ] **Step 3: Reescrever app/agents/extractor_agent.py**

```python
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
```

- [ ] **Step 4: Rodar testes para confirmar que passam**

```bash
python -m pytest tests/test_extractor_helpers.py -v
```

Esperado: 10 testes passando.

- [ ] **Step 5: Commit**

```bash
git add app/agents/extractor_agent.py tests/test_extractor_helpers.py
git commit -m "fix(extractor): lazy init, remove singleton global, helpers testados"
```

---

## Task 9: sheets_sync_agent.py

**Files:**
- Rewrite: `app/agents/sheets_sync_agent.py`

- [ ] **Step 1: Reescrever app/agents/sheets_sync_agent.py**

```python
import logging
from typing import Any, Dict, List

import gspread

from app.core.config import settings
from app.core.gcs_client import make_gcs_client
from app.agents.extractor_agent import HEADER_FIELDS

logger = logging.getLogger(__name__)

EXTRA_FIELDS = ["_thread_id", "_row_index_in_thread", "_key"]
ALL_FIELDS = HEADER_FIELDS + EXTRA_FIELDS


class SheetsSyncAgent:
    def run(self) -> None:
        gcs = make_gcs_client()

        data = gcs.download_json("tables/quotes_raw.json")
        if not data:
            logger.info("quotes_raw.json vazio ou ausente, nada a sincronizar.")
            return

        if not isinstance(data, list):
            logger.error("Formato inesperado em quotes_raw.json (esperado: lista).")
            return

        quotes: List[Dict[str, Any]] = data
        logger.info("%d linhas lidas de quotes_raw.json.", len(quotes))

        gc = gspread.service_account(
            filename=settings.SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        sh = gc.open_by_key(settings.SHEETS_SPREADSHEET_ID)

        try:
            ws = sh.worksheet(settings.SHEETS_QUOTE_SHEET_NAME)
            logger.info("Aba '%s' encontrada.", settings.SHEETS_QUOTE_SHEET_NAME)
        except gspread.WorksheetNotFound:
            logger.info("Aba '%s' não encontrada, criando...", settings.SHEETS_QUOTE_SHEET_NAME)
            ws = sh.add_worksheet(
                title=settings.SHEETS_QUOTE_SHEET_NAME,
                rows="1000",
                cols=str(len(ALL_FIELDS) + 5),
            )

        if not ws.row_values(1):
            ws.update("A1", [ALL_FIELDS])
            logger.info("Cabeçalho escrito em A1.")

        key_col_index = ALL_FIELDS.index("_key") + 1
        existing_keys = set(ws.col_values(key_col_index)[1:])
        logger.info("%d chaves existentes na planilha.", len(existing_keys))

        new_rows: List[List[Any]] = []
        for q in quotes:
            key = q.get("_key")
            if not key or key in existing_keys:
                continue
            new_rows.append([q.get(field, "") for field in ALL_FIELDS])
            existing_keys.add(key)

        if not new_rows:
            logger.info("Nenhuma linha nova para adicionar.")
            return

        ws.append_rows(new_rows, value_input_option="RAW")
        logger.info("%d novas linhas adicionadas à planilha.", len(new_rows))


def run_sheets_sync() -> None:
    SheetsSyncAgent().run()
```

- [ ] **Step 2: Commit**

```bash
git add app/agents/sheets_sync_agent.py
git commit -m "fix(sheets): gspread.service_account, corrige atributo SHEETS_QUOTE_SHEET_NAME, lazy init"
```

---

## Task 10: main.py

**Files:**
- Update: `main.py`

- [ ] **Step 1: Reescrever main.py**

```python
import logging

from app.agents.storage_agent import run_storage
from app.agents.extractor_agent import run_extractor
from app.agents.sheets_sync_agent import run_sheets_sync


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    logging.info("STEP 1/3 — StorageAgent (Gmail QUOTES → GCS)...")
    run_storage()
    logging.info("STEP 1/3 — OK")

    logging.info("STEP 2/3 — ExtractorAgent (GCS → LLM → tables)...")
    run_extractor()
    logging.info("STEP 2/3 — OK")

    logging.info("STEP 3/3 — SheetsSyncAgent (tables → Google Sheets)...")
    run_sheets_sync()
    logging.info("STEP 3/3 — OK")

    logging.info("Pipeline concluído com sucesso.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Rodar todos os testes para confirmar que nada quebrou**

```bash
python -m pytest tests/ -v
```

Esperado: 15 testes passando (5 de llm_parsing + 10 de extractor_helpers).

- [ ] **Step 3: Commit**

```bash
git add main.py
git commit -m "fix(main): usa StorageAgent, sem singletons globais"
```

---

## Task 11: scripts/generate_gmail_token.py

**Files:**
- Create: `scripts/__init__.py` (vazio)
- Create: `scripts/generate_gmail_token.py`

- [ ] **Step 1: Criar scripts/generate_gmail_token.py**

```python
#!/usr/bin/env python3
"""
Gera ou renova o token OAuth do Gmail.

Pré-requisito: credentials/gmail_client_secret.json baixado do Google Cloud Console
  (APIs & Services -> Credentials -> OAuth 2.0 Client IDs -> Download JSON)

Uso:
    python scripts/generate_gmail_token.py
"""
import json
import os
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]
CLIENT_SECRET_FILE = "credentials/gmail_client_secret.json"
TOKEN_OUTPUT = "credentials/gmail-token.json"


def _check_testing_mode(client_secret_path: str) -> None:
    try:
        with open(client_secret_path) as f:
            data = json.load(f)
        client_type = list(data.keys())[0]
        print(
            "\n[AVISO] Verifique se o OAuth app está em modo 'In production' no Google Cloud Console."
            "\n        Apps em modo 'Testing' têm refresh tokens que expiram em 7 dias."
            "\n        Caminho: APIs & Services → OAuth consent screen → Publishing status"
        )
    except Exception:
        pass


def main() -> None:
    if not os.path.exists(CLIENT_SECRET_FILE):
        print(f"ERRO: {CLIENT_SECRET_FILE} não encontrado.")
        print(
            "Baixe em: Google Cloud Console → APIs & Services → Credentials"
            " → OAuth 2.0 Client IDs → Download JSON"
        )
        sys.exit(1)

    _check_testing_mode(CLIENT_SECRET_FILE)

    print("\nAbrindo browser para autenticação...")
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
    creds = flow.run_local_server(port=0)

    os.makedirs("credentials", exist_ok=True)
    with open(TOKEN_OUTPUT, "w") as f:
        f.write(creds.to_json())

    print(f"\nToken salvo em: {TOKEN_OUTPUT}")
    print("\nPróximo passo — atualizar no Secret Manager:")
    print(
        "  gcloud secrets versions add gmail-token"
        f" --data-file={TOKEN_OUTPUT}"
        " --project=louro-jose-479223"
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add scripts/generate_gmail_token.py
git commit -m "feat(scripts): generate_gmail_token.py para renovar token OAuth"
```

---

## Task 12: scripts/setup_gmail_filters.py

**Files:**
- Create: `scripts/setup_gmail_filters.py`

- [ ] **Step 1: Criar scripts/setup_gmail_filters.py**

```python
#!/usr/bin/env python3
"""
Configura filtros Gmail para rotular emails de cotação com QUOTES.

Etapa 1: Garante que o label QUOTES existe.
Etapa 2: Cria filtros Gmail (aplicados em emails futuros).
Etapa 3: Varredura retroativa — aplica QUOTES em emails existentes que batem nas keywords.

Uso:
    python scripts/setup_gmail_filters.py
    python scripts/setup_gmail_filters.py --lookback-days 365
"""
import argparse
import datetime
import logging
import sys

from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]
TOKEN_FILE = "credentials/gmail-token.json"
LABEL_NAME = "QUOTES"

# Keywords para filtro por assunto
SUBJECT_KEYWORDS = [
    "cotação", "cotacao",
    "tarifa",
    "diária", "diaria",
    "proposta",
    "disponibilidade",
    "hospedagem",
    "reserva",
]

# Frases para filtro por corpo
BODY_PHRASES = [
    "cotação hotel", "cotacao hotel",
    "tarifa hotel",
    "proposta comercial",
    "diária hotel", "diaria hotel",
]

SUBJECT_QUERY = "subject:(" + " OR ".join(SUBJECT_KEYWORDS) + ")"
BODY_QUERY = " OR ".join(f'"{phrase}"' for phrase in BODY_PHRASES)
COMBINED_QUERY = f"({SUBJECT_QUERY}) OR ({BODY_QUERY})"


def get_or_create_label(service, label_name: str) -> str:
    resp = service.users().labels().list(userId="me").execute()
    for label in resp.get("labels", []):
        if label["name"].lower() == label_name.lower():
            logger.info("Label '%s' já existe (id=%s).", label_name, label["id"])
            return label["id"]
    body = {
        "name": label_name,
        "labelListVisibility": "labelShow",
        "messageListVisibility": "show",
    }
    created = service.users().labels().create(userId="me", body=body).execute()
    logger.info("Label '%s' criado (id=%s).", label_name, created["id"])
    return created["id"]


def create_filters(service, label_id: str) -> None:
    existing_resp = service.users().settings().filters().list(userId="me").execute()
    existing_queries = {
        f.get("criteria", {}).get("query", "")
        for f in existing_resp.get("filter", [])
    }

    filters_to_create = [
        {"criteria": {"query": SUBJECT_QUERY}, "action": {"addLabelIds": [label_id]}},
        {"criteria": {"query": BODY_QUERY}, "action": {"addLabelIds": [label_id]}},
    ]

    for body in filters_to_create:
        query = body["criteria"]["query"]
        if query in existing_queries:
            logger.info("Filtro já existe para: %.60s...", query)
            continue
        result = service.users().settings().filters().create(
            userId="me", body=body
        ).execute()
        logger.info("Filtro criado (id=%s): %.60s...", result.get("id"), query)


def apply_label_retroactively(service, label_id: str, lookback_days: int) -> int:
    since = (
        datetime.date.today() - datetime.timedelta(days=lookback_days)
    ).strftime("%Y/%m/%d")
    query = f"({COMBINED_QUERY}) after:{since} -label:{LABEL_NAME}"
    logger.info("Buscando emails sem '%s': %s", LABEL_NAME, query)

    messages = []
    page_token = None
    while True:
        resp = service.users().messages().list(
            userId="me", q=query, pageToken=page_token
        ).execute()
        messages.extend(resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    total = len(messages)
    logger.info("%d emails encontrados para rotular.", total)

    for i, msg in enumerate(messages, 1):
        service.users().messages().modify(
            userId="me",
            id=msg["id"],
            body={"addLabelIds": [label_id]},
        ).execute()
        if i % 20 == 0 or i == total:
            logger.info("Progresso: %d/%d", i, total)

    return total


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Configura filtros Gmail para o pipeline Louro José."
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=180,
        help="Quantos dias retroativos varrer para aplicar QUOTES (padrão: 180).",
    )
    args = parser.parse_args()

    try:
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, scopes=GMAIL_SCOPES)
    except FileNotFoundError:
        logger.error(
            "Token não encontrado em %s. Execute: python scripts/generate_gmail_token.py",
            TOKEN_FILE,
        )
        sys.exit(1)

    service = build("gmail", "v1", credentials=creds)

    logger.info("=== Etapa 1: Garantindo label '%s' ===", LABEL_NAME)
    label_id = get_or_create_label(service, LABEL_NAME)

    logger.info("=== Etapa 2: Criando filtros Gmail ===")
    create_filters(service, label_id)

    logger.info("=== Etapa 3: Varredura retroativa (%d dias) ===", args.lookback_days)
    count = apply_label_retroactively(service, label_id, args.lookback_days)

    logger.info("=== Setup concluído: %d emails rotulados retroativamente ===", count)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add scripts/setup_gmail_filters.py
git commit -m "feat(scripts): setup_gmail_filters.py — filtros Gmail + varredura retroativa"
```

---

## Task 13: Reconfigurar Cloud Run Job

Esta task é executada via linha de comando — não envolve código.

**Pré-requisito:** OAuth app publicado no Google Cloud Console e novo token gerado e enviado ao Secret Manager (Tasks 11 + fluxo de renovação).

- [ ] **Step 1: Remover env vars inválidas e reconfigurar o job com volume mounts**

```bash
gcloud run jobs update louro-jose-job \
  --region=us-central1 \
  --project=louro-jose-479223 \
  --set-env-vars="GCP_PROJECT_ID=louro-jose-479223,GCS_BUCKET=parrot-agents-dev,GMAIL_LABEL=QUOTES,SHEETS_SPREADSHEET_ID=1ukVazqwLD771HVpyEz4Edb9E8w3Bdqe_6tXlujTe5LI,SHEETS_QUOTE_SHEET_NAME=quotes_raw,OPENAI_BASE_URL=https://api.openai.com/v1,OPENAI_MODEL=gpt-4o,GMAIL_TOKEN_FILE=/secrets/gmail-token.json,SERVICE_ACCOUNT_FILE=/secrets/service-account.json" \
  --set-secrets="OPENAI_API_KEY=openai-api-key:latest,/secrets/gmail-token.json=gmail-token:latest,/secrets/service-account.json=service-account-json:latest"
```

- [ ] **Step 2: Verificar que a configuração ficou correta**

```bash
gcloud run jobs describe louro-jose-job \
  --region=us-central1 \
  --project=louro-jose-479223 \
  --format="yaml(spec.template.spec.template.spec)"
```

Confirmar que:
- `GMAIL_TOKEN_FILE` = `/secrets/gmail-token.json`
- `SERVICE_ACCOUNT_FILE` = `/secrets/service-account.json`
- `OPENAI_API_KEY` vem de `openai-api-key:latest`
- Os volumes `/secrets/gmail-token.json` e `/secrets/service-account.json` estão montados

- [ ] **Step 3: Build e deploy da nova imagem**

```bash
gcloud builds submit \
  --tag gcr.io/louro-jose-479223/louro-jose:$(date +%Y%m%d) \
  --project=louro-jose-479223 .

gcloud run jobs update louro-jose-job \
  --image gcr.io/louro-jose-479223/louro-jose:$(date +%Y%m%d) \
  --region=us-central1 \
  --project=louro-jose-479223
```

- [ ] **Step 4: Executar manualmente para validar**

```bash
gcloud run jobs execute louro-jose-job \
  --region=us-central1 \
  --project=louro-jose-479223 \
  --wait
```

Esperado: job termina com status `SUCCEEDED`.

- [ ] **Step 5: Verificar logs da execução**

```bash
gcloud logging read \
  "resource.type=cloud_run_job AND resource.labels.job_name=louro-jose-job" \
  --project=louro-jose-479223 \
  --limit=50 \
  --format="value(timestamp,textPayload)" \
  --freshness=1h
```

Esperado: logs mostrando os 3 steps do pipeline com `OK` no final.

- [ ] **Step 6: Commit final da branch e PR**

```bash
git add .
git commit -m "docs: atualiza README com novo fluxo de setup"
```

```bash
gh pr create \
  --title "rebuild: pipeline Gmail→GCS→Sheets sem LLM de classificação" \
  --body "Rebuild completo conforme spec docs/superpowers/specs/2026-05-24-rebuild-design.md"
```
