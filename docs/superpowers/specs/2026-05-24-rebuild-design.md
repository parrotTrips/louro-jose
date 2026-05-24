# Louro José — Rebuild Design

**Data:** 2026-05-24  
**Branch:** ai_agents  
**Status:** Aprovado

---

## Contexto e motivação

O pipeline atual está quebrado em produção. Causa raiz confirmada via logs do Cloud Run:

```
google.auth.exceptions.RefreshError: invalid_grant: Token has been expired or revoked.
```

O OAuth token do Gmail estava baked na imagem Docker e expirou. A configuração de secrets no Cloud Run estava incorreta (env vars com `/` no nome, que o Cloud Run não aceita), então o código nunca leu os secrets do Secret Manager e usou o arquivo da imagem. Além disso, o OAuth app estava em modo "Testing", que expira refresh tokens em 7 dias.

Outros bugs identificados na análise do código: falta de paginação no Gmail, atributo de config errado no SheetsSyncAgent, singletons instanciados no import (falha silenciosa), `gspread.authorize()` deprecado, `print()` em vez de `logging`, dependências desnecessárias no `requirements.txt`.

O rebuild resolve todos esses problemas e simplifica o pipeline removendo a classificação via LLM (substituída por filtros nativos do Gmail).

---

## Arquitetura

### Fluxo completo

```
[uma vez]  scripts/setup_gmail_filters.py
               → cria filtros no Gmail (keywords → label QUOTES)
               → varredura retroativa: aplica QUOTES em emails existentes

[agendado 2x/dia]  main.py  (Cloud Run Job + Cloud Scheduler)
               ↓
           [1] StorageAgent
               Gmail (threads com QUOTES) → GCS threads/*.json
               incremental por last_email_id
               ↓
           [2] ExtractorAgent
               GCS threads/*.json → LLM → GCS tables/quotes_raw.json
                                        → GCS tables/quotes_history.json
               incremental por last_message_id
               ↓
           [3] SheetsSyncAgent
               GCS tables/quotes_raw.json → Google Sheets
               deduplicação por _key
```

### Estrutura de arquivos

```
louro-jose/
├── app/
│   ├── __init__.py
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── storage_agent.py
│   │   ├── extractor_agent.py
│   │   └── sheets_sync_agent.py
│   └── core/
│       ├── __init__.py
│       ├── config.py
│       ├── gmail_client.py
│       ├── gcs_client.py
│       ├── llm_client.py
│       └── state.py
├── scripts/
│   ├── setup_gmail_filters.py
│   └── generate_gmail_token.py
├── main.py
├── Dockerfile
├── requirements.txt
├── .env.example
├── .dockerignore
└── .gitignore
```

---

## Seção 1: Classificação de emails

### Decisão: filtros nativos do Gmail (sem LLM)

O `LabelerStorageAgent` atual usa LLM para classificar emails como VIAGEM/NAO-VIAGEM. Isso é substituído por filtros do Gmail, que:

- Aplicam o label QUOTES automaticamente em emails novos que batem nas keywords
- São gratuitos e instantâneos (sem custo de LLM para classificação)
- Removem um ponto de falha do pipeline

### Keywords dos filtros

**Filtro por assunto:**
```
subject:(cotação OR tarifa OR diária OR proposta OR disponibilidade OR hospedagem OR reserva)
```

**Filtro por corpo:**
```
cotação hotel OR tarifa hotel OR proposta comercial OR diária hotel
```

As keywords ficam como constante no topo de `scripts/setup_gmail_filters.py` para fácil manutenção.

### Varredura retroativa

O `setup_gmail_filters.py` aceita `--lookback-days` (default: 180). Para emails existentes que batem nas keywords mas ainda não têm QUOTES, o script aplica o label em lote.

---

## Seção 2: Credenciais e autenticação

### Dois tipos de credencial

| Credencial | Uso | Armazenamento |
|---|---|---|
| `gmail-token.json` | Gmail API (OAuth 2.0 user token) | Secret Manager `gmail-token` |
| `service-account.json` | GCS + Google Sheets | Secret Manager `service-account-json` |
| `OPENAI_API_KEY` | LLM | Secret Manager `openai-api-key` (env var) |

### Fix do token Gmail

**Causa do problema:** OAuth consent screen em modo "Testing" → refresh tokens expiram em 7 dias.

**Solução:**
1. Publicar o OAuth app no Google Cloud Console (OAuth consent screen → "In production")
2. Regenerar o token com `scripts/generate_gmail_token.py`
3. Atualizar o secret no Secret Manager

**Script de renovação:**
```bash
python scripts/generate_gmail_token.py
# → lê credentials/gmail_client_secret.json
# → abre browser para autenticação OAuth
# → salva credentials/gmail-token.json

gcloud secrets versions add gmail-token \
  --data-file=credentials/gmail-token.json \
  --project=louro-jose-479223
```

### Fix da montagem de secrets no Cloud Run

**Bug atual:** secrets configurados como env vars com nomes `credentials/gmail-token.json` (inválido — Cloud Run não aceita `/` em nome de env var).

**Fix:** montar como arquivos via Cloud Run volume mounts:

| Secret Manager | Montado em |
|---|---|
| `gmail-token` | `/secrets/gmail-token.json` |
| `service-account-json` | `/secrets/service-account.json` |
| `openai-api-key` | env var `OPENAI_API_KEY` |

Env vars adicionais no Cloud Run:
```
GMAIL_TOKEN_FILE=/secrets/gmail-token.json
SERVICE_ACCOUNT_FILE=/secrets/service-account.json
```

### Desenvolvimento local

Credenciais ficam em `credentials/` (não commitado). `.env` aponta para os arquivos locais:
```
GMAIL_TOKEN_FILE=credentials/gmail-token.json
SERVICE_ACCOUNT_FILE=credentials/service-account.json
```

---

## Seção 3: Os três agentes

### `StorageAgent` (`app/agents/storage_agent.py`)

Responsabilidade: sincronizar threads com QUOTES do Gmail para o GCS.

**Sem LLM.** O agente não classifica nada — apenas sincroniza.

```
run()
  ├── Garante label QUOTES existe no Gmail
  ├── Lista todas as threads com QUOTES (com paginação — corrige bug atual)
  └── Para cada thread:
        ├── Baixa thread completa do Gmail
        ├── Lê last_email_id do estado (GCS state/threads_state.json)
        ├── Se não mudou → pula
        └── Se mudou → salva threads/<thread_id>.json + atualiza estado
```

**Correções vs `LabelerStorageAgent` atual:**
- Remove toda a fase de classificação LLM
- `list_threads_with_label` com paginação (bug atual: só primeira página ~100 threads)
- Lazy init de dependências
- `logging` ao invés de `print()`

### `ExtractorAgent` (`app/agents/extractor_agent.py`)

Responsabilidade: transformar threads brutas em linhas estruturadas de cotação.

```
run()
  ├── Lista blobs em threads/*.json no GCS
  ├── Carrega estado incremental (state/extractor_state.json)
  └── Para cada thread:
        ├── Verifica last_message_id → pula se não mudou
        ├── Monta texto limpo (_build_clean_thread_text — lógica mantida)
        ├── Chama LLM (extract_quotes)
        ├── Gera _key estável por cotação
        └── Em erro: loga + continua (não marca como processada)

  Após loop:
  ├── Salva tables/quotes_raw.json (lote desta execução)
  ├── Atualiza tables/quotes_history.json (acumulado, deduplicado por _key)
  └── Salva estado atualizado
```

**Correções vs hoje:**
- Parse de JSON do LLM com regex (mais robusto que `strip("`")`)
- `logging` ao invés de `print()`
- Lazy init

### `SheetsSyncAgent` (`app/agents/sheets_sync_agent.py`)

Responsabilidade: publicar o lote atual na planilha sem duplicar.

```
run()
  ├── Lê tables/quotes_raw.json do GCS
  ├── Abre planilha por SHEETS_SPREADSHEET_ID
  ├── Abre ou cria aba (nome de SHEETS_QUOTE_SHEET_NAME)
  ├── Garante cabeçalho na linha 1
  ├── Lê _keys existentes
  ├── Filtra linhas com _key novo
  └── Append das linhas novas
```

**Correções vs hoje:**
- `getattr(settings, "SHEETS_WORKSHEET_NAME")` → `settings.SHEETS_QUOTE_SHEET_NAME` (bug de atributo)
- `gspread.authorize()` → `gspread.service_account(filename=..., scopes=[...])` (API atual gspread 6.x, mais simples que passar Credentials manualmente)
- Lazy init

### `main.py`

```python
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_storage()      # STEP 1/3
    run_extractor()    # STEP 2/3
    run_sheets_sync()  # STEP 3/3
```

Cada `run_*` instancia o agente dentro da função. Sem singletons globais no nível de módulo.

### Core modules — mudanças

| Módulo | Mudança |
|---|---|
| `config.py` | `GMAIL_TOKEN_FILE` e `SERVICE_ACCOUNT_FILE` lidos de env var (não hardcoded) |
| `gmail_client.py` | `list_threads_with_label` com paginação; lazy init |
| `gcs_client.py` | Lazy init (conecta só na primeira chamada) |
| `llm_client.py` | Lazy init; sem mudança de lógica |
| `state.py` | Carregado quando `run()` é chamado, não no import |

---

## Seção 4: Scripts, dependências e infraestrutura

### `scripts/generate_gmail_token.py`

Roda localmente. Abre browser para autenticação OAuth e salva `credentials/gmail-token.json`. Avisa se o OAuth app ainda está em modo Testing.

### `scripts/setup_gmail_filters.py`

Roda localmente uma vez (ou quando keywords mudam).

```
Etapa 1 — Garante label QUOTES existe
Etapa 2 — Cria filtros Gmail (novos emails)
Etapa 3 — Varredura retroativa (--lookback-days, default 180)
           → busca emails sem QUOTES que batem na query
           → aplica label em lote
           → imprime contagem
```

### `requirements.txt`

Apenas dependências diretas, sem pins de versão desnecessários:

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

Remove: `openai` (não importado), `google-genai`, `google-generativeai`, `beautifulsoup4`, `oauth2client` (deprecado).

### `Dockerfile`

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

### `.dockerignore`

```
credentials/
scripts/
env/
.env
.git
__pycache__
*.pyc
.DS_Store
```

### `.env.example`

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

---

## Resumo de setup (uma vez por ambiente)

```bash
# 1. Publicar OAuth app no Google Cloud Console
#    APIs & Services → OAuth consent screen → Publishing status → Publish App

# 2. Gerar token Gmail
python scripts/generate_gmail_token.py
gcloud secrets versions add gmail-token \
  --data-file=credentials/gmail-token.json \
  --project=louro-jose-479223

# 3. Reconfigurar Cloud Run job
#    Trocar env vars quebradas por volume mounts dos secrets

# 4. Criar filtros Gmail + varredura retroativa
python scripts/setup_gmail_filters.py --lookback-days 180

# 5. Build e deploy
gcloud builds submit --tag gcr.io/louro-jose-479223/louro-jose:latest
gcloud run jobs update louro-jose-job \
  --image gcr.io/louro-jose-479223/louro-jose:latest \
  --region us-central1
```
