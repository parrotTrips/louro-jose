# Louro José — Pipeline Gmail → GCS → Google Sheets

Pipeline de cotações de viagem executado como Cloud Run Job, 2x por dia.

## Visão geral

O `main.py` roda três agentes em sequência:

1. **StorageAgent** — lê threads do Gmail com label `QUOTES` e salva no GCS
2. **ExtractorAgent** — lê threads do GCS, chama LLM, salva cotações estruturadas no GCS
3. **SheetsSyncAgent** — lê cotações do GCS e sincroniza na planilha Google Sheets

A classificação de emails **não usa LLM**: quem aplica o label `QUOTES` são filtros nativos do Gmail (criados por `scripts/setup_gmail_filters.py`).

## Fluxo de dados

```
Gmail (threads com label QUOTES)
  → GCS  threads/<thread_id>.json
         state/threads_state.json
  → LLM (ExtractorAgent)
  → GCS  tables/quotes_raw.json
         tables/quotes_history.json
  → Google Sheets (aba quotes_raw)
```

## Agendamento

Cloud Run Job (`louro-jose-job`), disparado pelo Cloud Scheduler:

- `12:00` (horário conforme timezone configurado no Scheduler)
- `17:00`

Em cada disparo, o pipeline completo é executado do início ao fim.

## O que cada agente faz

### 1) StorageAgent (`app/agents/storage_agent.py`)

- Lista todas as threads Gmail com o label `QUOTES` (com paginação completa).
- Para cada thread, compara o `last_message_id` com `state/threads_state.json`.
- Se houver novidade, salva `threads/<thread_id>.json` no GCS e atualiza o estado.

Resultado: GCS atualizado com threads novas ou modificadas; nenhuma chamada LLM.

### 2) ExtractorAgent (`app/agents/extractor_agent.py`)

- Lista todos os arquivos em `threads/*.json` no GCS.
- Para cada thread, verifica `state/extractor_state.json` e pula se não houve mudança.
- Chama o LLM para extrair campos estruturados de cotação.
- Salva:
  - `tables/quotes_raw.json` — lote da execução atual
  - `tables/quotes_history.json` — histórico acumulado (deduplicado por `_key`)

### 3) SheetsSyncAgent (`app/agents/sheets_sync_agent.py`)

- Lê `tables/quotes_raw.json` do GCS.
- Abre a planilha configurada por `SHEETS_SPREADSHEET_ID` (aba `SHEETS_QUOTE_SHEET_NAME`).
- Garante cabeçalho da aba.
- Faz append apenas das linhas cujo `_key` ainda não existe na planilha.

## Credenciais

Dois arquivos em `credentials/` (não commitado no repositório):

| Arquivo | Descrição |
|---|---|
| `gmail-token.json` | Token OAuth 2.0 do Gmail — gerado por `scripts/generate_gmail_token.py` |
| `service-account.json` | Service Account com acesso ao GCS e Google Sheets |

No Cloud Run, esses arquivos são montados via volume mounts (ver seção de setup).

## Variáveis de ambiente

Ver `.env.example`. Variáveis principais:

| Variável | Descrição |
|---|---|
| `GCP_PROJECT_ID` | ID do projeto GCP |
| `GCS_BUCKET` | Nome do bucket GCS |
| `GMAIL_LABEL` | Label Gmail a monitorar (padrão: `QUOTES`) |
| `GMAIL_TOKEN_FILE` | Caminho para `gmail-token.json` (padrão: `credentials/gmail-token.json`) |
| `SERVICE_ACCOUNT_FILE` | Caminho para `service-account.json` (padrão: `credentials/service-account.json`) |
| `OPENAI_API_KEY` | Chave da API OpenAI |
| `OPENAI_BASE_URL` | URL base da API (padrão: `https://api.openai.com/v1`) |
| `OPENAI_MODEL` | Modelo a usar (padrão: `gpt-4o`) |
| `SHEETS_SPREADSHEET_ID` | ID da planilha Google Sheets |
| `SHEETS_QUOTE_SHEET_NAME` | Nome da aba de destino (padrão: `quotes_raw`) |

## Arquivos no bucket GCS

```
threads/<thread_id>.json       # thread completa do Gmail
state/threads_state.json       # estado incremental do StorageAgent
state/extractor_state.json     # estado incremental do ExtractorAgent
tables/quotes_raw.json         # lote da última execução
tables/quotes_history.json     # histórico acumulado de todas as execuções
```

## Execução local

```bash
python main.py
```

## Desenvolvimento local

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -v
```

## Setup inicial (uma vez)

Passos necessários antes do primeiro deploy ou ao renovar credenciais.

### 1. Publicar o OAuth app no Google Cloud Console

No Console GCP → APIs & Services → OAuth consent screen → Publishing status → **Publish App**.

Enquanto o app estiver em modo "Testing", o token Gmail expira em 7 dias. Em modo "In production", a validade é de 6 meses (com refresh automático).

### 2. Gerar o token Gmail

```bash
python scripts/generate_gmail_token.py
```

Isso abre o fluxo OAuth no navegador e salva `credentials/gmail-token.json`.

### 3. Atualizar o Secret Manager com o novo token

```bash
gcloud secrets versions add gmail-token \
  --data-file=credentials/gmail-token.json \
  --project=louro-jose-479223
```

### 4. Criar filtros Gmail e fazer varredura retroativa

```bash
python scripts/setup_gmail_filters.py --lookback-days 180
```

Cria os filtros nativos do Gmail que aplicam `QUOTES` automaticamente em emails novos.
O flag `--lookback-days` aplica o label retroativamente nos emails dos últimos N dias.

### 5. Reconfigurar o Cloud Run Job

Os secrets que contêm `/` no nome precisam ser montados como arquivos (volume mounts), não como env vars. Execute o comando abaixo para reconfigurar o job:

```bash
gcloud run jobs update louro-jose-job \
  --region=us-central1 \
  --project=louro-jose-479223 \
  --set-env-vars="GCP_PROJECT_ID=louro-jose-479223,GCS_BUCKET=parrot-agents-dev,GMAIL_LABEL=QUOTES,SHEETS_SPREADSHEET_ID=1ukVazqwLD771HVpyEz4Edb9E8w3Bdqe_6tXlujTe5LI,SHEETS_QUOTE_SHEET_NAME=quotes_raw,OPENAI_BASE_URL=https://api.openai.com/v1,OPENAI_MODEL=gpt-4o,GMAIL_TOKEN_FILE=/secrets/gmail-token.json,SERVICE_ACCOUNT_FILE=/secrets/service-account.json" \
  --set-secrets="OPENAI_API_KEY=openai-api-key:latest,/secrets/gmail-token.json=gmail-token:latest,/secrets/service-account.json=service-account-json:latest"
```

### 6. Build e deploy da imagem

```bash
gcloud builds submit \
  --tag gcr.io/louro-jose-479223/louro-jose:latest \
  --project=louro-jose-479223 .

gcloud run jobs update louro-jose-job \
  --image gcr.io/louro-jose-479223/louro-jose:latest \
  --region=us-central1 \
  --project=louro-jose-479223
```

### 7. Executar manualmente para validar

```bash
gcloud run jobs execute louro-jose-job \
  --region=us-central1 \
  --project=louro-jose-479223 \
  --wait
```

## Comportamento incremental

- **StorageAgent**: só regrava thread no GCS quando há novo `last_message_id`.
- **ExtractorAgent**: só reextrai thread quando o último email mudou.
- **SheetsSyncAgent**: só adiciona linha se o `_key` ainda não existe na planilha.

Isso reduz custo de LLM, evita reprocessamento desnecessário e garante idempotência entre as execuções das 12h e 17h.
