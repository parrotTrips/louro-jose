# Parrot Agents – Labeler + Storage (Gmail → GCS)

Este repositório implementa a **primeira etapa** de um pipeline baseado em agentes para processar cotações de viagem recebidas por e-mail.

Neste estágio, temos **um agente principal**:

> **LabelerStorageAgent**
> 1. Usa um **LLM** para classificar emails como *VIAGEM* ou *NAO-VIAGEM*  
> 2. Aplica o rótulo **`QUOTES`** no Gmail para emails de viagem  
> 3. Lê todas as **threads** com `QUOTES`  
> 4. Salva essas threads no **Google Cloud Storage (GCS)**  
> 5. Mantém um **estado incremental** para não reprocessar a mesma thread sempre

Este README explica:

- Estrutura do projeto  
- Pré-requisitos (APIs, credenciais, `.env`)  
- Fluxo detalhado do Labeler  
- O que você deve ver no Gmail e no GCS ao rodar  
- Como rodar localmente  
- Próximos passos naturais (Agente Extrator)


## 1. Estrutura do projeto

Estrutura recomendada:

    project/
    │── credentials/
    │     ├── parrot-gmails.json          # token OAuth do Gmail (já autorizado)
    │     └── service-account.json        # chave JSON da service account do GCP
    │
    │── .env                              # variáveis de ambiente do projeto
    │── .gitignore
    │── main.py                           # entrypoint do Labeler
    │
    └── app/
          ├── core/
          │     ├── config.py             # carrega .env e paths de credencial
          │     ├── gmail_client.py       # acesso ao Gmail (labels, mensagens, threads)
          │     ├── gcs_client.py         # acesso ao Google Cloud Storage
          │     ├── state.py              # controle de estado (threads já processadas)
          │     └── llm_client.py         # chamadas ao LLM via OpenRouter
          │
          └── agents/
                ├── __init__.py
                └── labeler_storage_agent.py  # agente Labeler + Storage (2 fases)


## 2. Pré-requisitos de ambiente

### 2.1. Bibliotecas Python

Dentro do seu `venv`, instale:

    pip install python-dotenv google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2 google-cloud-storage requests

Essas libs cobrem:

- `.env` → `python-dotenv`
- Gmail API → `google-api-python-client`, `google-auth-*`
- Cloud Storage → `google-cloud-storage`
- LLM via HTTP → `requests`


### 2.2. Projeto GCP

Você está usando o projeto:

    GCP_PROJECT_ID = louro-jose-479223

Requisitos:

- Billing **ativo** no projeto  
- APIs relevantes habilitadas:
  - Gmail API  
  - Cloud Storage (`storage.googleapis.com`)


### 2.3. Bucket no Cloud Storage

Bucket utilizado:

    gs://parrot-agents-dev

Esse bucket será usado para:

- Armazenar threads em `threads/`
- Armazenar estado em `state/threads_state.json`

No `.env`:

    GCS_BUCKET=parrot-agents-dev


### 2.4. Service Account (GCS)

Service account:

    parrot-agents-sa@louro-jose-479223.iam.gserviceaccount.com

Permissão no projeto (exemplo de binding):

    roles/storage.admin

Chave JSON gerada em:

    credentials/service-account.json

O código usa esse arquivo para autenticar no GCS.


### 2.5. Token do Gmail (`parrot-gmails.json`)

Na pasta `credentials/`, existe:

    credentials/parrot-gmails.json

Esse arquivo é o **token OAuth** de um usuário Gmail que:

- Já foi autorizado para:
  - ler emails
  - modificar rótulos
- Tem acesso à conta de email que você quer usar no pipeline

Esse token é usado exclusivamente para acessar o Gmail (não usa service account).


### 2.6. LLM via OpenRouter

No `.env`, configure:

    OPENROUTER_API_KEY=...          # sua chave OpenRouter
    OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
    OPENROUTER_MODEL=openai/gpt-4o  # modelo usado para classificação

O cliente `llm_client.py` chama o endpoint:

    POST https://openrouter.ai/api/v1/chat/completions

com:

- Header: `Authorization: Bearer <OPENROUTER_API_KEY>`
- Body JSON contendo o modelo e as mensagens.


## 3. Arquivos principais e responsabilidades

### 3.1. `app/core/config.py` — Configurações

Responsável por:

- Carregar variáveis do `.env`  
- Expor tudo como `settings.X`  
- Centralizar paths de credenciais

Principais campos:

- `settings.GCP_PROJECT_ID`
- `settings.GCS_BUCKET`
- `settings.GMAIL_LABEL` (padrão `"QUOTES"`)
- `settings.SHEET_ID` (ainda não usado nesta fase)
- `settings.OPENROUTER_API_KEY`, `OPENROUTER_BASE_URL`, `OPENROUTER_MODEL`
- `settings.GMAIL_TOKEN_FILE` → `credentials/parrot-gmails.json`
- `settings.SERVICE_ACCOUNT_FILE` → `credentials/service-account.json`

Esse módulo é o “cérebro de configuração” do projeto: qualquer mudança de projeto/bucket/modelo é feita aqui ou no `.env`, sem precisar mexer no resto do código.


### 3.2. `app/core/gmail_client.py` — Cliente Gmail

Este módulo encapsula toda a interação com o Gmail.

Funções principais:

1. `get_or_create_label(label_name)`

   - Verifica se o label existe no Gmail.
   - Se não existir, cria.
   - Retorna o **ID interno** do label (por exemplo, `Label_123456`).

2. `search_messages(query)`

   - Busca mensagens usando a sintaxe de pesquisa do Gmail (a mesma da barra de busca).
   - Exemplo de query usada:
     - `newer_than:50d` → pega emails dos últimos 50 dias.
   - Faz paginação para buscar todas as mensagens que batem com a query.
   - Retorna uma lista de dicionários como:
     - `{"id": "<MESSAGE_ID>"}`.

3. `get_message(message_id, fmt="full")`

   - Retorna uma mensagem completa (headers, payload com body, labels, etc.).

4. `add_label_to_message(message_id, label_id)`

   - Adiciona um label específico a uma mensagem.
   - Do ponto de vista do usuário, isso faz a thread aparecer com aquele label.

5. `list_threads_with_label(label_id)`

   - Lista threads que possuem o label informado.
   - Cada item contém ao menos `{"id": "<THREAD_ID>"}`.

6. `get_thread(thread_id)`

   - Retorna o conteúdo completo da thread, incluindo todas as mensagens:
     - `thread["messages"]` é a lista de emails daquela conversa.

7. `decode_email(message)`

   - Decodifica o body em base64 e retorna um objeto `email.message.EmailMessage`.
   - Permite extrair o texto do corpo do email (plain text).

O objetivo desse módulo é deixar o agente o mais “limpo” possível, evitando repetição de código da API do Gmail.


### 3.3. `app/core/gcs_client.py` — Cliente GCS

Responsável por:

- Conectar ao Cloud Storage usando a **service account** (`service-account.json`)  
- Salvar JSONs no bucket  
- Ler JSONs do bucket

Funções:

- `upload_json(path, data)`  
  - Serializa `data` (dict) para JSON.
  - Faz upload para `gs://<bucket>/<path>`.
  - Exemplos de paths:
    - `threads/<THREAD_ID>.json`
    - `state/threads_state.json`

- `download_json(path)`  
  - Se o arquivo existir, baixa o conteúdo e faz `json.loads(...)`.
  - Se não existir, retorna `None`.

É usado por `state.py` e pelo próprio agente para persistência no GCS.


### 3.4. `app/core/state.py` — Controle incremental de threads

Controla até onde cada thread já foi processada (qual foi o último email verificado).

Arquivo de estado é salvo em:

    gs://parrot-agents-dev/state/threads_state.json

Formato:

    {
      "THREAD_ID_1": { "last_email_id": "MSG_ID_10" },
      "THREAD_ID_2": { "last_email_id": "MSG_ID_7" }
    }

Funções:

- `get_last_email(thread_id)`  
  - Retorna o último email processado daquela thread (ou `None` se nunca processou).

- `update_thread(thread_id, last_email_id)`  
  - Atualiza o estado em memória.
  - Persiste o JSON atualizado no GCS.
  - É chamado sempre que uma thread é salva/atualizada no Storage.

Esse estado é usado na **Fase 2** para evitar reprocessar threads que não tiveram novos emails desde a última execução do agente.


### 3.5. `app/core/llm_client.py` — LLM via OpenRouter

Responsável por falar com o LLM (por exemplo, GPT-4o) via API do OpenRouter.

Função principal:

    classify_email(self, text: str) -> str

Ela:

1. Monta um prompt em português para o modelo:

       Você é um classificador de emails.

       Responda APENAS com uma das opções:

       - VIAGEM
       - NAO-VIAGEM

       Essa classificação deve identificar emails que são:
       - cotações de hotel
       - mensagens de fornecedores de hospedagem
       - respostas sobre disponibilidade, tarifas, política de cancelamento
       - temas relacionados a viagens

       Email:
       <texto do email>

2. Faz uma requisição `POST` para:

       https://openrouter.ai/api/v1/chat/completions

   usando o modelo configurado em `OPENROUTER_MODEL` (por exemplo, `openai/gpt-4o`).

3. Lê a resposta do modelo, converte para maiúsculas e normaliza:
   - Se a resposta contiver “VIAGEM”, retorna `"VIAGEM"`.
   - Caso contrário, retorna `"NAO-VIAGEM"`.

O agente usa esse resultado para decidir se aplica o label `QUOTES` ou ignora o email.


### 3.6. `app/agents/labeler_storage_agent.py` — Agente Labeler + Storage

Este é o agente que liga tudo:

- Gmail
- LLM (classificação)
- Label QUOTES
- Cloud Storage
- Estado incremental

Ele possui **duas fases** bem definidas:

#### Fase 1 — Classificação + Rotulagem (LLM → `QUOTES`)

Objetivo:  
Percorrer os emails recentes e rotular como `QUOTES` todos aqueles que o modelo entender que são de viagem/cotação/fornecedor de hospedagem.

Passos da Fase 1:

1. Garante que o label `QUOTES` existe:

       label_id = gmail_client.get_or_create_label(settings.GMAIL_LABEL)

2. Busca emails dos últimos 50 dias:

       messages = gmail_client.search_messages("newer_than:50d")

3. Para cada mensagem (`msg_id`):

   - Baixa a mensagem completa:

         email_obj = gmail_client.get_message(msg_id, fmt="full")

   - Verifica se ela já tem o label `QUOTES`:
     - Se tiver, pula a classificação (já foi classificada em execução anterior).

   - Decodifica o corpo do email em texto (`decode_email`).

   - Envia o texto para o LLM:

         classification = llm_client.classify_email(text[:8000])

   - Se `classification == "VIAGEM"`:
     - Aplica o label `QUOTES` nesse email (o que, visualmente, faz a thread aparecer com aquele label):

           gmail_client.add_label_to_message(msg_id, label_id)

   - Se `classification == "NAO-VIAGEM"`:
     - Ignora o email (não recebe label QUOTES).

Resultado da Fase 1:

- Seu Gmail passa a ter o label `QUOTES` em todas as conversas de viagem/cotação detectadas pelo LLM.
- Threads não relacionadas a viagem/cotação permanecem sem esse label.


#### Fase 2 — Threads com QUOTES → Storage + Estado

Objetivo:  
Pegar todas as threads que possuem o label `QUOTES`, salvar no Cloud Storage e marcar no estado qual foi o último email processado em cada thread.

Passos da Fase 2:

1. Lista as threads com o label `QUOTES`:

       threads = gmail_client.list_threads_with_label(label_id)

2. Para cada `thread_id`:

   - Baixa a thread completa:

         thread_full = gmail_client.get_thread(thread_id)
         messages = thread_full.get("messages", [])

   - Se não houver mensagens, pula.

   - Descobre o ID do último email da thread:

         last_email_id = messages[-1]["id"]

   - Consulta o estado atual:

         last_processed = state.get_last_email(thread_id)

   - Se `last_processed == last_email_id`:
     - Conclusão: nada novo foi adicionado à thread desde a última execução.
     - Resultado: a thread é ignorada (já está sincronizada).

   - Caso contrário:

     - Salva a thread completa no GCS:

           gcs_client.upload_json(f"threads/{thread_id}.json", thread_full)

     - Atualiza o estado com o novo `last_email_id`:

           state.update_thread(thread_id, last_email_id)

Resultado da Fase 2:

- Todas as threads de viagem (rotuladas como `QUOTES`) são salvas no bucket, cada uma como um JSON:
  - `threads/<THREAD_ID>.json`
- O estado é atualizado em:
  - `state/threads_state.json`
- Execuções futuras do agente são incrementais:
  - Threads sem novos emails não são regravadas no Storage.
  - Threads com novos emails são sincronizadas novamente.


### 3.7. `main.py` — Entry point

Arquivo simples que apenas executa o agente:

    from app.agents.labeler_storage_agent import labeler_storage_agent

    if __name__ == "__main__":
        labeler_storage_agent.run()


## 4. O que você deve ver ao executar o código

### 4.1. Comando de execução

Na raiz do projeto:

    python main.py

### 4.2. Logs esperados no terminal

Saída típica:

    🔵 Iniciando Labeler com LLM (2 fases)
    ✓ Label encontrado/criado: QUOTES (id=Label_XXXXXXX)

    📥 Fase 1: buscando emails (newer_than:50d)...
    → Encontrados 123 emails nos últimos 50 dias.

    📨 Email 18762f93a12 sendo analisado pelo LLM...
    → Classificação LLM: VIAGEM
       ✓ Label QUOTES aplicado a este email (e thread).

    📨 Email 18762f93b55 sendo analisado pelo LLM...
    → Classificação LLM: NAO-VIAGEM
       → Classificado como NAO-VIAGEM, ignorando.

    📨 Email 18762f93c09 já tem QUOTES, pulando fase de classificação.
    ...

    📂 Fase 2: sincronizando threads com QUOTES para o Storage...
    → Encontradas 15 threads com QUOTES.

    📦 Processando thread 179cc4f83a...
    [GCS] Upload OK → threads/179cc4f83a.json
    [STATE] Thread 179cc4f83a atualizada → último email 18763a8f2eb34
       ✓ Thread salva no Storage e estado atualizado.

    📦 Processando thread 890bb43fe2...
       → Nenhum email novo na thread desde o último processamento, pulando.

    🏁 Labeler finalizado com sucesso.

IDs, quantidades e textos variam, mas o fluxo geral deve seguir esse padrão.


### 4.3. O que deve aparecer no Gmail

Depois de rodar o agente:

- Emails (e suas respectivas conversas) relacionados a:
  - cotações de hotel
  - fornecedores de hospedagem
  - informações de tarifas, disponibilidade
  - políticas de cancelamento/pagamento
- …devem aparecer com o **label `QUOTES`** no Gmail.

Alguns comportamentos importantes:

- Emails já rotulados como `QUOTES` são detectados e não são reclassificados.
- Emails nos últimos 50 dias que antes não tinham label são analisados pelo LLM:
  - Se forem de viagem/cotação, passam a ter o label `QUOTES`.
  - Se não forem, permanecem sem esse label.


### 4.4. O que deve aparecer no Cloud Storage

No bucket `parrot-agents-dev`, você deve ver:

1. Pasta `threads/`:

       threads/
        ├── 179cc4f83a.json
        ├── 890bb43fe2.json
        ├── ...

   Cada arquivo JSON representa uma **thread completa** marcada com `QUOTES`.

   O conteúdo inclui:

   - `id`: id da thread
   - `messages`: lista de emails da thread
     - cada mensagem com:
       - `id`
       - `payload` (headers, corpo codificado etc.)
       - `threadId`
       - `labelIds` (incluindo `QUOTES`)

2. Pasta `state/`:

       state/
        └── threads_state.json

   Exemplo de conteúdo:

       {
         "179cc4f83a": {
           "last_email_id": "18763a8f2eb34"
         },
         "890bb43fe2": {
           "last_email_id": "18766bbf441ae"
         }
       }

   Isso mostra:

   - Quais threads já foram sincronizadas.
   - Qual foi o último email considerado em cada thread.

Execuções futuras:

- Se nenhuma nova mensagem entrar numa thread `QUOTES`, ela não será regravada.
- Se novos emails forem adicionados à thread, o agente:
  - baixará a thread atualizada,
  - salvará novamente o JSON da thread,
  - atualizará o `last_email_id` no arquivo de estado.


## 5. Próximos passos naturais

Depois deste Labeler estar funcional e validado, os próximos passos naturais do projeto são:

1. **Agente Extrator (ExtractorAgent)**

   - Ler os arquivos `threads/*.json` no GCS.
   - Para cada thread, extrair informações estruturadas de acordo com o cabeçalho:

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
         ]

   - Esse agente provavelmente também usará LLM para:
     - identificar blocos relevantes em cada email da thread,
     - extrair campos, normalizar datas, valores etc.
   - Salvar o resultado em:
     - uma aba de Google Sheets, e/ou
     - uma tabela em BigQuery, para análise posterior.

2. **Dockerizar o projeto**

   - Criar um `Dockerfile` com:
     - instalação das dependências,
     - cópia do código,
     - `ENTRYPOINT` chamando o `main.py`.
   - Enviar a imagem para o Artifact Registry.
   - Criar um **Cloud Run Job** que executa esse container.

3. **Agendar execução diária**

   - Usar o Cloud Scheduler para acionar o Cloud Run Job diariamente (por exemplo, às 07:00 BRT).
   - Todo dia, o pipeline:
     - lê emails novos dos últimos X dias,
     - classifica com LLM,
     - rotula como `QUOTES`,
     - sincroniza as threads para o Storage,
     - (e depois, com o Extrator) atualiza a base estruturada em Sheets/BigQuery.

Com isso, você terá um fluxo agente-based robusto, incremental e pronto para ser expandido com novos agentes (por exemplo, Extrator, Normalizador, Consolidator, etc.).
