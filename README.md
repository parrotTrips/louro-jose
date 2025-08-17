Gmail Threads Dumper — Documentação do Projeto

==============================================

Este repositório implementa um MVP simples e modular para:
1. Autenticar no Gmail via OAuth;
2. Listar rótulos (labels) da caixa de entrada (teste rápido);
3. Buscar mensagens por rótulo e/ou consulta (--q), agrupar por thread e salvar 1 JSON por thread em raw_messages/.
4. Extrair informações dessas mensagens com uma LLM e enquadrar nos campos do cabeçalho da aba quotes;
5. Persistir os dados extraídos na planilha do Google Sheets indicada no .env.

-------------------------------------------------------------------------------
1) Estrutura de Pastas e Papéis de Cada Arquivo
-------------------------------------------------------------------------------

.
├── credentials/
│   └── real-credentials-parrots-gmail.json
│      → Credenciais OAuth do Google obtidas no Google Cloud Console (Client ID/Secret).
│        Este arquivo é usado apenas localmente para iniciar o fluxo de autorização.
│
├── raw_messages/
│   → Pasta onde serão salvos os JSONs resultantes (um arquivo por thread do Gmail).
│
├── outputs/
│   → (Novo) Artefatos das fases de extração e persistência:
│      - quotes_extracted.jsonl  (saída da extração via LLM, 1 JSON por linha)
│      - quotes_extracted.csv    (opcional, se decidir gerar CSV)
│
├── token_files/
│   → Pasta onde ficará o token de acesso/refresh gerado após o primeiro login (OAuth).
│     O arquivo padrão é `token_gmail_v1.json`. Se apagado, o login será solicitado novamente.
│
├── utils/
│   ├── __init__.py
│   │  → Arquivo vazio para tornar `utils` um pacote Python importável.
│   │
│   ├── mime.py
│   │  → Funções utilitárias para lidar com MIME:
│   │     - `get_header(...)`: obtém um header específico (ex.: From, To, Subject).
│   │     - `extract_prefer_plaintext(...)`: extrai o corpo preferindo `text/plain`; se não existir,
│   │       converte `text/html` em texto legível (remove scripts/styles e normaliza quebras de linha).
│   │     - Helpers para decodificar Base64URL e percorrer partes MIME recursivamente.
│   │
│   ├── gmail_query.py
│   │  → Funções para conversar com a Gmail API em alto nível:
│   │     - `find_label_id(...)`: resolve o ID de um rótulo pelo nome (ex.: "COMPLETE_DATA").
│   │     - `list_messages(...)`: lista mensagens respeitando rótulos, query e paginação.
│   │     - `get_thread(...)`: busca o conteúdo completo de uma thread (todas as mensagens).
│   │     - `simplify_message(...)`: reduz cada mensagem para um dicionário padrão:
│   │       { timestamp (ISO São Paulo), sender, recipient, subject, body }
│   │     - `build_gmail_query(...)`: compõe a string de busca (q/after/before).
│   │     - `unique_thread_ids(...)`: deduplica mensagens por thread preservando ordem.
│   │
│   ├── headers.py
│   │  → (Novo) Lista **única e ordenada** dos 15 campos do cabeçalho da aba `quotes`.
│   │
│   ├── prompt.py
│   │  → (Novo) Instruções do sistema (`SYSTEM_INSTRUCTIONS`) e gerador de prompt
│   │     (`build_user_prompt(...)`) para a LLM.
│   │
│   ├── text_clean.py
│   │  → (Novo) Limpeza do corpo do e-mail (remove “forwarded”, cabeçalhos repetidos, links/assinaturas).
│   │
│   ├── io_email.py
│   │  → (Novo) Carrega os JSONs de `raw_messages/` (formato com `emails[0]` ou “flat”)
│   │     e infere timestamp a partir do nome do arquivo quando necessário.
│   │
│   └── json_utils.py
│      → (Novo) Utilitários genéricos: `force_json_object(...)`, `blank_row(...)`, `ensure_dir(...)`.
│
├── login_gmail.py
│  → Responsável pela autenticação (OAuth) e criação do cliente Gmail:
│    - Usa `credentials/real-credentials-parrots-gmail.json` e salva/renova token em `token_files/token_gmail_v1.json`.
│    - Escopo padrão: `https://www.googleapis.com/auth/gmail.readonly`.
│
├── list_labels.py
│  → Script de verificação rápida:
│    - Realiza login e imprime todos os rótulos disponíveis da conta (para validar acesso).
│
├── dump_threads.py
│  → Script principal de coleta:
│    - Parâmetros:
│      --label "NOME_DO_ROTULO"   (ex.: COMPLETE_DATA)  [opcional]
│      --q     "consulta gmail"   (ex.: from:foo@bar.com has:attachment)  [opcional]
│      --after YYYY/MM/DD         (ex.: 2025/08/01)  [opcional]
│      --before YYYY/MM/DD        (ex.: 2025/08/13)  [opcional]
│      --max   500                (quantidade máx. de mensagens a varrer; não de threads)
│    - Faz a busca, agrupa por thread e salva 1 arquivo JSON por thread em `raw_messages/`.
│    - Converte HTML para texto quando não houver `text/plain`.
│
├── llm_extract_quotes.py
│  → (Novo) **Fase 1** — Extração via LLM:
│    - Lê `raw_messages/*.json`, limpa o corpo e chama o Gemini (API Key no `.env`).
│    - Enquadra os dados **exatamente** nos 15 campos da aba `quotes`.
│    - Salva `outputs/quotes_extracted.jsonl` (um objeto JSON por linha).
│
└── save_quotes_to_csv.py
   → (Novo) **Fase 2** — Persistência:
     - Lê `outputs/quotes_extracted.jsonl` e **faz append na aba `quotes`** da planilha
       indicada por `SHEET_ID` (no `.env`) usando `utils/login_sheets`.
     - (Opcional) pode ser adaptado para também gerar `outputs/quotes_extracted.csv`.



-------------------------------------------------------------------------------
2) Pré-requisitos
-------------------------------------------------------------------------------

- Ter o arquivo de credenciais OAuth do Google salvo em:
  `credentials/real-credentials-parrots-gmail.json`
- Ter credentials/sheets-parrots.json
- Ter um GEMINI_API_KEY no arquivo dotenv

Variáveis de ambiente:

# LLM (Gemini)
GEMINI_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxx
GEMINI_MODEL_NAME=gemini-1.5-flash   # opcional (pode usar gemini-1.5-pro)

# Google Sheets
SHEET_ID=1AbCDeFGhiJKlmnOPqRS_tuvWxYZ1234567890


-------------------------------------------------------------------------------
3) Instalação (primeira vez)
-------------------------------------------------------------------------------

1. Crie e ative o ambiente virtual:
   - macOS/Linux:
     ```
     python3 -m venv env
     source env/bin/activate
     ```
   - Windows (PowerShell):
     ```
     py -m venv env
     .\env\Scripts\Activate.ps1
     ```

2. Instale as dependências:
 ```
 pip install -r requirements.txt
 ```

3. Primeiro teste: listar rótulos (labels)
------------------------------------------
$ python3 list_labels.py

O que acontece:
- Na primeira execução, abre-se uma janela do navegador para você autorizar o acesso somente-leitura ao Gmail (escopo: https://www.googleapis.com/auth/gmail.readonly).
- Ao autorizar, um token é salvo em: token_files/token_gmail_v1.json.
- Nas próximas execuções, o token é reutilizado e renovado automaticamente sem pedir login.
- A saída esperada é uma lista de rótulos, por exemplo:
  📬 Rótulos encontrados:
   - INBOX
   - SENT
   - COMPLETE_DATA
   - ...

Se quiser forçar um novo login (ou trocar de conta), apague o arquivo:
  token_files/token_gmail_v1.json
e rode novamente o list_labels.py.

3) Coleta: salvar 1 JSON por thread (dump)
------------------------------------------
Exemplo por rótulo + janela de datas:
$ python3 dump_threads.py --label COMPLETE_DATA --after 2025/08/01 --before 2025/08/13 --max 200

Exemplo por consulta livre (sem rótulo):
$ python3 dump_threads.py --q "from:alguem@empresa.com subject:cotação" --max 100

Exemplo combinando rótulo e consulta:
$ python3 dump_threads.py --label COMPLETE_DATA --q "from:alguem@empresa.com" --max 200

O que acontece:
- O script monta a busca usando os parâmetros fornecidos:
  • --label: restringe a mensagens com o rótulo informado (ex.: COMPLETE_DATA).
  • --q: passa a consulta conforme a sintaxe de busca do Gmail (ex.: from:, to:, subject:, has:attachment, etc.).
  • --after e --before: filtros de data no formato YYYY/MM/DD (padrão do Gmail).
    - Regra prática: after:D/ M/ A significa “mais recentes que essa data” (exclusivo).
      before:D/ M/ A significa “mais antigas que essa data” (exclusivo).
    - Ex.: after:2025/08/01 AND before:2025/08/13 cobre aproximadamente 2025-08-01 até 2025-08-12.
  • --max: limita a quantidade de MENSAGENS escaneadas na busca (não é o número final de threads).
- As mensagens encontradas são agrupadas por threadId.
- Para cada thread:
  • Baixa-se o conteúdo completo da thread (todas as mensagens).
  • Cada mensagem é simplificada para {timestamp, sender, recipient, subject, body}.
  • O corpo (body) prioriza text/plain; se indisponível, converte-se text/html para texto limpo.
  • As mensagens são ordenadas cronologicamente.
  • Gera-se um arquivo JSON por thread em raw_messages/.

4. Onde ver os resultados
-------------------------
- Os arquivos são gravados em: raw_messages/
- Nome do arquivo:
  YYYYMMDD_HHMM__Nome_Email__Assunto.json
  • YYYYMMDD_HHMM vem do timestamp da primeira mensagem da thread (timezone São Paulo).
  • Nome_Email é baseado no header “From”.
  • Assunto é sanitizado para formar um nome de arquivo seguro.
- Exemplo para inspecionar rapidamente:
  $ ls -1 raw_messages | head
  $ cat raw_messages/20250808_1241__Fulano_fulano@exemplo.com__Assunto.json

5. Fluxo geral (visão resumida)
-------------------------------
- login_gmail.py: faz OAuth; cria/renova token; retorna o cliente Gmail autenticado.
- list_labels.py: sanity check — mostra os rótulos disponíveis.
- dump_threads.py:
  1) (Opcional) resolve o ID do rótulo informado.
  2) Monta a query (q/after/before) para a Gmail API.
  3) Lista mensagens (até --max), agrupa por threadId.
  4) Para cada thread, busca conteúdo completo, simplifica mensagens e salva 1 JSON em raw_messages/.
- utils/mime.py: lida com MIME, headers e conversão HTML→texto.
- utils/gmail_query.py: utilitários para busca, threads e normalização de mensagens.

6. Exemplos úteis de consultas (parâmetro --q)
----------------------------------------------
- Por remetente:
  --q "from:alguem@empresa.com"
- Por assunto contendo palavras:
  --q "subject:cotação"
- E-mails com anexos:
  --q "has:attachment"
- Múltiplas condições:
  --q "from:alguem@empresa.com subject:paraty has:attachment"

7. Fase 1 — Extração via LLM (Gemini)
-------------------------------------
Execução:
`$ python3 llm_extract_quotes.py`

O que acontece:

- Para cada arquivo em raw_messages/, o corpo é higienizado (remoção de “Forwarded message”, cabeçalhos repetidos, links/assinaturas).
- O modelo Gemini recebe metadados (timestamp, destinatário, assunto, remetente) e o corpo limpo.
- A LLM enquadra as informações exatamente nos 15 campos da aba quotes, retornando um JSON por e-mail.
- O script salva um JSON por linha em outputs/quotes_extracted.jsonl.
- Observações:
- Quando alguma informação não existe no e-mail, é gravada como "" (string vazia).
- O script reforça timestamp/destinatário/assunto a partir dos metadados caso a LLM deixe em branco.

8. Fase 2 — Persistência na Planilha (aba quotes)
-------------------------------------------------
Execução:
`$ python3 save_quotes_to_csv.py`

O que acontece:

- Lê outputs/quotes_extracted.jsonl.
- Confere o cabeçalho atual da aba quotes (somente avisa se estiver diferente).
- Converte cada objeto JSON em uma linha na ordem do cabeçalho e faz append na aba quotes.
- Possíveis avisos/erros:
- Cabeçalho diferente: o script apenas alerta e continua o append.
- 403/permiso: compartilhe a planilha com o e-mail da service account.
- SHEET_ID vazio: defina no .env.