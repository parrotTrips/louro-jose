# Documentação dos scripts

## 1. google_apis.py

Módulo genérico para autenticar e criar instâncias de clientes das APIs do Google usando OAuth 2.0.

### Dependências
Instalação:
    pip install -r requirements.txt

### Organização de arquivos
- credentials/credentials-parrots-gmail.json  
  → JSON de credenciais OAuth 2.0 obtido no Google Cloud Console  
- token_files/  
  → Pasta onde serão armazenados tokens de acesso atualizados automaticamente  

### Função create_service(api_name, api_version, scopes, prefix='')
1. Define:
   - CLIENT_SECRET_FILE  
     Caminho para o arquivo de credenciais (dentro de `credentials/`)
   - token_file  
     Nome padrão: `token_<API>_<VERSÃO><prefix>.json`  
   - token_path  
     Caminho completo dentro de `token_files/`

2. Carregamento de credenciais:
   - Se `token_path` existir, carrega credenciais via `Credentials.from_authorized_user_file()`.  
   - Se não houver credenciais válidas ou elas estiverem expiradas:
     - Se existir `refresh_token`, faz `creds.refresh(Request())`.  
     - Caso contrário, inicia fluxo OAuth local com  
       `InstalledAppFlow.from_client_secrets_file(...).run_local_server()`.

3. Salvamento de credenciais:
   - Grava JSON de `creds` em `token_path` para usos futuros.

4. Criação do serviço:
   - Chama `build(api_name, api_version, credentials=creds, static_discovery=False)`.  
   - Em caso de sucesso, imprime  
     `"<API> <VERSÃO> service created successfully"` e retorna o objeto `service`.  
   - Se falhar, remove `token_path` (para evitar credenciais corrompidas) e retorna `None`.

### Exemplo de uso interno em outros scripts
```python
from google_apis import create_service

SCOPES = ['https://mail.google.com/']
service = create_service('gmail', 'v1', SCOPES)
if not service:
    print('❌ Falha na autenticação')
