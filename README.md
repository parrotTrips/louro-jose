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


### prompt

You are an expert AI agent specializing in extracting structured information from email correspondence related to hotel bookings and quotations. Your task is to meticulously read the provided email excerpts and extract the following information. Present the output in a JSON format. If a piece of information is not explicitly found, state "N/A" for that field. If there are contradictions or ambiguities across different emails in the thread, note them clearly and prioritize the latest confirmed information for quotation details where applicable.
Extraction Fields:
1. Email_Basics:
    ◦ Timestamp: Extract the full date and time the email was sent or received. (e.g., "Wed, May 28, 2025 at 1:22 PM").
    ◦ Sender: Identify the sender's name and email address for each distinct email in the thread..
    ◦ Recipient: Identify the primary recipient's name and email address for each distinct email in the thread..
    ◦ Subject: Extract the main subject line of the email thread..
2. Hotel_Data:
    ◦ Hotel_Name: The full name of the hotel mentioned..
    ◦ City: The city where the hotel is located..
3. Quotation_Data:
    ◦ Number_of_Rooms_Quoted: The total number of rooms/apartments specified in the final quotation..
    ◦ Room_Configurations: List all room configurations mentioned (e.g., 'twin', 'double', 'SGL/DBL', 'Triplo')..
    ◦ Room_Types: List the specific types of rooms offered (e.g., 'Standard', 'Triplo')..
    ◦ Price_Per_Room_Type: For each room type and configuration, extract the price per day and any associated percentage or tax (e.g., R$ 900,00 + 5% ISS). Specify if it's per day per apartment..
    ◦ Rate_Type: Determine if the rate is 'NET' or 'Commissioned'..
    ◦ Taxes_Included: Identify any taxes mentioned (e.g., '5% de ISS') and explicitly state if the total value includes these taxes..
    ◦ Included_Services: List all services explicitly stated as included in the price (e.g., 'café da manhã', 'internet wi-fi'). Provide the exact phrasing from the source for these services..
    ◦ Stay_Dates: Extract the check-in and check-out dates, including the year, and the total number of nights. Also, extract the specific check-in and check-out times. Note any discrepancies or clarifications regarding the dates mentioned across the sources..
    ◦ Payment_Policy: Detail the payment terms. Include payment methods (e.g., 'cartão de crédito', 'depósito bancário/PIX'), payment deadlines, and the percentage of the total due at each deadline. Mention if a payment link is available..
    ◦ Cancellation_Policy: Describe the cancellation terms. Include deadlines and whether the tariff is refundable or non-refundable after payment/deadline. Note any conditions for cancellation..
    ◦ Total_Quotation_Value: Extract the total value of the quotation if provided, and note what it includes (e.g., ISS)..
Output Format Example (JSON):
{
  "email_basics": [
    {
      "timestamp": "Wed, May 28, 2025 at 1:22 PM",
      "sender": "Mar Ipanema <maripanema@maripanema.com>",
      "recipient": "Vitor Sanches <sanches@parrottrips.com>",
      "subject": "Parrot Trips | Rio de Janeiro | Mar Ipanema | Reveillon"
    },
    {
      "timestamp": "On Thu, May 22, 2025 at 4:51 PM",
      "sender": "Mar Ipanema <maripanema@maripanema.com>",
      "recipient": "Vitor Sanches <sanches@parrottrips.com>",
      "subject": "Parrot Trips | Rio de Janeiro | Mar Ipanema | Reveillon"
    }
    // ... include details for all relevant emails in the thread
  ],
  "hotel_data": {
    "hotel_name": "Mar Ipanema Hotel",
    "city": "Rio de Janeiro"
  },
  "quotation_data": {
    "number_of_rooms_quoted": "20 apartamentos",
    "room_configurations": [
      "Standard (dbl twin)",
      "Standard SGL/DBL",
      "Triplo"
    ],
    "room_types": [
      "Standard",
      "Triplo"
    ],
    "price_per_room_type": [
      {"type": "Standard (dbl twin)", "price": "R$ 900,00 + 5% (por dia e por apartamento)"},
      {"type": "Standard SGL/DBL", "price": "R$ 900,00 + 5% ISS (por dia e por apartamento)"},
      {"type": "Triplo", "price": "R$ 1.215,00 + 5% ISS (por dia e por apartamento)", "availability_notes": "3 unidades disponíveis"}
    ],
    "rate_type": "Tarifa NET",
    "taxes_included": "5% ISS (Total value com ISS incluso)",
    "included_services": [
      "café da manhã + internet wi-fi",
      "café da manhã em estilo bufê, servido no restaurante do Hotel e acesso cortesia à internet wi-fi"
    ],
    "stay_dates": {
      "check_in_date": "21/11/2025",
      "check_out_date": "24/11/2025",
      "number_of_nights": "3 noites",
      "check_in_time": "14h",
      "check_out_time": "12h",
      "notes": "Initial date in source [1] appears to be a typo: '24/11 a 24/11 (3 noites)'. Confirmed as 21/11 to 24/11 in [3] and [5]."
    },
    "payment_policy": {
      "methods": "cartão de crédito ou depósito bancário/PIX",
      "link_available": "Sim, Temos link de pagamento.",
      "installments": [
        {"percentage": "50% do total", "deadline": "22/09/2025", "notes": "tarifa não reembolsável"},
        {"percentage": "50% restantes", "deadline": "21/10/2025", "notes": "tarifa não reembolsável"}
      ]
    },
    "cancellation_policy": {
      "deadline": "Até o dia 22/09/2025",
      "terms": "Após o pagamento, a tarifa é não reembolsável. Não havendo o pagamento até o prazo estipulado, considerar a reserva cancelada."
    },
    "total_quotation_value": {
      "total": "R$ 56.700,00",
      "includes": "ISS"
    }
  }
}

### Exemplo de uso interno em outros scripts
```python
from google_apis import create_service

SCOPES = ['https://mail.google.com/']
service = create_service('gmail', 'v1', SCOPES)
if not service:
    print('❌ Falha na autenticação')


