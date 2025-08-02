# google_apis.py
import os
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

def create_service(api_name, api_version, scopes, prefix=''):
    working_dir = os.getcwd()
    CLIENT_SECRET_FILE = os.path.join(working_dir, 'credentials', 'credentials-parrots-gmail.json')
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = scopes

    creds = None
    token_dir = 'token_files'
    os.makedirs(os.path.join(working_dir, token_dir), exist_ok=True)
    token_file = f'token_{API_SERVICE_NAME}_{API_VERSION}{prefix}.json'
    token_path = os.path.join(working_dir, token_dir, token_file)

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=creds, static_discovery=False)
        print(f'{API_SERVICE_NAME} {API_VERSION} service created successfully')
        return service
    except Exception as e:
        print(e)
        print(f'Failed to create service instance for {API_SERVICE_NAME}')
        if os.path.exists(token_path):
            os.remove(token_path)
        return None
