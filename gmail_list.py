from google_apis import create_service

def main():
    SERVICE_NAME = 'gmail'
    VERSION      = 'v1'
    SCOPES       = ['https://mail.google.com/']

    service = create_service(SERVICE_NAME, VERSION, SCOPES)
    if not service:
        print('❌ Não foi possível autenticar no Gmail.')
        return

    resp = service.users().labels().list(userId='me').execute()
    labels = resp.get('labels', [])
    print('📬 Rótulos encontrados:')
    for l in labels:
        print(' -', l['name'])

if __name__ == '__main__':
    main()
