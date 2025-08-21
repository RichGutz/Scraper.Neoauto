import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pathlib import Path

# Define los SCOPES necesarios para Google Drive
# drive.file: Acceso a archivos creados o abiertos por la app
# drive.appdata: Acceso a la carpeta de datos de la app
# drive.metadata.readonly: Acceso a metadatos de archivos (solo lectura)
# drive: Acceso completo a Google Drive (más amplio, úsalo con precaución)
SCOPES = ['https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.metadata.readonly']

# Ruta al directorio donde se guardarán las credenciales
GOOGLE_DRIVE_DIR = Path(__file__).resolve().parent

# Ruta al archivo de credenciales descargado de Google Cloud Console
# Asegúrate de que el nombre del archivo coincida con el que descargaste
CLIENT_SECRET_FILE = next(GOOGLE_DRIVE_DIR.glob('client_secret_*.json'), None)

if not CLIENT_SECRET_FILE:
    print(f"Error: No se encontró el archivo client_secret_*.json en {GOOGLE_DRIVE_DIR}")
    print("Por favor, descarga tus credenciales de Google Cloud Console y colócalas en esta carpeta.")
    exit()

TOKEN_FILE = GOOGLE_DRIVE_DIR / 'token.json'

def authorize_google_drive():
    creds = None
    # El archivo token.json almacena los tokens de acceso y actualización del usuario.
    # Se crea automáticamente la primera vez que se completa el flujo de autorización.
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    
    # Si no hay credenciales (válidas) disponibles, permite que el usuario inicie sesión.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CLIENT_SECRET_FILE), SCOPES)
            creds = flow.run_local_server(port=0)
        # Guarda las credenciales para la próxima ejecución
        with open(str(TOKEN_FILE), 'w') as token:
            token.write(creds.to_json())
    
    print(f"Autorización completada. El archivo token.json ha sido creado/actualizado en {TOKEN_FILE}")
    return creds

if __name__ == '__main__':
    authorize_google_drive()
