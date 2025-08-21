import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from pathlib import Path
import mimetypes

# --- Configuración ---
# Define los SCOPES necesarios para Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.metadata.readonly']

# Rutas a los archivos de credenciales y token
GOOGLE_DRIVE_DIR = Path(__file__).resolve().parent
CLIENT_SECRET_FILE = next(GOOGLE_DRIVE_DIR.glob('client_secret_*.json'), None)
TOKEN_FILE = GOOGLE_DRIVE_DIR / 'token.json'

# Rutas de las carpetas locales a subir
LOCAL_BASE_PATH = Path(__file__).resolve().parent.parent # Scraper.Neoauto
LOCAL_FOLDERS_TO_UPLOAD = [
    LOCAL_BASE_PATH / 'outputs',
    LOCAL_BASE_PATH / 'model_pages'
]

# Nombre de la carpeta raíz en Google Drive donde se subirán los archivos
DRIVE_ROOT_FOLDER_NAME = "Neoauto Analysis"

# --- Funciones de Google Drive ---

def get_drive_service():
    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CLIENT_SECRET_FILE:
                raise FileNotFoundError(f"No se encontró client_secret_*.json en {GOOGLE_DRIVE_DIR}")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CLIENT_SECRET_FILE), SCOPES)
            creds = flow.run_local_server(port=0) # Esto no debería ejecutarse si token.json es válido
        with open(str(TOKEN_FILE), 'w') as token:
            token.write(creds.to_json())
    
    return build('drive', 'v3', credentials=creds)

def create_or_get_folder(service, name, parent_id=None):
    try:
        q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id: q += f" and '{parent_id}' in parents"
        
        response = service.files().list(q=q, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        
        if files: return files[0]['id']
        
        file_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id: file_metadata['parents'] = [parent_id]
        
        folder = service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')
    except HttpError as error:
        print(f"Ocurrió un error al crear/obtener la carpeta: {error}")
        return None

def upload_file(service, file_path, folder_id):
    try:
        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type is None: mime_type = 'application/octet-stream'

        file_metadata = {'name': file_path.name, 'parents': [folder_id]}
        media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
        
        # Buscar si el archivo ya existe en la carpeta de destino
        q = f"name='{file_path.name}' and '{folder_id}' in parents and trashed=false"
        response = service.files().list(q=q, spaces='drive', fields='files(id)').execute()
        existing_files = response.get('files', [])

        if existing_files: # Actualizar archivo existente
            file_id = existing_files[0]['id']
            service.files().update(fileId=file_id, media_body=media, fields='id').execute()
            print(f"Archivo actualizado: {file_path.name}")
        else: # Subir nuevo archivo
            service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f"Archivo subido: {file_path.name}")
        return True
    except HttpError as error:
        print(f"Ocurrió un error al subir el archivo {file_path.name}: {error}")
        return False

def upload_folder_recursive(service, local_folder_path, parent_drive_id):
    print(f"Subiendo carpeta: {local_folder_path.name} a Drive...")
    drive_folder_id = create_or_get_folder(service, local_folder_path.name, parent_drive_id)
    if not drive_folder_id: return False

    for item in local_folder_path.iterdir():
        if item.is_file():
            upload_file(service, item, drive_folder_id)
        elif item.is_dir():
            upload_folder_recursive(service, item, drive_folder_id)
    return True

def get_shareable_link(service, file_id):
    try:
        # Asegurarse de que el archivo sea público (lectura para cualquiera con el enlace)
        # Primero, verificar si ya tiene el permiso 'anyoneWithLink'
        permissions = service.permissions().list(fileId=file_id, fields='permissions(id, role, type)').execute()
        has_public_permission = False
        for p in permissions.get('permissions', []):
            if p['type'] == 'anyone' and p['role'] == 'reader':
                has_public_permission = True
                break
        
        if not has_public_permission:
            permission_body = {'type': 'anyone', 'role': 'reader'}
            service.permissions().create(fileId=file_id, body=permission_body, fields='id').execute()
            print(f"Permisos de lectura pública añadidos para el archivo {file_id}")

        # Obtener el enlace compartible
        file = service.files().get(fileId=file_id, fields='webViewLink').execute()
        return file.get('webViewLink')
    except HttpError as error:
        print(f"Ocurrió un error al obtener el enlace compartible para {file_id}: {error}")
        return None

def main_upload_logic():
    try:
        service = get_drive_service()
        
        # Crear o obtener la carpeta raíz principal en Google Drive
        root_drive_folder_id = create_or_get_folder(service, DRIVE_ROOT_FOLDER_NAME)
        if not root_drive_folder_id: 
            print("No se pudo crear/obtener la carpeta raíz en Google Drive.")
            return None

        # Subir las carpetas locales
        for local_folder in LOCAL_FOLDERS_TO_UPLOAD:
            if local_folder.exists():
                upload_folder_recursive(service, local_folder, root_drive_folder_id)
            else:
                print(f"Advertencia: La carpeta local {local_folder} no existe y no será subida.")
        
        # Obtener el enlace de index.semanal.html
        index_html_path = LOCAL_BASE_PATH / 'outputs' / 'index.semanal.html'
        if index_html_path.exists():
            # Primero, encontrar el ID del archivo index.semanal.html en Drive
            # Asumimos que está dentro de la carpeta 'outputs' que está dentro de DRIVE_ROOT_FOLDER_NAME
            outputs_drive_folder_id = create_or_get_folder(service, 'outputs', root_drive_folder_id) # Obtener ID de la carpeta 'outputs' en Drive
            if outputs_drive_folder_id:
                q = f"name='index.semanal.html' and '{outputs_drive_folder_id}' in parents and trashed=false"
                response = service.files().list(q=q, spaces='drive', fields='files(id)').execute()
                files = response.get('files', [])
                if files:
                    index_html_drive_id = files[0]['id']
                    shareable_link = get_shareable_link(service, index_html_drive_id)
                    print(f"Enlace compartible para index.semanal.html: {shareable_link}")
                    return shareable_link
                else:
                    print("index.semanal.html no encontrado en Google Drive después de la subida.")
            else:
                print("No se pudo encontrar la carpeta 'outputs' en Google Drive.")
        else:
            print(f"Advertencia: {index_html_path} no existe localmente. No se generará enlace.")
        
        return None

    except HttpError as error:
        print(f"Ocurrió un error de HTTP en el proceso de subida a Drive: {error}")
        return None
    except Exception as e:
        print(f"Ocurrió un error inesperado en el proceso de subida a Drive: {e}")
        return None

if __name__ == '__main__':
    print("Iniciando drive_uploader.py directamente...")
    link = main_upload_logic()
    if link: print(f"Proceso completado. Enlace principal: {link}")
    else: print("Proceso de subida a Drive fallido o enlace no generado.")
