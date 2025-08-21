"""
Servicio de Envío de Correos Electrónicos con Gmail.

Este script se encarga de la última etapa del pipeline: la distribución de los
reportes generados. Utiliza la API de Gmail para enviar correos electrónicos
de forma automatizada.

Funcionalidad Principal:
1.  **Autenticación Segura con Google (OAuth 2.0)**:
    - Utiliza el flujo de OAuth 2.0 para obtener permisos de manera segura.
    - Busca un archivo `token.json` con credenciales válidas. Si no existe o
      ha expirado, utiliza `credentials.json` para guiar al usuario a través
      de un flujo de autenticación en el navegador la primera vez.
    - Guarda el token de acceso y de refresco en `token.json` para futuras
      ejecuciones sin necesidad de intervención manual.

2.  **Lectura de Configuración**: 
    - Lee el archivo `destinatarios.txt` para obtener una lista de correos
      electrónicos y los asuntos correspondientes para cada destinatario.
    - Construye dinámicamente la ruta al archivo de reporte HTML generado por
      `main.py`, basándose en la fecha actual (p. ej., 
      `gmail_attractive_leads_2025-08-10.html`).

3.  **Construcción y Envío de Correos**:
    - Lee el contenido del reporte HTML optimizado para Gmail.
    - Crea un mensaje de correo electrónico en formato MIME, estableciendo el
      destinatario, remitente, asunto y el cuerpo HTML.
    - Utiliza el servicio de la API de Gmail para enviar el mensaje.

4.  **Gestión de Envíos y Limpieza**:
    - Tras enviar un correo, utiliza la API para mover inmediatamente el mensaje
      enviado a la papelera. Esto mantiene la bandeja de "Enviados" del usuario
      limpia, útil para no saturarla con correos automáticos.

5.  **Control por Argumentos de Línea de Comandos**:
    - `--enviar-correos`: Activa el envío real. Sin este flag, el script se
      ejecuta en modo de prueba (test mode) sin enviar nada.
    - `--produccion`: Un flag para indicar que la ejecución es en un entorno de
      producción (aunque su lógica específica puede variar).
    - `--ciclos` y `--retardo-segundos`: Permiten enviar el mismo reporte
      múltiples veces con una pausa entre cada envío.

El script también mantiene un registro de sus operaciones en `envio_reporte.log`.
"""
import os
import datetime
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import argparse
import time
from pathlib import Path

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    print("ERROR: Faltan librerías de Google.")
    print("Instala las librerías con: pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib")
    exit()

# --- CONFIGURACIÓN ---
SCOPES = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.modify'
]
RECIPIENT_EMAILS_FILE = 'destinatarios.txt'
LOG_FILE = 'envio_reporte.log'

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def autenticar_google():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists('credentials.json'):
                logger.error("CRITICAL ERROR: 'credentials.json' not found.")
                return None
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def leer_cuerpo_html(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"ERROR: HTML file not found: '{filename}'.")
        return None
    except Exception as e:
        logger.error(f"ERROR reading HTML file '{filename}': {e}")
        return None

def leer_destinatarios_y_asuntos(filename):
    destinatarios = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip() and ',' in line:
                    email, subject = line.split(',', 1)
                    destinatarios.append({'email': email.strip(), 'subject': subject.strip()})
        return destinatarios
    except Exception as e:
        logger.error(f"ERROR reading recipients file '{filename}': {e}")
        return None

def crear_mensaje_email(destinatario_email, asunto, cuerpo_html, drive_link=None): # MODIFIED SIGNATURE
    mensaje = MIMEMultipart()
    mensaje['to'] = destinatario_email
    mensaje['from'] = 'me'
    mensaje['subject'] = asunto
    
    full_html_content = cuerpo_html
    if drive_link:
        # Add a prominent link at the top of the email body
        drive_link_html = f'<p style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">Puedes acceder al análisis completo en Google Drive aquí: <a href="{drive_link}" style="color: #1a73e8; text-decoration: none; font-weight: bold;">Acceder al Dashboard en Google Drive</a></p><br>'
        full_html_content = drive_link_html + cuerpo_html # Prepend the link
        
    mensaje.attach(MIMEText(full_html_content, 'html')) # Use full_html_content
    return {'raw': base64.urlsafe_b64encode(mensaje.as_bytes()).decode()}

def mover_correo_a_papelera(service_gmail, message_id):
    try:
        service_gmail.users().messages().trash(userId='me', id=message_id).execute()
        logger.info(f"Message ID '{message_id}' moved to trash.")
    except Exception as e:
        logger.error(f"Unexpected error while moving email '{message_id}' to trash: {e}")

def main():
    parser = argparse.ArgumentParser(description="Script to send email reports.")
    parser.add_argument('--enviar-correos', action='store_true', help='Activates email sending.')
    parser.add_argument('--produccion', action='store_true', help='Indicates a production run.')
    parser.add_argument('--ciclos', type=int, default=1, help='Number of sending cycles.')
    parser.add_argument('--retardo-segundos', type=int, default=0, help='Delay in seconds between cycles.')
    parser.add_argument('--drive-link', type=str, help='Google Drive link to include in the email.', default=None) # NEW
    args = parser.parse_args()

    logger.info("--- INITIATING EMAIL SENDER SCRIPT ---")

    if not args.enviar_correos:
        logger.info("Test mode: No emails will be sent. Use '--enviar-correos' to activate.")
        return

    logger.info("[Step 1/4] Authenticating with Google...")
    creds = autenticar_google()
    if not creds: logger.critical("PROCESS FAILED: AUTHENTICATION"); return

    logger.info("[Step 2/4] Building Gmail service...")
    try:
        service_gmail = build('gmail', 'v1', credentials=creds)
    except Exception as e:
        logger.critical(f"Error building Gmail service: {e}"); return

    logger.info("[Step 3/4] Finding today's report file...")
    try:
        script_dir = Path(__file__).parent
        outputs_dir = script_dir.parent / "outputs"
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        html_file_path = outputs_dir / f"gmail_attractive_leads_{today_str}.html"
        html_filename = html_file_path.name
        logger.info(f"Report file to send: {html_file_path}")
    except Exception as e:
        logger.critical(f"Could not construct path to HTML file: {e}")
        return

    cuerpo_html = leer_cuerpo_html(html_file_path)
    if not cuerpo_html: logger.critical(f"PROCESS STOPPED: Could not read email body from {html_file_path}."); return

    logger.info(f"[Step 4/4] Reading recipients from '{RECIPIENT_EMAILS_FILE}'...")
    destinatarios_info = leer_destinatarios_y_asuntos(RECIPIENT_EMAILS_FILE)
    if not destinatarios_info: logger.critical("PROCESS STOPPED: No recipients found."); return

    new_subject = f"Lista de Leads Calientes {html_filename}"
    logger.info(f"--- STARTING EMAIL SEND (Cycles: {args.ciclos}, Delay: {args.retardo_segundos}s) ---")
    for i in range(args.ciclos):
        logger.info(f"--- Starting send cycle {i + 1} of {args.ciclos} ---")
        for dest_info in destinatarios_info:
            logger.info(f"Preparing email for: {dest_info['email']}")
            mensaje_final = crear_mensaje_email(dest_info['email'], new_subject, cuerpo_html, args.drive_link) # MODIFIED
            try:
                sent_message = service_gmail.users().messages().send(userId='me', body=mensaje_final).execute()
                logger.info(f"  SUCCESS. Email sent. Message ID: {sent_message['id']}")
                mover_correo_a_papelera(service_gmail, sent_message['id'])
            except Exception as error:
                logger.error(f"  ERROR sending email to {dest_info['email']}: {error}")
        logger.info(f"--- Send cycle {i + 1} finished ---")
        if i < args.ciclos - 1 and args.retardo_segundos > 0:
            logger.info(f"Waiting {args.retardo_segundos} seconds...")
            time.sleep(args.retardo_segundos)

    logger.info("--- EMAIL SENDING PROCESS FINISHED ---")

if __name__ == '__main__':
    main()
