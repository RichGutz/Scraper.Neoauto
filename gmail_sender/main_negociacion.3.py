import os
import datetime
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import argparse
import time

# Intenta importar las librerías de Google
try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    print("ERROR: Faltan librerías de Google.")
    print("Por favor, instala las librerías ejecutando el siguiente comando en tu terminal:")
    print("pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib argparse")
    exit()

# --- CONFIGURACIÓN ---
# Alcances para los permisos de Google.
SCOPES = [
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.modify'  # Añadido para mover a papelera
]

# Nombres de los archivos externos
EMAIL_OFFER_BODY_FILE = 'mi_oferta.txt'  # Archivo con el texto de la oferta (contenido HTML)
RECIPIENT_EMAILS_FILE = 'destinatarios.txt'  # Archivo con la lista de correos y asuntos

# --- FIN DE LA CONFIGURACIÓN ---

# --- Configuración del LOG ---
LOG_FILE = 'negociacion_deudas.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,  # Nivel de detalle: INFO, DEBUG, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def autenticar_google():
    """Maneja la autenticación de usuario con las APIs de Google."""
    creds = None
    logger.info("--> Buscando 'token.json'...")
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        logger.info("--> 'token.json' encontrado.")

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("--> El token ha expirado. Refrescando...")
            creds.refresh(Request())
        else:
            logger.info("--> Se necesita autenticación por primera vez o el token es inválido.")
            if not os.path.exists('credentials.json'):
                logger.error(
                    "--> ERROR CRÍTICO: No se encontró 'credentials.json'. Descárgalo desde Google Cloud Console.")
                return None
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        logger.info("--> Guardando/actualizando 'token.json' para futuras ejecuciones.")
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    logger.info("--> Autenticación exitosa.")
    return creds


def leer_cuerpo_oferta(filename):
    """Lee el contenido del cuerpo de la oferta desde un archivo de texto."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        logger.info(f"--> Cuerpo de la oferta leído de '{filename}'.")
        return content
    except FileNotFoundError:
        logger.error(f"--> ERROR: No se encontró el archivo del cuerpo de la oferta: '{filename}'.")
        return None
    except Exception as e:
        logger.error(f"--> ERROR al leer el archivo del cuerpo de la oferta '{filename}': {e}")
        return None


def leer_destinatarios_y_asuntos(filename):
    """Lee los correos y asuntos personalizados desde un archivo de texto."""
    destinatarios = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and ',' in line:
                    email, subject = line.split(',', 1)  # Divide solo en la primera coma
                    destinatarios.append({'email': email.strip(), 'subject': subject.strip()})
                elif line:
                    logger.warning(
                        f"--> ADVERTENCIA: Línea en '{filename}' con formato incorrecto (sin coma): '{line}'. Ignorando.")
        logger.info(f"--> {len(destinatarios)} destinatarios y asuntos leídos de '{filename}'.")
        return destinatarios
    except FileNotFoundError:
        logger.error(f"--> ERROR: No se encontró el archivo de destinatarios: '{filename}'.")
        return None
    except Exception as e:
        logger.error(f"--> ERROR al leer el archivo de destinatarios '{filename}': {e}")
        return None


def crear_mensaje_email(destinatario_email, asunto, cuerpo_html):
    """Crea el cuerpo del mensaje de correo electrónico (sin archivos adjuntos)."""
    logger.info(f"    --> Creando cuerpo del email para '{destinatario_email}' con asunto: '{asunto}'.")

    mensaje = MIMEMultipart()
    mensaje['to'] = destinatario_email
    mensaje['from'] = 'me'  # 'me' se refiere a la cuenta autenticada
    mensaje['subject'] = asunto

    # Asumimos que mi_oferta.txt puede contener HTML o texto plano
    mensaje.attach(MIMEText(cuerpo_html, 'html'))

    encoded_message = base64.urlsafe_b64encode(mensaje.as_bytes()).decode()
    return {'raw': encoded_message}


def mover_correo_a_papelera(service_gmail, message_id):
    """Mueve un correo enviado a la papelera."""
    try:
        service_gmail.users().messages().modify(
            userId='me',
            id=message_id,
            body={'addLabelIds': ['TRASH'], 'removeLabelIds': ['SENT']}
        ).execute()
        logger.info(f"  --> Correo con ID '{message_id}' movido a la papelera.")
    except HttpError as error:
        logger.error(f"  --> ERROR al mover el correo '{message_id}' a la papelera: {error}")
    except Exception as e:
        logger.error(f"  --> ERROR inesperado al mover el correo '{message_id}' a la papelera: {e}")


def main():
    """Función principal que orquesta todo el proceso."""
    parser = argparse.ArgumentParser(description="Script para enviar ofertas de negociación de deudas.")
    parser.add_argument('--enviar-correos', action='store_true',
                        help='Activa el envío de correos a los destinatarios definidos en destinatarios.txt.')
    parser.add_argument('--produccion', action='store_true',
                        help='Indica que se está ejecutando en modo producción (para logs y confirmación).')
    parser.add_argument('--ciclos', type=int,
                        help='Número de ciclos de envío a ejecutar. Si no se especifica, se preguntará.')
    parser.add_argument('--retardo-segundos', type=int,
                        help='Retardo en segundos entre cada ciclo de envío. Si no se especifica, se preguntará.')

    args = parser.parse_args()

    logger.info("\n--- INICIANDO PROCESO DE NEGOCIACIÓN AUTOMÁTICA ---")

    logger.info("\n[Paso 1 de 4] Autenticando con Google...")
    creds = autenticar_google()
    if not creds:
        logger.critical("\n--- PROCESO FALLIDO: AUTENTICACIÓN ---")
        return

    logger.info("\n[Paso 2 de 4] Construyendo servicios de Google...")
    try:
        service_gmail = build('gmail', 'v1', credentials=creds)
        logger.info("--> Servicio para Gmail: OK")
    except HttpError as error:
        logger.critical(f"--> Ocurrió un error al construir los servicios de Google: {error}")
        return

    logger.info(f"\n[Paso 3 de 4] Leyendo cuerpo de la oferta desde '{EMAIL_OFFER_BODY_FILE}'...")
    cuerpo_oferta = leer_cuerpo_oferta(EMAIL_OFFER_BODY_FILE)
    if cuerpo_oferta is None:
        logger.critical("\n--- PROCESO DETENIDO: No se pudo leer el cuerpo de la oferta. ---")
        return

    logger.info(f"\n[Paso 4 de 4] Leyendo destinatarios y asuntos desde '{RECIPIENT_EMAILS_FILE}'...")
    destinatarios_info = leer_destinatarios_y_asuntos(RECIPIENT_EMAILS_FILE)
    if destinatarios_info is None or not destinatarios_info:
        logger.critical("\n--- PROCESO DETENIDO: No se pudieron leer los destinatarios o la lista está vacía. ---")
        return

    # --- Determinar el número de ciclos y retardo ---
    num_loops = args.ciclos
    retardo_segundos = args.retardo_segundos

    if args.enviar_correos:  # Solo preguntar si se activa el envío
        if num_loops is None:
            num_loops_str = input(
                "¿Cuántos ciclos de envío deseas ejecutar para todos los destinatarios? (1 por defecto): ")
            try:
                num_loops = int(num_loops_str)
                if num_loops < 1:
                    num_loops = 1
                    logger.warning("Número de ciclos inválido (<1), se usará 1 ciclo.")
            except ValueError:
                num_loops = 1
                logger.warning("Entrada inválida para número de ciclos, se usará 1 ciclo.")

        if retardo_segundos is None:
            retardo_str = input("¿Cuántos segundos de retardo entre ciclos deseas? (0 por defecto): ")
            try:
                retardo_segundos = int(retardo_str)
                if retardo_segundos < 0:
                    retardo_segundos = 0
                    logger.warning("Retardo en segundos inválido (<0), se usará 0 segundos.")
            except ValueError:
                retardo_segundos = 0
                logger.warning("Entrada inválida para retardo, se usará 0 segundos.")

        # Asegurarse de que los valores sean al menos 0 (para retardo) o 1 (para ciclos) si no se especificaron o fueron inválidos
        num_loops = max(1, num_loops if num_loops is not None else 1)
        retardo_segundos = max(0, retardo_segundos if retardo_segundos is not None else 0)

        logger.info("\n--- INICIANDO ENVÍO DE CORREOS ---")

        if not args.produccion:
            logger.warning(
                f"\n  ADVERTENCIA: Modo de prueba (sin --produccion). Los correos se enviarán a los destinatarios listados en 'destinatarios.txt'. Se ejecutarán {num_loops} ciclo(s).")
        else:
            logger.info(
                f"\n  Modo producción activo. Los correos se enviarán a los destinatarios listados en 'destinatarios.txt'. Se ejecutarán {num_loops} ciclo(s).")

        if retardo_segundos > 0:
            logger.info(f"  Se ha configurado un retardo de {retardo_segundos} segundos entre ciclos.")

        # --- ENVOLVEMOS EL BUCLE DE ENVÍO EN OTRO BUCLE PARA LOS LOOPS ---
        for i in range(num_loops):
            # Imprime en la terminal el ciclo actual
            print(f"\n*** EJECUTANDO CICLO DE ENVÍO {i + 1} de {num_loops} ***")
            logger.info(f"\n--- INICIANDO CICLO DE ENVÍO {i + 1} de {num_loops} ---")
            for dest_info in destinatarios_info:
                destinatario_real = dest_info['email']
                asunto_personalizado = dest_info['subject']

                # El destinatario final SIEMPRE será el de destinatarios.txt si --enviar-correos está activo
                destinatario_final = destinatario_real

                logger.info(f"\n  -> Preparando correo para: {destinatario_final} (Asunto: '{asunto_personalizado}')")

                # Aquí se añade el print statement para cada destinatario
                print(f"  [Ciclo {i + 1}/{num_loops}] Enviando correo a: {destinatario_final}")

                mensaje_final = crear_mensaje_email(destinatario_final, asunto_personalizado, cuerpo_oferta)

                try:
                    logger.info(f"  -> Intentando enviar correo a: {destinatario_final}")
                    sent_message = service_gmail.users().messages().send(userId='me', body=mensaje_final).execute()
                    logger.info(f"  --> ÉXITO. Correo enviado. Message ID: {sent_message['id']}")

                    # Mover el correo a la papelera después de enviarlo
                    mover_correo_a_papelera(service_gmail, sent_message['id'])

                except HttpError as error:
                    logger.error(f"  --> ERROR al enviar el correo a {destinatario_final}: {error}")
                    print(f"  [Ciclo {i + 1}/{num_loops}] ERROR al enviar a: {destinatario_final} - {error}") # Añadido para errores
                except Exception as e:
                    logger.error(f"  --> ERROR inesperado al enviar el correo a {destinatario_final}: {e}")
                    print(f"  [Ciclo {i + 1}/{num_loops}] ERROR inesperado al enviar a: {destinatario_final} - {e}") # Añadido para errores

            logger.info(f"--- FINALIZADO CICLO DE ENVÍO {i + 1} de {num_loops} ---")

            # --- PAUSA ENTRE CICLOS (si no es el último ciclo) ---
            if i < num_loops - 1 and retardo_segundos > 0:
                logger.info(f"  Esperando {retardo_segundos} segundos antes del siguiente ciclo...")
                print(f"  Esperando {retardo_segundos} segundos antes del siguiente ciclo...")
                time.sleep(retardo_segundos)
        # --- FIN DEL BUCLE DE LOOPS ---

        logger.info("\n--- PROCESO DE ENVÍO DE CORREOS FINALIZADO ---")
    else:
        logger.info("\n--- MODO DE PRUEBA: No se enviaron correos. Usa '--enviar-correos' para activarlo. ---")

    logger.info("\n--- PROCESO PRINCIPAL FINALIZADO ---")


if __name__ == '__main__':
    main()