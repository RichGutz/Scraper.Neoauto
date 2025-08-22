
# Documentación: `gmail_sender.py`

## Propósito

Servicio de Envío de Correos Electrónicos con Gmail.

Este script se encarga de la última etapa del pipeline: la distribución de los reportes generados. Utiliza la API de Gmail para enviar correos electrónicos de forma automatizada.

## Funcionalidad Principal

1.  **Autenticación Segura con Google (OAuth 2.0)**:
    - Utiliza el flujo de OAuth 2.0 para obtener permisos de manera segura.
    - Busca un archivo `token.json` con credenciales válidas. Si no existe o ha expirado, utiliza `credentials.json` para guiar al usuario a través de un flujo de autenticación en el navegador la primera vez.
    - Guarda el token de acceso y de refresco en `token.json` para futuras ejecuciones sin necesidad de intervención manual.

2.  **Lectura de Configuración**: 
    - Lee el archivo `destinatarios.txt` para obtener una lista de correos electrónicos y los asuntos correspondientes para cada destinatario.
    - Construye dinámicamente la ruta al archivo de reporte HTML generado por `main.py`, basándose en la fecha actual (p. ej., `gmail_attractive_leads_2025-08-10.html`).

3.  **Construcción y Envío de Correos**:
    - Lee el contenido del reporte HTML optimizado para Gmail.
    - Crea un mensaje de correo electrónico en formato MIME, estableciendo el destinatario, remitente, asunto y el cuerpo HTML.
    - Utiliza el servicio de la API de Gmail para enviar el mensaje.

4.  **Gestión de Envíos y Limpieza**:
    - Tras enviar un correo, utiliza la API para mover inmediatamente el mensaje enviado a la papelera. Esto mantiene la bandeja de "Enviados" del usuario limpia.

5.  **Control por Argumentos de Línea de Comandos**:
    - `--enviar-correos`: Activa el envío real. Sin este flag, el script se ejecuta en modo de prueba.
    - `--produccion`: Flag para indicar un entorno de producción.
    - `--ciclos` y `--retardo-segundos`: Permiten envíos múltiples con pausas.

El script también mantiene un registro de sus operaciones en `envio_reporte.log`.
