"""
Orquestador Principal del Proceso Diario de Scraping y Análisis de NeoAuto.

Este script actúa como el punto de entrada para la ejecución diaria de todas las
tareas relacionadas con la recopilación, procesamiento y análisis de datos de
vehículos de segunda mano de NeoAuto.

El proceso se ejecuta en la siguiente secuencia:
1.  **Lanzamiento de VPN**: Inicia ProtonVPN para asegurar una conexión anónima
    y evitar bloqueos de IP. Espera un tiempo prudencial para que la conexión
    se establezca.
2.  **Ejecución de Scripts Secuenciales**: Invoca una serie de scripts, cada uno
    responsable de una etapa específica del pipeline de datos. Si un script
    falla, el orquestador detiene la ejecución para evitar errores en cascada.

La secuencia de scripts ejecutados es:
    - `2.DIARIO.daily_urls_extraction.VCLI.py`: Extrae las URLs de los anuncios
      publicados "hoy" en NeoAuto y las guarda en Supabase.
    - `4.DIARIO.SEMANAL.SCRAPER.NEOAUTO.SUPABASE.PARA.CRON.py`: Realiza el scraping
      detallado de las URLs obtenidas, guardando los resultados en archivos .txt.
    - `5.DIARIO.SEMANAL.Procesador_txt.a.json.DEEPSEEK_VCLI.py`: Procesa los .txt,
      extrae la información estructurada y la convierte a formato .json.
    - `6.json_a_supabase.DEEP.SEEK.CRON.VCLI.py`: Sube los datos de los archivos
      .json a la tabla principal de detalles de autos en Supabase.
    - `main.py`: Ejecuta el análisis de mercado completo sobre los datos históricos,
      genera los reportes HTML (principal y por modelo) y filtra los leads
      atractivos del día.
    - `gmail_sender.py`: Envía el reporte de leads atractivos por correo
      electrónico a los destinatarios configurados.

El script utiliza un sistema de logging centralizado en `main_daily.log` para
registrar el progreso y los errores de toda la operación.
"""
import subprocess
import sys
from pathlib import Path
import logging
from datetime import datetime
import time

# Configuración básica de logging para main_daily.py
log_file_path = Path(__file__).parent / "main_daily.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8', mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def launch_vpn_and_wait():
    """Lanza la VPN y espera para que se establezca la conexión."""
    vpn_path = r"C:\Program Files\Proton\VPN\ProtonVPN.Launcher.exe"
    logger.info(f"Lanzando Proton VPN desde: {vpn_path}")
    try:
        subprocess.Popen(vpn_path)
        wait_time = 30
        logger.info(f"Esperando {wait_time} segundos para que la conexión VPN se establezca...")
        time.sleep(wait_time)
        logger.info("Pausa finalizada. Se asume que la VPN está conectada.")
        return True
    except FileNotFoundError:
        logger.error(f"Error Crítico: No se encontró el ejecutable de Proton VPN en la ruta: {vpn_path}")
        return False
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado al lanzar la VPN: {e}")
        return False

def run_script(script_path: Path, script_name: str, script_args: list = []):
    """Ejecuta un script desde su propio directorio, captura su output y solo muestra advertencias o errores."""
    logger.info(f"--- Iniciando: {script_name} (Args: {script_args}) ---")
    try:
        command = [sys.executable, str(script_path)] + script_args
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            encoding='utf-8',
            errors='replace', # <--- THIS IS THE MISSING LINE
            cwd=script_path.parent
        )
        logging.debug(f"Output de {script_name}:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Advertencias de {script_name}:\n{result.stderr}")
        logger.info(f"--- Finalizado: {script_name} (Exitosamente) ---")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"--- ERROR en {script_name}: El script falló con código de error {e.returncode}. ---")
        if e.stdout:
            logger.error(f"Stdout de {script_name}:\n{e.stdout}")
        if e.stderr:
            logger.error(f"Stderr de {script_name}:\n{e.stderr}")
        return False
    except Exception as e:
        logger.error(f"--- ERROR Inesperado en {script_name}: {e} ---")
        return False

def main():
    logger.info("=" * 60)
    logger.info(f"Iniciando el proceso diario de scraping y reporte ({datetime.now()})")
    logger.info("=" * 60)

    if not launch_vpn_and_wait():
        logger.critical("No se pudo iniciar la VPN. Abortando la ejecución.")
        sys.exit(1)

    base_path = Path(__file__).parent
    extractores_path = base_path / "extractores"
    gmail_sender_path = base_path / "gmail_sender"

    scripts_to_run = [
        (extractores_path / "2.DIARIO.daily_urls_extraction.VCLI.py", "2.DIARIO.daily_urls_extraction.VCLI.py", []),
        (extractores_path / "4.DIARIO.SEMANAL.SCRAPER.NEOAUTO.SUPABASE.PARA.CRON.py", "4.DIARIO.SEMANAL.SCRAPER.NEOAUTO.SUPABASE.PARA.CRON.py", []),
        (extractores_path / "5.DIARIO.SEMANAL.Procesador_txt.a.json.DEEPSEEK_VCLI.py", "5.DIARIO.SEMANAL.Procesador_txt.a.json.DEEPSEEK_VCLI.py", []),
        (extractores_path / "6.json_a_supabase.DEEP.SEEK.CRON.VCLI.py", "6.json_a_supabase.DEEP.SEEK.CRON.VCLI.py", []),
        (base_path / "main.py", "main.py", []),
        (gmail_sender_path / "gmail_sender.py", "gmail_sender.py", ["--enviar-correos", "--produccion"])
    ]

    for script_path, script_name, args in scripts_to_run:
        if not run_script(script_path, script_name, args):
            logger.critical(f"Orquestación detenida debido a un error en {script_name}.")
            sys.exit(1)

    logger.info("=" * 60)
    logger.info("Proceso diario completado exitosamente.")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
