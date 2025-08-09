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
            cwd=script_path.parent # <-- FIX: Ejecutar el script desde su propio directorio
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