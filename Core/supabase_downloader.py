# Core/supabase_downloader.py
import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# --- Configuración (sin cambios) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)

current_script_dir = Path(__file__).resolve().parent
dotenv_path = current_script_dir / ".env"
RULES_CSV_PATH = current_script_dir / "reglas_modelos_base.csv"
TABLE_NAME = "autos_detalles"
LOADED_MODEL_RULES = None
TARGET_MAKE_MAPPING = {
    'mercedes benz': 'mercedes', 'mercedes-benz': 'mercedes', 'mercedes': 'mercedes',
    'vw': 'volkswagen', 'volkswagen': 'volkswagen', 'toyota': 'toyota', 'bmw': 'bmw',
    'nissan': 'nissan', 'hyundai': 'hyundai', 'subaru': 'subaru', 'mazda': 'mazda',
    'ford': 'ford', 'kia': 'kia', 'jeep': 'jeep', 'audi': 'audi', 'honda': 'honda',
    'chevrolet': 'chevrolet', 'mitsubishi': 'mitsubishi', 'suzuki': 'suzuki', 'volvo': 'volvo'
}

def _load_env_and_rules():
    # ... (sin cambios)
    pass

def initialize_supabase_client() -> Client:
    # ... (sin cambios)
    pass
    
# --- INICIO DE LA LÓGICA CORREGIDA ---

def fetch_recent_data(client: Client, table_name: str, days: int) -> list:
    """
    Descarga datos de los últimos 'days' días, ordenados del más nuevo al más viejo.
    Este método es más robusto que descargar la tabla entera.
    """
    logger.info(f"Iniciando descarga ROBUSTA: Obteniendo datos de los últimos {days} días de '{table_name}'")
    start_date = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        response = client.from_(table_name).select('*') \
            .gte('DateTime', start_date) \
            .order('DateTime', desc=True) \
            .execute()
        
        data = response.data or []
        logger.info(f"Descarga completada. Total de filas recuperadas: {len(data)}")
        return data
    except Exception as e:
        logger.error(f"Error fatal durante la descarga: {e}", exc_info=True)
        return []

def _standardize_data_and_get_base_model(df: pd.DataFrame) -> pd.DataFrame:
    # ... (lógica de estandarización sin cambios) ...
    return df

def descargar_y_cargar_datos(csv_file_path: Path, **kwargs) -> pd.DataFrame:
    logger.info("Iniciando descarga y procesamiento...")
    _load_env_and_rules()
    
    try:
        supabase_client = initialize_supabase_client()
        # Usamos la nueva función para obtener solo los datos de los últimos 8 días
        data = fetch_recent_data(supabase_client, TABLE_NAME, days=8)
        
        if not data:
            logger.warning(f"No se descargaron datos recientes de la tabla '{TABLE_NAME}'.")
            return pd.DataFrame()

        df_raw = pd.DataFrame(data)
        logger.info(f"Datos crudos cargados en memoria ({len(df_raw)} filas).")
        
        df_processed = _standardize_data_and_get_base_model(df_raw.copy())

        # Guardar en CSV para persistencia
        csv_file_path.parent.mkdir(parents=True, exist_ok=True)
        df_processed.to_csv(csv_file_path, index=False, encoding='utf-8')
        logger.info(f"Datos procesados guardados en: {csv_file_path}")

        if 'DateTime' in df_processed.columns:
            df_processed['DateTime'] = pd.to_datetime(df_processed['DateTime'], errors='coerce', utc=True)
        
        return df_processed
        
    except Exception as e:
        logger.error(f"Error general en descargar_y_cargar_datos: {e}", exc_info=True)
        return pd.DataFrame()

# El resto de funciones (_get_model_base_heuristic, save_data_to_csv, etc.) deben estar
# presentes en tu archivo, pero no necesitan cambios.
# Por claridad, solo se muestra la función modificada 'descargar_y_cargar_datos'
# y la nueva función 'fetch_recent_data'.