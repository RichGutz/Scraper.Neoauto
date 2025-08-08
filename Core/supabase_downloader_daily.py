# core/supabase_downloader.py
import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd
import io
from datetime import datetime
import sys
from pathlib import Path
import re
import webbrowser

# Configuración de logging
log_dir_module = Path(__file__).parents[1]
log_file_path_module = log_dir_module / "supabase_downloader.log"
log_dir_module.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path_module, encoding='utf-8', mode='a'),
    ]
)
logger = logging.getLogger(__name__)

current_script_dir = Path(__file__).resolve().parent
dotenv_path = current_script_dir / ".env"
RULES_CSV_PATH = current_script_dir / "reglas_modelos_base.csv" # Ruta al archivo de reglas

SUPABASE_URL = None
SUPABASE_KEY = None
TABLE_NAME = "autos_detalles_diarios"

TARGET_MAKE_MAPPING = {
    'mercedes benz': 'mercedes', 'mercedes-benz': 'mercedes', 'mercedes': 'mercedes',
    'vw': 'volkswagen', 'volkswagen': 'volkswagen',
    'toyota': 'toyota', 'bmw': 'bmw', 'nissan': 'nissan',
    'hyundai': 'hyundai', 'subaru': 'subaru', 'mazda': 'mazda',
    'ford': 'ford', 'kia': 'kia', 'jeep': 'jeep', 'audi': 'audi',
    'honda': 'honda', 'chevrolet': 'chevrolet', 'mitsubishi': 'mitsubishi',
    'suzuki': 'suzuki', 'volvo': 'volvo'
}
STANDARDIZED_TARGET_MAKES = sorted(list(set(TARGET_MAKE_MAPPING.values())))
LOADED_MODEL_RULES = None # Caché para las reglas

def _load_env_and_rules():
    global SUPABASE_URL, SUPABASE_KEY, LOADED_MODEL_RULES
    if not (SUPABASE_URL and SUPABASE_KEY):
        loaded_dotenv = load_dotenv(dotenv_path)
        current_logger = logging.getLogger(__name__) if __name__ == "__main__" else logger
        if loaded_dotenv:
            current_logger.info(f"Variables de entorno cargadas desde {dotenv_path}.")
        else:
            current_logger.warning(f"No se pudo cargar el archivo .env desde {dotenv_path}.")
        SUPABASE_URL = os.getenv("SUPABASE_URL")
        SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    if LOADED_MODEL_RULES is None:
        rules_logger = logging.getLogger(__name__) if __name__ == "__main__" else logger
        try:
            if RULES_CSV_PATH.exists():
                LOADED_MODEL_RULES = pd.read_csv(RULES_CSV_PATH)
                LOADED_MODEL_RULES['make_rule_match'] = LOADED_MODEL_RULES['make_rule_match'].astype(str).str.lower().str.strip()
                LOADED_MODEL_RULES['model_pattern_input_lower'] = LOADED_MODEL_RULES['model_pattern_input_lower'].astype(str).str.lower().str.strip()
                LOADED_MODEL_RULES['model_base_target'] = LOADED_MODEL_RULES['model_base_target'].astype(str).str.strip()
                LOADED_MODEL_RULES['match_type'] = LOADED_MODEL_RULES['match_type'].astype(str).str.lower().str.strip()
                LOADED_MODEL_RULES['priority'] = pd.to_numeric(LOADED_MODEL_RULES['priority'], errors='coerce').fillna(0).astype(int)
                LOADED_MODEL_RULES['pattern_length'] = LOADED_MODEL_RULES['model_pattern_input_lower'].str.len()
                LOADED_MODEL_RULES.sort_values(by=['priority', 'pattern_length'], ascending=[False, False], inplace=True)
                rules_logger.info(f"Archivo de reglas '{RULES_CSV_PATH.name}' cargado y ordenado. {len(LOADED_MODEL_RULES)} reglas.")
                if __name__ == "Core.supabase_downloader" or __name__ == "__main__": # Log más detallado si se ejecuta directo
                    rules_logger.info(f"Primeras 5 reglas cargadas: \n{LOADED_MODEL_RULES.head().to_string()}")

            else:
                rules_logger.warning(f"Archivo de reglas '{RULES_CSV_PATH.name}' no encontrado. La derivación de Model_Base será limitada.")
                LOADED_MODEL_RULES = pd.DataFrame(columns=['make_rule_match', 'model_pattern_input_lower', 'model_base_target', 'match_type', 'priority', 'pattern_length'])
        except Exception as e_rules:
            rules_logger.error(f"Error cargando o procesando el archivo de reglas '{RULES_CSV_PATH.name}': {e_rules}", exc_info=True)
            LOADED_MODEL_RULES = pd.DataFrame(columns=['make_rule_match', 'model_pattern_input_lower', 'model_base_target', 'match_type', 'priority', 'pattern_length'])


def initialize_supabase_client() -> Client:
    _load_env_and_rules()
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL y SUPABASE_KEY deben estar definidos en .env (initialize_supabase_client)")
        raise ValueError("SUPABASE_URL y SUPABASE_KEY no configurados.")
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        logger.error(f"Error al inicializar cliente Supabase: {e}")
        raise

def fetch_all_data_from_table(client: Client, table_name: str) -> list:
    logger.info(f"Recuperando datos de tabla: '{table_name}'")
    all_data = []
    offset = 0
    limit = 1000 # Ajustar si es necesario y permitido por Supabase
    while True:
        try:
            response = client.from_(table_name).select('*').range(offset, offset + limit - 1).execute()
            if response.data:
                all_data.extend(response.data)
                if len(all_data) % 5000 == 0 or len(response.data) < limit or offset + limit > 25000 : # Log más frecuente al final
                    logger.info(f"Fetched {len(response.data)} rows (total: {len(all_data)}) from offset {offset}.")
                if len(response.data) < limit: break
                offset += limit
            else:
                logger.info(f"No más datos para recuperar de tabla '{table_name}'. Total: {len(all_data)}")
                break
        except Exception as e:
            logger.error(f"Error recuperando datos de Supabase para tabla '{table_name}': {e}")
            # Considerar reintentos o manejo de errores más específico si es necesario
            raise
    logger.info(f"Datos totales recuperados de '{table_name}': {len(all_data)} filas.")
    return all_data

def save_data_to_csv(df: pd.DataFrame, filename: str):
    # Si el df está vacío y el archivo existe, no lo sobrescribas (para no perder datos si algo falla)
    if df.empty and Path(filename).exists() and Path(filename).stat().st_size > 0:
        logger.warning(f"DataFrame para guardar en '{filename}' está vacío, pero el archivo ya existe y tiene contenido. No se sobrescribirá.")
        return
    if df.empty:
        logger.warning(f"DataFrame vacío. Creando archivo CSV vacío en '{filename}'.")
        # Intentar crear con columnas si es posible, o simplemente vacío
        pd.DataFrame(columns=df.columns if not df.columns.empty else None).to_csv(filename, index=False, encoding='utf-8')
        return
    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"Datos (DataFrame con {len(df)} filas) guardados en '{filename}'")
    except Exception as e:
        logger.error(f"Error guardando DataFrame en CSV '{filename}': {e}")
        raise


def _get_model_base_heuristic(model_name_std: str, make_name_rule_match: str) -> str:
        # ### INICIO DE LÍNEAS DE DEPURACIÓN TEMPORAL ###
    if 'hilux' in model_name_std.lower():
        print(f"DEBUG HILUX - Input a la función: '{model_name_std}'")
    # ### FIN DE LÍNEAS DE DEPURACIÓN TEMPORAL ###
    
    if pd.isna(model_name_std) or model_name_std == "Desconocido" or not model_name_std:
        return "Desconocido"

    global LOADED_MODEL_RULES
    if LOADED_MODEL_RULES is None:
        # Este caso no debería ocurrir si _load_env_and_rules se llama correctamente antes.
        # Pero como fallback, intentar cargarlas.
        logger.warning("_get_model_base_heuristic: LOADED_MODEL_RULES es None. Intentando recargar reglas.")
        _load_env_and_rules() 
        if LOADED_MODEL_RULES is None: # Si falla de nuevo
            logger.error("_get_model_base_heuristic: Falló la recarga de LOADED_MODEL_RULES. Devolviendo modelo original.")
            return model_name_std 

    model_name_std_lower = model_name_std.lower().strip()
    # make_name_rule_match ya debería estar en minúsculas y estandarizado.
    
    if not LOADED_MODEL_RULES.empty:
        rules_for_make = LOADED_MODEL_RULES[LOADED_MODEL_RULES['make_rule_match'] == make_name_rule_match]
        
        # Las reglas ya están ordenadas por prioridad y longitud de patrón al cargar
        for _, rule in rules_for_make.iterrows():
            pattern = rule['model_pattern_input_lower']
            match_type = rule['match_type']
            target_base = rule['model_base_target']

            match_found = False
            if match_type == 'exact' and model_name_std_lower == pattern:
                match_found = True
            elif match_type == 'startswith' and model_name_std_lower.startswith(pattern):
                match_found = True
            elif match_type == 'contains' and pattern in model_name_std_lower:
                match_found = True
            
            if match_found:
                # logger.debug(f"Regla CSV '{match_type}' aplicada para '{make_name_rule_match} - {model_name_std}': Patrón '{pattern}' -> '{target_base}' (Prioridad: {rule['priority']})")
                                # ### INICIO DE LÍNEA DE DEPURACIÓN TEMPORAL ###
                if 'hilux' in model_name_std.lower():
                    print(f"DEBUG HILUX - ¡COINCIDENCIA! Patrón: '{pattern}', Tipo: '{match_type}', Resultado: '{target_base}'")
                # ### FIN DE LÍNEA DE DEPURACIÓN TEMPORAL ###
                return target_base
    
    # Fallback si no hay reglas CSV o ninguna coincide: devolver el modelo específico estandarizado
    # logger.debug(f"No se aplicó regla CSV para '{make_name_rule_match} - {model_name_std}'. Devolviendo: '{model_name_std}' (modelo específico estandarizado).")
    return model_name_std 


def _standardize_data_and_get_base_model(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Iniciando estandarización de Make/Model y creación de Model_Base ({len(df)} filas iniciales).")
    if df.empty:
        logger.warning("_standardize_data_and_get_base_model recibió un DataFrame vacío.")
        # Devolver un DataFrame vacío con las columnas esperadas para evitar errores downstream
        return pd.DataFrame(columns=['Make', 'Model', 'Model_Base'] + [col for col in df.columns if col not in ['Make','Model']])


    df_std = df.copy()
    desconocido_str = "Desconocido"
    empty_like_strings = ['', 'Nan', 'none', 'null', 'otros'] # Simplificado, se usará con lower()
    # Lista más completa de valores a considerar como "Desconocido" o vacío, insensible a mayúsculas/minúsculas
    empty_like_values_check = ['', 'nan', 'none', 'null', 'desconocido', 'otros', 'otros modelos']


    # 1. Estandarizar 'Make'
    if 'Make' in df_std.columns:
        # Primero, una columna para el matching de reglas (lower, y mapeado por TARGET_MAKE_MAPPING)
        df_std['Make_Rule_Match_Input'] = df_std['Make'].fillna(desconocido_str).astype(str).str.lower().str.strip()
        df_std['Make_Rule_Match_Input'] = df_std['Make_Rule_Match_Input'].map(TARGET_MAKE_MAPPING).fillna(df_std['Make_Rule_Match_Input'])
        
        # Luego, la columna 'Make' final (TitleCase de la forma estandarizada por TARGET_MAKE_MAPPING)
        df_std['Make'] = df_std['Make_Rule_Match_Input'].str.title()
        df_std.loc[df_std['Make'].str.lower().isin(empty_like_values_check) | df_std['Make'].isnull(), 'Make'] = desconocido_str
    else:
        logger.warning("Columna 'Make' no encontrada. Se creará 'Make' como Desconocido.")
        df_std['Make'] = desconocido_str
        df_std['Make_Rule_Match_Input'] = desconocido_str.lower()

    # 2. Estandarizar 'Model' (modelo específico) -> se guarda en 'Model'
    if 'Model' in df_std.columns:
        df_std['Model'] = df_std['Model'].fillna(desconocido_str).astype(str).str.strip()
        # Corregir ALL CAPS ALPHA a Title Case
        all_upper_alpha_condition = df_std['Model'].str.isupper() & df_std['Model'].str.isalpha()
        df_std.loc[all_upper_alpha_condition, 'Model'] = df_std.loc[all_upper_alpha_condition, 'Model'].str.title()
        
        # Estandarizaciones específicas (ejemplos, expandir según necesidad desde el CSV o aquí)
        df_std['Model'] = df_std['Model'].str.replace(r'\s*CR\s*V\s*$', 'CR-V', regex=True, case=False)
        df_std['Model'] = df_std['Model'].str.replace(r'^X\sTrail$', 'X-Trail', case=False, regex=True) # Solo si es exacto "X Trail"
        df_std['Model'] = df_std['Model'].str.replace(r'All\sNew', 'All-New', case=False, regex=False)
        
        df_std.loc[df_std['Model'].str.lower().isin(empty_like_values_check) | df_std['Model'].isnull() | df_std['Model'].str.match(r'^\s*$'), 'Model'] = desconocido_str
    else:
        logger.warning("Columna 'Model' no encontrada. Se creará 'Model' como Desconocido.")
        df_std['Model'] = desconocido_str

    # 3. Crear 'Model_Base' usando las reglas del CSV y el 'Model' (específico estandarizado)
    # _get_model_base_heuristic espera make_name_rule_match (lower) y model_name_std (case original procesado)
    df_std['Model_Base'] = df_std.apply(lambda row: _get_model_base_heuristic(row['Model'], row['Make_Rule_Match_Input']), axis=1)
    
    # Limpiar columna temporal
    df_std.drop(columns=['Make_Rule_Match_Input'], inplace=True, errors='ignore')

    logger.info(f"Estandarización y derivación de Model_Base completada. Columnas finales principales: Make, Model, Model_Base.")
    return df_std


def generate_unique_make_model_report_html(df_for_report: pd.DataFrame, output_filepath: Path):
    # (Función sin cambios, igual a la versión anterior, omitida por brevedad)
    logger.info(f"Generando reporte HTML de Marcas y Modelos únicos (para marcas seleccionadas) en: {output_filepath}")
    html_parts = ["<html><head><title>Reporte de Marcas y Modelos Únicos (Marcas Seleccionadas)</title>"]
    html_parts.append("<style>body {font-family: sans-serif; margin:20px;} table {border-collapse: collapse; margin-bottom: 20px; font-size:0.9em;} th, td {border: 1px solid #ddd; padding: 6px; text-align: left;} th {background-color: #f2f2f2;} h1, h2, h3 {margin-top: 25px; color: #333;} .make-block {margin-bottom: 20px; padding:10px; page-break-inside: avoid;}</style>")
    html_parts.append("</head><body>")
    html_parts.append("<h1>Reporte de Marcas y Modelos Únicos (Marcas Seleccionadas)</h1>")
    html_parts.append(f"<p>Este reporte muestra las variaciones encontradas en las columnas 'Make' y 'Model' de los datos crudos descargados, <strong>filtrado solo para las siguientes marcas estandarizadas: {', '.join(STANDARDIZED_TARGET_MAKES)}</strong>.</p>")
    html_parts.append("<p>Utiliza este reporte para identificar cómo necesitas ajustar el archivo de reglas <code>reglas_modelos_base.csv</code>.</p>")
    if 'Make' not in df_for_report.columns or 'Model' not in df_for_report.columns:
        logger.error("Las columnas 'Make' y/o 'Model' no se encuentran en el DataFrame para el reporte HTML.")
        html_parts.append("<p><strong>Error: Columnas 'Make' y/o 'Model' no encontradas.</strong></p>")
    else:
        make_col_report = df_for_report['Make'].fillna("NULO_EN_CRUDO").astype(str).str.strip()
        html_parts.append("<h2>Análisis de la Columna 'Make' (Original, después de filtrar por marcas seleccionadas)</h2>")
        make_counts_report = make_col_report.value_counts(dropna=False).sort_index().reset_index()
        make_counts_report.columns = ['Make (Original en Datos Filtrados)', 'Count']
        html_parts.append("<h3>Valores Únicos y Conteos para 'Make':</h3>")
        html_parts.append(make_counts_report.to_html(index=False, escape=True, classes='table table-striped table-sm'))
        html_parts.append("<h2>Análisis de la Columna 'Model' por Marca (Original, después de filtrar por marcas seleccionadas)</h2>")
        df_report_copy = df_for_report.copy()
        df_report_copy['Make_For_Report_Display'] = df_report_copy['Make'].fillna("NULO_EN_CRUDO").astype(str).str.strip().str.title()
        sorted_unique_makes_for_report_display = sorted(df_report_copy['Make_For_Report_Display'].unique())
        for make_display_name in sorted_unique_makes_for_report_display:
            if make_display_name.lower() in ['nan', 'desconocido', 'nulo_en_crudo']: continue
            html_parts.append(f"<div class='make-block'><h3>Modelos para Marca: '{make_display_name}'</h3>")
            models_for_this_make_report = df_report_copy[df_report_copy['Make_For_Report_Display'] == make_display_name]['Model'].fillna("NULO_EN_CRUDO").astype(str).str.strip()
            unique_models_for_make_counts_report = models_for_this_make_report.value_counts(dropna=False).sort_index().reset_index()
            unique_models_for_make_counts_report.columns = [f'Model (Original para {make_display_name})', 'Count']
            if unique_models_for_make_counts_report.empty: html_parts.append("<p>No se encontraron modelos.</p>")
            else:
                html_parts.append(f"<p>{len(unique_models_for_make_counts_report)} formas únicas de 'Model' encontradas para '{make_display_name}':</p>")
                html_parts.append(unique_models_for_make_counts_report.to_html(index=False, escape=True, classes='table table-striped table-sm'))
            html_parts.append("</div>")
    html_parts.append("</body></html>")
    final_html = "".join(html_parts)
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(output_filepath, "w", encoding="utf-8") as f: f.write(final_html)
    logger.info(f"Reporte HTML de datos (marcas seleccionadas) generado: {output_filepath}")
    try:
        webbrowser.open(f"file://{output_filepath.resolve()}")
    except Exception: logger.info(f"No se pudo abrir reporte HTML. Abre manualmente: {output_filepath.resolve()}")


def generate_unique_make_model_report_txt(df_for_report: pd.DataFrame, output_filepath: Path):
    # (Función sin cambios, igual a la versión anterior, omitida por brevedad)
    logger.info(f"Generando reporte TXT de Marcas y Modelos únicos (para marcas seleccionadas) en: {output_filepath}")
    report_lines = []
    report_lines.append("Reporte de Marcas y Modelos Únicos (Marcas Seleccionadas)\n")
    report_lines.append("============================================================\n")
    report_lines.append(f"Este reporte muestra las variaciones encontradas en las columnas 'Make' y 'Model' de los datos crudos descargados,\n")
    report_lines.append(f"filtrado solo para las siguientes marcas estandarizadas: {', '.join(STANDARDIZED_TARGET_MAKES)}\n\n")
    report_lines.append("Utiliza este reporte para identificar cómo necesitas ajustar el archivo de reglas `reglas_modelos_base.csv`.\n\n")
    if 'Make' not in df_for_report.columns or 'Model' not in df_for_report.columns:
        logger.error("Las columnas 'Make' y/o 'Model' no se encuentran en el DataFrame para el reporte TXT.")
        report_lines.append("\nERROR: Columnas 'Make' y/o 'Model' no encontradas.\n")
    else:
        df_report_copy_txt = df_for_report.copy()
        df_report_copy_txt['Make_For_Report_Display_Txt'] = df_report_copy_txt['Make'].fillna("NULO_EN_CRUDO").astype(str).str.strip().str.title()
        sorted_unique_makes_for_report_txt = sorted(df_report_copy_txt['Make_For_Report_Display_Txt'].unique())
        for make_display_name in sorted_unique_makes_for_report_txt:
            if make_display_name.lower() in ['nan', 'desconocido', 'nulo_en_crudo']: continue
            report_lines.append(f"\n--- Marca: {make_display_name} ---\n")
            models_for_this_make_series = df_report_copy_txt[df_report_copy_txt['Make_For_Report_Display_Txt'] == make_display_name]['Model'].fillna("NULO_EN_CRUDO").astype(str).str.strip()
            unique_models_with_counts = models_for_this_make_series.value_counts(dropna=False).sort_index()
            if unique_models_with_counts.empty: report_lines.append("  No se encontraron modelos para esta marca.\n")
            else:
                report_lines.append(f"  {len(unique_models_with_counts)} formas únicas de 'Model' encontradas (Conteo):\n")
                for model_name, count in unique_models_with_counts.items():
                    report_lines.append(f"    - \"{model_name}\" (Count: {count})\n")
    output_filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(output_filepath, "w", encoding="utf-8") as f: f.write("".join(report_lines))
    logger.info(f"Reporte TXT de datos (marcas seleccionadas) generado: {output_filepath}")


def descargar_y_cargar_datos(csv_file_path: Path, 
                             generate_raw_html_report: bool = False, raw_html_report_path: Path = None,
                             generate_raw_txt_report: bool = False, raw_txt_report_path: Path = None
                             ) -> pd.DataFrame:
    logger.info(f"Iniciando descarga de datos a: {csv_file_path}")
    _load_env_and_rules() 
    try:
        supabase_client = initialize_supabase_client()
        data = fetch_all_data_from_table(supabase_client, TABLE_NAME)
        csv_file_path.parent.mkdir(parents=True, exist_ok=True)

        if data:
            df_initial_raw = pd.DataFrame(data)
            logger.info(f"Datos crudos descargados ({len(df_initial_raw)} filas). Columnas originales: {df_initial_raw.columns.tolist()}")

            df_for_report_and_processing = df_initial_raw.copy()
            if 'Make' in df_for_report_and_processing.columns:
                df_for_report_and_processing['make_lower_temp_filter'] = df_for_report_and_processing['Make'].fillna('').astype(str).str.lower().str.strip()
                df_for_report_and_processing['make_standardized_for_filter'] = df_for_report_and_processing['make_lower_temp_filter'].map(TARGET_MAKE_MAPPING)
                original_row_count_before_filter = len(df_for_report_and_processing)
                df_for_report_and_processing = df_for_report_and_processing[df_for_report_and_processing['make_standardized_for_filter'].isin(STANDARDIZED_TARGET_MAKES)].copy()
                filtered_row_count_after_filter = len(df_for_report_and_processing)
                logger.info(f"Filtrado por marca para reporte/procesamiento completado. Filas antes: {original_row_count_before_filter}, Filas después: {filtered_row_count_after_filter}")
                df_for_report_and_processing.drop(columns=['make_lower_temp_filter', 'make_standardized_for_filter'], inplace=True, errors='ignore')
            else:
                logger.warning("Columna 'Make' no encontrada. No se pudo filtrar por marca. Reportes y procesamiento usarán todos los datos.")
            
            if generate_raw_html_report and raw_html_report_path:
                generate_unique_make_model_report_html(df_for_report_and_processing.copy(), raw_html_report_path) 
            if generate_raw_txt_report and raw_txt_report_path:
                generate_unique_make_model_report_txt(df_for_report_and_processing.copy(), raw_txt_report_path)

            df_processed = _standardize_data_and_get_base_model(df_for_report_and_processing)
            save_data_to_csv(df_processed, str(csv_file_path))
            logger.info(f"Datos (filtrados y procesados) guardados en CSV: {csv_file_path}")
            df_final_return = pd.read_csv(str(csv_file_path), low_memory=False)
            if 'DateTime' in df_final_return.columns:
                df_final_return['DateTime'] = pd.to_datetime(df_final_return['DateTime'], errors='coerce', utc=True)
        else:
            logger.warning(f"No se descargaron datos de la tabla '{TABLE_NAME}'.")
            df_final_return = pd.DataFrame()
            expected_cols = ['URL', 'DateTime', 'Make', 'Model', 'Price', 'Year', 'Model_Base']
            for col in expected_cols:
                if col not in df_final_return.columns: 
                    dtype = 'float64' if col in ['Price', 'Year'] else 'object'
                    if col == 'DateTime': dtype = 'datetime64[ns, UTC]'
                    df_final_return[col] = pd.Series(dtype=dtype)
            if not Path(csv_file_path).exists() or Path(csv_file_path).stat().st_size == 0 :
                df_final_return.to_csv(str(csv_file_path), index=False) 
        return df_final_return
    except Exception as e:
        logger.error(f"Error general en descargar_y_cargar_datos: {e}", exc_info=True)
        return pd.DataFrame()


if __name__ == "__main__":
    root_logger_main = logging.getLogger()
    for handler in root_logger_main.handlers[:]:
        root_logger_main.removeHandler(handler)
        handler.close()
    
    test_log_dir_main = Path(__file__).parent
    test_log_file_main = test_log_dir_main / "supabase_downloader_direct_run.log"
    test_log_dir_main.mkdir(parents=True, exist_ok=True)

    file_h_main = logging.FileHandler(test_log_file_main, mode='w', encoding='utf-8')
    file_h_main.setLevel(logging.DEBUG)
    stream_h_main = logging.StreamHandler(sys.stdout)
    stream_h_main.setLevel(logging.INFO) 

    logging.basicConfig(
        level=logging.DEBUG, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[file_h_main, stream_h_main]
    )
    
    for lib_logger_name_main in ["hpack", "httpcore", "httpx", "supabase"]:
        lib_logger_main = logging.getLogger(lib_logger_name_main)
        lib_logger_main.setLevel(logging.WARNING)
    
    script_main_logger = logging.getLogger(__name__)
    script_main_logger.info(f"--- Ejecutando supabase_downloader.py directamente ---")
    
    _load_env_and_rules() # Carga .env y también el CSV de reglas
    if not (SUPABASE_URL and SUPABASE_KEY):
        script_main_logger.error("Variables SUPABASE_URL o SUPABASE_KEY no cargadas. Verifica .env")
        sys.exit(1)
    else: script_main_logger.info("Variables de Supabase cargadas.")
    
    if LOADED_MODEL_RULES is not None and not LOADED_MODEL_RULES.empty:
        script_main_logger.info(f"Archivo de reglas '{RULES_CSV_PATH.name}' cargado con {len(LOADED_MODEL_RULES)} reglas.")
    elif LOADED_MODEL_RULES is not None:
        script_main_logger.warning(f"Archivo de reglas '{RULES_CSV_PATH.name}' está vacío o no se pudo parsear correctamente.")
    else:
         script_main_logger.warning(f"Archivo de reglas '{RULES_CSV_PATH.name}' no encontrado.")

    csv_output_path_main_script = Path(__file__).resolve().parents[1] / "data" / "diario" / "diario_estandarizado.csv"
    script_main_logger.info(f"Ruta de salida para el CSV procesado: {csv_output_path_main_script}")

    report_html_path_main_script = Path(__file__).resolve().parents[1] / "data" / "diario" / "reporte_diagnostico_make_model_diario.html"
    script_main_logger.info(f"Ruta para el reporte de diagnóstico HTML: {report_html_path_main_script}")
    
    report_txt_path_main_script = Path(__file__).resolve().parents[1] / "data" / "diario" / "reporte_diagnostico_make_model_diario.txt"
    script_main_logger.info(f"Ruta para el reporte de diagnóstico TXT: {report_txt_path_main_script}")

    try:
        df_procesado_para_main_script = descargar_y_cargar_datos(
            csv_output_path_main_script, 
            generate_raw_html_report=True, 
            raw_html_report_path=report_html_path_main_script,
            generate_raw_txt_report=True, 
            raw_txt_report_path=report_txt_path_main_script
        )

        if not df_procesado_para_main_script.empty:
            script_main_logger.info(f"Datos descargados y procesados (para main.py) exitosamente. Total filas: {len(df_procesado_para_main_script)}")
            info_buffer_main_script = io.StringIO()
            df_procesado_para_main_script.info(verbose=True, buf=info_buffer_main_script)
            script_main_logger.debug(f"DataFrame Info (Procesado para main.py):\n{info_buffer_main_script.getvalue()}")
            if {'Make', 'Model', 'Model_Base'}.issubset(df_procesado_para_main_script.columns):
                script_main_logger.info(f"Primeras filas del DF PROCESADO (devuelto a main.py) (Make, Model específico, Model_Base):\n{df_procesado_para_main_script[['Make', 'Model', 'Model_Base']].head().to_string()}")
            else:
                script_main_logger.warning("Una o más columnas ('Make', 'Model', 'Model_Base') no están en el DF procesado final.")
        else:
            script_main_logger.warning(f"El DataFrame procesado para main.py está vacío.")
            if os.path.exists(csv_output_path_main_script):
                 script_main_logger.info(f"Se ha creado un archivo CSV (posiblemente vacío o con errores) en: {csv_output_path_main_script}")
    except Exception as e_main_script:
        script_main_logger.error(f"Error durante la ejecución directa de supabase_downloader.py: {e_main_script}", exc_info=True)

    script_main_logger.info(f"--- Fin de la ejecución directa de supabase_downloader.py ---")
    script_main_logger.info(f"Revisa el reporte de diagnóstico HTML en: {report_html_path_main_script.resolve()}")
    script_main_logger.info(f"Revisa el reporte de diagnóstico TXT en: {report_txt_path_main_script.resolve()}")
    script_main_logger.info(f"El CSV con datos procesados para main.py está en: {csv_output_path_main_script.resolve()}")