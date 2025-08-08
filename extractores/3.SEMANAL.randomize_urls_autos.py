import os
import sys
import logging
from dotenv import load_dotenv
from datetime import datetime
import time
import random
from supabase import create_client, Client
from typing import List, Dict, Optional
from logging import StreamHandler
import math # Importar math para calcular el número de lotes de inserción

# ----------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # Log para el script de preparación
        logging.FileHandler("url_preparation_script.log", encoding='utf-8'),
        StreamHandler(sys.stdout)
    ]
)

load_dotenv()

# Configura Supabase (URL/KEY desde .env)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Verificamos que las credenciales de Supabase estén cargadas
if not SUPABASE_URL or not SUPABASE_KEY:
    logging.error("Error: SUPABASE_URL o SUPABASE_KEY no encontrados en el archivo .env")
    sys.exit(1) # Salir si no se encuentran las credenciales

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    # Prueba simple de conexión
    try:
        # Prueba de conexión a una de las tablas que usaremos
        test_response = supabase.table('urls_autos').select('id', count='exact').limit(1).execute()
        if test_response is not None and hasattr(test_response, 'count') and test_response.count is not None:
            logging.info("Conexión a Supabase exitosa en randomize_urls_autos.")
        else:
            error_message = test_response.error if hasattr(test_response, 'error') else "Respuesta inesperada"
            if test_response.count is None and test_response.data == []:
                logging.info("Conexión a Supabase exitosa, tabla 'urls_autos' parece vacía pero accesible.")
            else:
                logging.warning(f"Conexión a Supabase exitosa, pero la prueba simple a urls_autos falló o retornó inesperado: {error_message}. Detalles: {test_response}")
    except Exception as e:
        logging.warning(f"Error al realizar prueba de conexión simple a urls_autos en Supabase: {e}")

except Exception as e:
    logging.critical(f"Error al crear cliente Supabase en randomize_urls_autos: {e}", exc_info=True)
    sys.exit(1)


# Constantes de configuración
SOURCE_TABLE = 'urls_autos'
DESTINATION_TABLE = 'urls_autos_random'
BATCH_SIZE = 1000 # Número de URLs a obtener en cada lote de la tabla de origen
INSERT_BATCH_SIZE = 500 # Número de URLs a insertar en cada lote en la tabla de destino
PAUSE_BETWEEN_FETCH_BATCHES_MS = 200 # Pausa entre cada obtención de lote de URLs (en milisegundos)
PAUSE_BETWEEN_INSERT_BATCHES_MS = 500 # Pausa entre cada inserción de lote de URLs (en milisegundos)

def clear_destination_table(table_name: str) -> bool:
    """Limpia (elimina todos los registros) de la tabla de destino."""
    logging.info(f"Limpiando la tabla '{table_name}'...")
    try:
        response = supabase.table(table_name).delete().neq('id', 0).execute()
        if hasattr(response, 'error') and response.error:
            logging.error(f"❌ Error al limpiar la tabla '{table_name}': {response.error}")
            return False
        else:
            logging.info(f"✅ Tabla '{table_name}' limpiada exitosamente.")
            return True
    except Exception as e:
        logging.error(f"❌ Excepción inesperada al intentar limpiar la tabla '{table_name}': {str(e)}", exc_info=True)
        return False

def get_all_urls_from_source(source_table: str) -> List[Dict]:
    """Obtiene todas las URLs y marcas de la tabla de origen en lotes."""
    logging.info(f"Obteniendo todas las URLs de la tabla '{source_table}' en lotes de {BATCH_SIZE}...")
    all_urls_data: List[Dict] = []
    offset = 0
    urls_with_null_marca = 0 # Contador para URLs con marca nula

    while True:
        try:
            # Obtener URLs y marcas de la tabla urls_autos
            response = supabase.table(source_table).select('url, marca').order('id').range(offset, offset + BATCH_SIZE - 1).execute()

            if response is None or not hasattr(response, 'data') or not isinstance(response.data, list):
                logging.warning(f"La respuesta de Supabase al obtener URLs del batch {offset} no contiene datos válidos o el resultado está vacío.")
                break # Salir del bucle si no hay datos válidos

            urls_batch = response.data
            if not urls_batch:
                logging.info(f"Batch {offset} de la tabla '{source_table}' vacío. No hay más URLs para obtener.")
                break # No hay más datos, salir del bucle

            # Filtrar URLs donde 'marca' es None (se mantiene por la restricción NOT NULL en 'marca')
            filtered_urls_batch = []
            for url_data in urls_batch:
                if isinstance(url_data, dict) and url_data.get('marca') is not None:
                    filtered_urls_batch.append(url_data)
                else:
                    urls_with_null_marca += 1
                    logging.warning(f"Ignorando URL con 'marca' nula o formato incorrecto: {url_data.get('url', 'URL Desconocida') if isinstance(url_data, dict) else str(url_data)}")

            all_urls_data.extend(filtered_urls_batch)
            logging.info(f"Obtenidos {len(filtered_urls_batch)} URLs válidas en el batch {offset}. Total obtenidos hasta ahora: {len(all_urls_data)}")
            if urls_with_null_marca > 0:
                logging.info(f"Total de URLs con 'marca' nula o formato incorrecto ignoradas hasta ahora: {urls_with_null_marca}")

            offset += BATCH_SIZE
            time.sleep(PAUSE_BETWEEN_FETCH_BATCHES_MS / 1000) # Pausa en segundos

        except Exception as e:
            logging.error(f"❌ Error al obtener URLs del batch {offset} de '{source_table}': {str(e)}", exc_info=True)
            break # Salir en caso de error

    logging.info(f"Finalizada la obtención de URLs. Total de URLs obtenidas de '{source_table}': {len(all_urls_data)}")
    if urls_with_null_marca > 0:
        logging.warning(f"ATENCIÓN: Se ignoraron {urls_with_null_marca} URLs debido a que tenían el valor 'marca' nulo o formato incorrecto. Esto podría indicar problemas en el script de carga de URLs.")
    return all_urls_data

def insert_urls_batched(table_name: str, urls_to_insert: List[Dict], batch_size: int, pause_ms: int) -> int:
    """Inserta URLs en lotes en la tabla de destino."""
    if not urls_to_insert:
        logging.info(f"No hay URLs para insertar en '{table_name}'.")
        return 0

    logging.info(f"Iniciando inserción de {len(urls_to_insert)} URLs en la tabla '{table_name}' en lotes de {batch_size}...")
    inserted_count = 0
    total_batches = math.ceil(len(urls_to_insert) / batch_size)

    for i in range(total_batches):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, len(urls_to_insert))
        batch_to_insert = urls_to_insert[start_index:end_index]

        logging.info(f"Insertando lote {i + 1}/{total_batches} ({len(batch_to_insert)} URLs) en '{table_name}'...")
        try:
            response = supabase.table(table_name).insert(batch_to_insert, returning='minimal').execute()

            is_successful = False
            if response is not None:
                if not hasattr(response, 'error'):
                    is_successful = True
                elif hasattr(response, 'error') and response.error is None:
                    is_successful = True
                else:
                    is_successful = False
            else:
                is_successful = False

            if is_successful:
                inserted_count += len(batch_to_insert)
                logging.info(f"✅ Lote {i + 1}/{total_batches} insertado exitosamente en '{table_name}'.")
            else:
                error_message = ""
                if hasattr(response, 'error') and response.error is not None:
                    error_message = str(response.error)
                elif response is None:
                    error_message = "Respuesta de Supabase fue None."
                else:
                    error_message = f"Respuesta inesperada del cliente Supabase (no se pudo determinar el error): {response}"
                logging.error(f"❌ Error al insertar lote {i + 1}/{total_batches} en '{table_name}'. Detalles: {error_message}", exc_info=True)

        except Exception as e:
            logging.error(f"❌ Excepción inesperada al insertar lote {i + 1}/{total_batches} en '{table_name}': {str(e)}", exc_info=True)

        time.sleep(pause_ms / 1000) # Pausa en segundos

    logging.info(f"Finalizada la inserción de URLs. Total de URLs insertadas exitosamente en '{table_name}': {inserted_count}/{len(urls_to_insert)}")
    return inserted_count


def main():
    logging.info("🏁 Iniciando script de preparación de URLs para procesamiento aleatorio 🏁")

    # 1. Limpiar la tabla de destino
    if not clear_destination_table(DESTINATION_TABLE):
        logging.critical(f"🛑 No se pudo limpiar la tabla de destino '{DESTINATION_TABLE}'. Terminando.")
        sys.exit(1)

    # 2. Obtener todas las URLs de la tabla de origen
    all_urls_data = get_all_urls_from_source(SOURCE_TABLE)
    logging.info(f"Obtenidas un total de {len(all_urls_data)} URLs de la tabla '{SOURCE_TABLE}'.")

    if not all_urls_data:
        logging.warning("No se obtuvieron URLs de la tabla de origen. El script finalizará.")
        sys.exit(0)

    # NO SE REALIZA DEDUPLICACIÓN AQUÍ, SE ASUME QUE LA BASE DE DATOS LA PERMITIRÁ
    # (¡ASEGÚRATE DE HABER ELIMINADO LA RESTRICCIÓN ÚNICA EN SUPABASE!)

    # 3. Mezclar aleatoriamente la lista de URLs
    logging.info("Mezclando aleatoriamente la lista de URLs...")
    random.shuffle(all_urls_data)
    logging.info("Lista de URLs mezclada.")

    # 4. Insertar las URLs mezcladas en la tabla de destino en lotes
    inserted_count = insert_urls_batched(DESTINATION_TABLE, all_urls_data, INSERT_BATCH_SIZE, PAUSE_BETWEEN_INSERT_BATCHES_MS)

    # 5. Verificar si la inserción fue completa
    if inserted_count == len(all_urls_data):
        logging.info(f"🎉 Script de preparación completado exitosamente. Se insertaron {inserted_count} URLs mezcladas en '{DESTINATION_TABLE}'.")
        sys.exit(0) # Salir con éxito
    else:
        logging.warning(f"⚠️ Script de preparación finalizado con advertencias. Se intentaron insertar {len(all_urls_data)} URLs, pero solo se confirmaron {inserted_count} inserciones exitosas en '{DESTINATION_TABLE}'. Revisa los logs de inserción.")
        sys.exit(1)


if __name__ == "__main__":
    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_KEY"):
        logging.critical("Error: SUPABASE_URL o SUPABASE_KEY no encontrados en el archivo .env. Terminando.")
        sys.exit(1)

    try:
        main()
    except KeyboardInterrupt:
        logging.warning("⏹️ Proceso de preparación de URLs detenido manualmente.")
        sys.exit(0)
    except Exception as e:
        logging.critical(f"💥 Error fatal no manejado en el main loop de preparación: {str(e)}", exc_info=True)
        sys.exit(1)