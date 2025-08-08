
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
import logging
import random
from supabase import create_client, Client
from typing import List, Dict, Optional
import sys
from logging import StreamHandler
import os
from dotenv import load_dotenv
from datetime import datetime
import csv
import json # NUEVO: Importar módulo json para cargar el archivo de normalización
from pathlib import Path # NUEVO: Importar Path para manejo de rutas de archivos


# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('car_urls.log', encoding='utf-8'),
        StreamHandler(sys.stdout)
    ]
)


# --- Cargar variables de entorno ---
load_dotenv()


class CarUrlsGenerator:
    """Clase para generar URLs de autos desde NeoAuto y guardar en Supabase."""

    # BORRAR/MODIFICAR: Ya no se define BRAND_NORMALIZATION directamente aquí.
    # Se cargará dinámicamente desde un archivo JSON.
    # BRAND_NORMALIZATION = {
    #     'mercedes benz': 'mercedes',
    #     'mercedes-benz': 'mercedes',
    #     'bmw': 'bmw',
    #     'toyota': 'toyota',
    #     'volkswagen': 'volkswagen',
    #     'nissan': 'nissan',
    #     'hyundai': 'hyundai',
    #     'subaru': 'subaru',
    #     'mazda': 'mazda',
    #     'ford': 'ford',
    #     'kia': 'kia',
    #     'jeep': 'jeep',
    #     'audi': 'audi',
    #     'honda': 'honda',
    #     'chevrolet': 'chevrolet',
    #     'mitsubishi': 'mitsubishi',
    #     'suzuki': 'suzuki',
    #     'volvo': 'volvo'
    # }

    def __init__(self):
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        if not supabase_url or not supabase_key:
            logging.error("Error: SUPABASE_URL o SUPABASE_KEY no encontrados en el archivo .env")
            self.supabase: Optional[Client] = None
            logging.critical(" Cliente Supabase no inicializado debido a credenciales faltantes.")
        else:
            try:
                self.supabase = create_client(supabase_url, supabase_key)
                # Opcional: Prueba simple de conexión al construir la clase
                try:
                    test_response = self.supabase.table('conteo_marcas').select('id').limit(1).execute()

                    if test_response is not None and hasattr(test_response, 'error') and test_response.error is None:
                        logging.info("Conexión a Supabase exitosa en CarUrlsGenerator.")
                    else:
                        error_message = test_response.error if hasattr(test_response, 'error') else "Respuesta inesperada"
                        # MODIFICAR: Eliminar exc_info del warning de prueba simple si no es un error fatal
                        logging.warning(f"Conexión a Supabase exitosa, pero la prueba simple en conteo_marcas falló o retornó inesperado: {error_message}. Detalles: {test_response}")

                except Exception as e:
                    logging.warning(f"Error general al realizar prueba de conexión simple en conteo_marcas: {e}", exc_info=True)

            except Exception as e:
                logging.critical(f"Error al crear cliente Supabase en CarUrlsGenerator: {e}", exc_info=True)
                self.supabase = None

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        # NUEVO: Cargar el diccionario de normalización al inicializar la clase
        self.BRAND_NORMALIZATION = self._load_brand_normalization_data()


    # NUEVO: Método para cargar el diccionario de normalización desde un archivo JSON
    def _load_brand_normalization_data(self) -> Dict[str, str]:
        """Carga el diccionario de normalización de marcas desde un archivo JSON."""
        script_dir = Path(__file__).parent # Obtiene el directorio del script actual
        json_file_path = script_dir / 'marcas_y_sinonimos.json'
        
        if not json_file_path.exists():
            logging.error(f" Error: El archivo de normalización de marcas no se encontró en: {json_file_path}")
            # Retorna un diccionario vacío para evitar fallos, pero el proceso principal debería abortar si esto es crítico.
            return {}
        
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            normalized_data = {}
            for key, value in raw_data.items():
                if isinstance(value, list):
                    # MODIFICACIÓN CLAVE: Manejar el caso de que el valor sea una lista.
                    # Esto fue lo que causó el TypeError. Se toma el primer elemento.
                    logging.warning(f" Valor de normalización para '{key}' es una lista: {value}. Se usará el primer elemento: '{value[0]}'. Considera corregir el JSON a un string simple.")
                    normalized_data[key.lower().strip()] = str(value[0]).lower().strip() if value else key.lower().strip()
                elif isinstance(value, str):
                    normalized_data[key.lower().strip()] = value.lower().strip()
                else:
                    logging.warning(f" Valor de normalización inesperado para '{key}': {value} (tipo: {type(value)}). Se convertirá a string.")
                    normalized_data[key.lower().strip()] = str(value).lower().strip()
                    
            logging.info(f"Diccionario de normalización de marcas cargado exitosamente desde {json_file_path}")
            return normalized_data
        except json.JSONDecodeError as e:
            logging.error(f" Error al decodificar el JSON de marcas y sinónimos en {json_file_path}: {e}")
            return {}
        except Exception as e:
            logging.error(f" Error inesperado al cargar el diccionario de normalización desde {json_file_path}: {e}", exc_info=True)
            return {}


    def normalize_brand_name(self, raw_name: str) -> str:
        """Normaliza el nombre de la marca para consistencia."""
        if not raw_name:
            return ""

        # MODIFICAR: Asegurarse de que el valor obtenido del diccionario sea un string antes de strip/lower
        normalized = self.BRAND_NORMALIZATION.get(raw_name.lower().strip())
        if normalized is None:
            return raw_name.lower().strip() # Si no se encuentra, retorna el nombre original en minúsculas
        return str(normalized).lower().strip() # Asegurar que siempre sea un string


    def get_brands_from_supabase(self) -> List[Dict]:
        """Obtiene marcas y conteos desde Supabase."""
        if not self.supabase:
            logging.error("No se puede obtener marcas: Cliente Supabase no inicializado o falló la conexión.")
            return []

        try:
            logging.info("Obteniendo marcas desde la tabla 'conteo_marcas'...")
            
            # MODIFICACIÓN CLAVE: Asegurarse de que los valores sean strings únicos y válidos
            # El error "unhashable type: 'list'" ocurría si BRAND_NORMALIZATION.values() contenía listas.
            # La función _load_brand_normalization_data() ahora se encarga de que sean strings.
            # Si el JSON fue modificado externamente para tener listas, el _load_brand_normalization_data() ya lo manejaría.
            valid_normalized_brands = list(set(self.BRAND_NORMALIZATION.values()))
            
            response = self.supabase.table('conteo_marcas').select('marca, cantidad_autos').in_('marca', valid_normalized_brands).execute()

            if response is None or not hasattr(response, 'data') or not isinstance(response.data, list):
                logging.warning("La respuesta de Supabase al obtener marcas de conteo_marcas no contiene datos válidos o el resultado está vacío.")
                return []

            brands_data = []
            for row in response.data:
                if isinstance(row, dict) and row.get('marca') and isinstance(row.get('cantidad_autos'), int):
                    brands_data.append(row)
                else:
                    logging.warning(f"Fila con datos de marca inválidos o incompletos en conteo_marcas: {row}")

            logging.info(f"Obtenidas {len(brands_data)} marcas VÁLIDAS desde 'conteo_marcas'.")
            return brands_data

        except Exception as e:
            logging.error(f"Error obteniendo marcas de Supabase: {str(e)}", exc_info=True)
            return []

    def generate_search_urls(self, brand_data: dict) -> list:
        """Genera URLs de búsqueda para una marca."""
        normalized_brand = brand_data.get('marca', '')
        cantidad_autos = brand_data.get('cantidad_autos', 0)

        if not normalized_brand or cantidad_autos <= 0:
            logging.warning(f"Datos de marca o cantidad inválidos para generar URLs: {brand_data}. Saltando.")
            return []

        # NeoAuto muestra 20 autos por página. Ajusta si esto cambia.
        num_pages = (cantidad_autos + 19) // 20
        base_url = f"https://neoauto.com/venta-de-autos-usados-{normalized_brand}"
        # Ordenar por fecha de publicación descendente (el más nuevo primero)
        search_urls = [f"{base_url}?ord_publication_date=1&page={page}" for page in range(1, num_pages + 1)]

        return search_urls


    def extract_car_urls(self, search_url: str, brand: str) -> list:
        """Extrae URLs de autos individuales desde una URL de búsqueda."""
        try:
            logging.debug(f"Fetching search page: {search_url[:80]}...")
            response = self.session.get(search_url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            car_urls = []
            # Patrón para identificar URLs de listados individuales
            pattern = re.compile(r'auto/usado/[^"]+')

            # Buscar enlaces de resultados
            for link in soup.find_all('a', class_='c-results__link'):
                if href := link.get('href'):
                    if match := pattern.search(href):
                        full_url = f"https://neoauto.com/{match.group()}" if not href.startswith('http') else href
                        car_urls.append({
                            'marca': brand,
                            'url': full_url,
                            'procesado': False,
                            'fecha_extraccion': datetime.now().isoformat()
                        })

            page_num_match = re.search(r'page=(\d+)', search_url)
            page_number = page_num_match.group(1) if page_num_match else 'N/A'
            logging.info(f"Página {page_number}: Extraídas {len(car_urls)} URLs de listado para {brand.upper()}.")

            logging.debug(f"Extraídas {len(car_urls)} URLs de listado desde {search_url[:80]}...")
            return car_urls

        except requests.exceptions.RequestException as e:
            logging.error(f"Error de petición al acceder a {search_url}: {str(e)}", exc_info=True)
            return []
        except Exception as e:
            logging.error(f"Error inesperado al extraer URLs de {search_url}: {str(e)}", exc_info=True)
            return []


    def clear_urls_autos_table(self) -> bool:
        """Limpia (elimina todos los registros) de la tabla urls_autos."""
        table_name = 'urls_autos'
        if not self.supabase:
            logging.error(f"No se puede limpiar la tabla '{table_name}': Cliente Supabase no inicializado.")
            return False

        logging.info(f"Limpiando la tabla '{table_name}' completamente antes de poblarla...")
        try:
            response = self.supabase.table(table_name).delete().neq('id', 0).execute()

            if hasattr(response, 'error') and response.error:
                logging.error(f" Error al limpiar la tabla '{table_name}': {response.error}")
                return False
            else:
                logging.info(f" Tabla '{table_name}' limpiada exitosamente.")
                return True

        except Exception as e:
            logging.error(f" Excepción inesperada al intentar limpiar la tabla '{table_name}': {str(e)}", exc_info=True)
            return False


    def save_to_supabase(self, car_urls_batch: list) -> bool:
        """Guarda un lote de URLs de autos en Supabase."""
        if not car_urls_batch:
            logging.info("No hay URLs en este lote para guardar en urls_autos.")
            return True

        if not self.supabase:
            logging.error("No se puede guardar URLs: Cliente Supabase no inicializado.")
            return False

        try:
            logging.info(f"Intentando insertar {len(car_urls_batch)} URLs en la tabla 'urls_autos'...")
            response = self.supabase.table('urls_autos').insert(car_urls_batch, returning='minimal').execute()

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
                logging.info(f" Insertadas {len(car_urls_batch)} URLs en 'urls_autos'.")
                return True
            else:
                error_message = ""
                if hasattr(response, 'error') and response.error is not None:
                    error_message = str(response.error)
                elif response is None:
                    error_message = "Respuesta de Supabase fue None."
                else:
                    error_message = f"Respuesta inesperada del cliente Supabase (no se pudo determinar el error): {response}"

                logging.warning(f" Error al insertar lote de URLs. Detalles del error: {error_message}", exc_info=True)
                return False

        except Exception as e:
            logging.error(f"Error inesperado al guardar URLs en Supabase: {str(e)}", exc_info=True)
            return False

    def save_urls_to_csv(self, urls_data: List[Dict], filename: str):
        """Guarda una lista de diccionarios de URLs en un archivo CSV."""
        if not urls_data:
            logging.info(f"No hay URLs para guardar en '{filename}'.")
            return

        logging.info(f"Guardando {len(urls_data)} URLs en el archivo CSV: '{filename}'...")
        # --- NUEVA LÍNEA PARA IMPRIMIR LA RUTA COMPLETA ---
        full_path = os.path.abspath(filename)
        logging.info(f"Ruta completa de guardado intentada: {full_path}")
        # ----------------------------------------------------
        if not urls_data:
            logging.warning(f"No hay datos para guardar en el CSV '{filename}'.")
            return

        all_possible_keys = set()
        for d in urls_data:
            all_possible_keys.update(d.keys())

        desired_order = ['marca', 'url', 'procesado', 'fecha_extraccion']
        fieldnames = []
        for key in desired_order:
            if key in all_possible_keys:
                fieldnames.append(key)
                all_possible_keys.discard(key)

        fieldnames.extend(sorted(list(all_possible_keys)))


        try:
            # USAR full_path AQUÍ para la apertura del archivo
            with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(urls_data)
            logging.info(f" URLs guardadas exitosamente en '{filename}'.")
        except Exception as e:
            logging.error(f" Error al guardar URLs en CSV '{filename}': {str(e)}", exc_info=True)


    def process_brand(self, brand_data: dict) -> List[Dict]:
        """Procesa una marca completa: genera URLs de búsqueda y extrae URLs de listado."""
        normalized_brand = brand_data.get('marca', '')
        cantidad_autos = brand_data.get('cantidad_autos', 0)

        if not normalized_brand or cantidad_autos <= 0:
            logging.warning(f"Saltando procesamiento de marca con datos inválidos o sin autos: {brand_data}")
            return []

        search_urls = self.generate_search_urls({'marca': normalized_brand, 'cantidad_autos': cantidad_autos})
        if not search_urls:
            logging.warning(f"No se generaron URLs de búsqueda válidas para la marca '{normalized_brand}'.")
            return []

        all_car_urls_for_brand = []
        for i, search_url in enumerate(search_urls):
            logging.info(f"Extrayendo URLs de la página {i+1}/{len(search_urls)} para {normalized_brand.upper()}: {search_url[:80]}...")
            car_urls_from_page = self.extract_car_urls(search_url, normalized_brand)
            all_car_urls_for_brand.extend(car_urls_from_page)
            time.sleep(random.uniform(1, 3)) # Pausa para evitar saturar el servidor

        logging.info(f"Para la marca {normalized_brand.upper()}: Total de URLs extraídas de todas sus páginas (incluyendo duplicados) ANTES de guardar: {len(all_car_urls_for_brand)}")

        return all_car_urls_for_brand


    def run(self) -> bool:
        """Ejecuta el proceso completo de obtención y guardado de URLs, marca por marca."""
        logging.info(" Iniciando proceso de obtención y guardado de URLs de autos de NeoAuto (Marca por Marca) ")

        if not self.supabase:
            logging.critical(" Cliente Supabase no inicializado. No se puede continuar.")
            return False
            
        if not self.BRAND_NORMALIZATION: # NUEVO: Verificar si la carga del JSON falló
            logging.critical(" No se pudo cargar el diccionario de normalización de marcas. Terminando.")
            return False

        # 1. Limpiar urls_autos COMPLETAMENTE al inicio de la corrida
        if not self.clear_urls_autos_table():
            logging.critical(" No se pudo limpiar la tabla de destino 'urls_autos'. Terminando.")
            return False

        # 2. Obtener datos de marcas desde conteo_marcas
        brands_data = self.get_brands_from_supabase()
        if not brands_data:
            logging.error("No se obtuvieron marcas desde Supabase. Asegúrate de que 'extractor.py' corrió correctamente y pobló 'conteo_marcas'.")
            logging.info(" Proceso de obtención de URLs finalizado (no hay marcas para procesar).")
            return True

        logging.info(f"Procesando y guardando URLs marca por marca para {len(brands_data)} marcas válidas...")

        total_urls_extracted_across_all_brands = 0
        total_urls_inserted_across_all_brands = 0

        for brand_data in brands_data:
            normalized_brand = brand_data.get('marca', 'N/A').upper()
            cantidad_autos = brand_data.get('cantidad_autos', 0)
            logging.info(f"\n--- Procesando marca: {normalized_brand} ({cantidad_autos} autos) ---")

            urls_for_current_brand_raw = self.process_brand(brand_data)
            total_urls_extracted_across_all_brands += len(urls_for_current_brand_raw)

            if not urls_for_current_brand_raw:
                logging.warning(f"No se encontraron URLs para la marca {normalized_brand} en sus páginas de búsqueda. Saltando guardado para esta marca.")
                continue

            # --- GUARDAR CSV PARA LA MARCA ANTES DE SUPABASE ---
            csv_filename = f"{normalized_brand.lower()}_extracted_urls.csv"
            self.save_urls_to_csv(urls_for_current_brand_raw, csv_filename)
            # --------------------------------------------------------

            urls_to_save = urls_for_current_brand_raw

            logging.info(f"Marca {normalized_brand}: Preparando {len(urls_to_save)} URLs (incluyendo posibles duplicados) para guardar en Supabase.")

            if self.save_to_supabase(urls_to_save):
                total_urls_inserted_across_all_brands += len(urls_to_save)
            else:
                logging.error(f" Falló el guardado del lote de URLs para la marca '{normalized_brand}'.")

            # Opcional: Pausa entre procesar y guardar cada marca
            # time.sleep(random.uniform(3, 8))


        # 4. Resumen final
        logging.info(f"\n--- Proceso de obtención y guardado de URLs (Marca por Marca) finalizado ---")
        logging.info(f"Resumen Total:")
        logging.info(f"   URLs extraídas (totales en CSV individuales): {total_urls_extracted_across_all_brands}")
        logging.info(f"   URLs intentadas insertar exitosamente en 'urls_autos': {total_urls_inserted_across_all_brands}")

        if brands_data and total_urls_inserted_across_all_brands == 0 and total_urls_extracted_across_all_brands > 0:
            logging.warning(" Se procesaron marcas y extrajeron URLs, pero ninguna URL pudo ser insertada en 'urls_autos'. Revisa los logs de errores. (Esto no debería ocurrir si la restricción UNIQUE fue eliminada).")
            return False
        elif not brands_data:
            logging.info(" Proceso de obtención de URLs finalizado (no hay marcas para procesar).")
            return True

        logging.info(" Proceso de obtención y guardado de URLs finalizado.")
        return True


if __name__ == "__main__":
    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_KEY"):
        logging.critical(" No se pueden obtener credenciales o conectar a Supabase. Terminando.")
        sys.exit(1)


    generator = CarUrlsGenerator()

    if not hasattr(generator, 'supabase') or generator.supabase is None:
        logging.critical(" Cliente Supabase no disponible después de la inicialización. Terminando.")
        sys.exit(1)
        
    if not generator.BRAND_NORMALIZATION: # NUEVO: Abortar si la carga del JSON falló
        logging.critical(" El diccionario de normalización de marcas no se cargó correctamente. Terminando.")
        sys.exit(1)


    try:
        if generator.run():
            try:
                logging.info("\n Resumen final de URLs en 'urls_autos' por marca en Supabase:")
                if hasattr(generator, 'supabase') and generator.supabase:
                    # MODIFICAR: Cambiar la forma de obtener el conteo total para evitar el error PGRST123
                    # Se consulta un 'id' simple para verificar si hay registros y obtener el conteo exacto.
                    count_check_response = generator.supabase.table('urls_autos').select('id', count='exact').limit(0).execute() # limit(0) para no traer datos, solo el count
                    
                    if count_check_response and count_check_response.count is not None and count_check_response.count > 0:
                        logging.info(f"Total de URLs en la tabla 'urls_autos': {count_check_response.count}")
                        
                        # Fetch all 'marca' entries
                        all_marcas_response = generator.supabase.table('urls_autos').select('marca').execute()
                        
                        if all_marcas_response and hasattr(all_marcas_response, 'data') and isinstance(all_marcas_response.data, list):
                            marcas_df = pd.DataFrame(all_marcas_response.data)
                            if not marcas_df.empty:
                                brand_counts = marcas_df['marca'].value_counts().reset_index()
                                brand_counts.columns = ['marca', 'count']
                                brand_counts = brand_counts.sort_values(by='count', ascending=False)
                                
                                for index, row in brand_counts.iterrows():
                                    logging.info(f"   {row['marca']}: {row['count']} URLs")
                            else:
                                logging.info("No se encontraron marcas para generar el resumen.")
                        else:
                            logging.warning("No se pudieron obtener los datos de las marcas para el resumen final.")
                    else:
                        logging.info("La tabla 'urls_autos' está vacía después de la ejecución.")

                else:
                    logging.warning("Cliente Supabase no disponible para mostrar resumen final de Supabase.")

            except Exception as e:
                logging.error(f"Error mostrando resumen final de urls_autos desde Supabase: {str(e)}", exc_info=True)
            sys.exit(0)
        else:
            logging.error("El proceso de obtención y guardado de URLs terminó con errores.")
            sys.exit(1)

    except KeyboardInterrupt:
        logging.warning(" Proceso de obtención y guardado de URLs detenido manualmente. Los CSVs de las marcas ya procesadas deberían estar disponibles.")
        sys.exit(0)
    except Exception as e:
        logging.critical(f" Error fatal no manejado en el main de car_urls_module: {str(e)}", exc_info=True)
        sys.exit(1)