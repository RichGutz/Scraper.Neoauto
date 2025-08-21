"""
Importador de Datos JSON a Supabase.

Este script es el puente final entre los datos procesados localmente y la base
db de datos central en la nube. Su función es tomar los archivos JSON estructurados
y cargarlos en la tabla de Supabase correspondiente.

Funcionalidad Principal:
1.  **Búsqueda de Archivos JSON**: Escanea el directorio `results_json` en busca
    de nuevos archivos .json.

2.  **Conexión a Supabase**: Establece una conexión con el cliente de Supabase
    para poder realizar operaciones en la base de datos.

3.  **Validación y Mapeo de Datos**: Antes de la inserción, cada archivo JSON
    es validado para asegurar que contiene los campos mínimos requeridos (como
    URL y precio). Si faltan datos cruciales como Marca, Modelo o Año, intenta
    extraerlos de la propia URL del anuncio como un mecanismo de fallback.
    Luego, mapea los datos del JSON a la estructura de columnas de la tabla
    `autos_detalles_diarios` en Supabase.

4.  **Verificación de Duplicados**: Realiza una consulta a Supabase para verificar
    si la URL del anuncio ya existe en la tabla. Si ya existe, omite la
    inserción para evitar registros duplicados.

5.  **Inserción de Datos**: Si la validación es exitosa y no es un duplicado,
    inserta el nuevo registro en la tabla `autos_detalles_diarios`.

6.  **Movimiento de Archivos Procesados**: Tras el intento de procesamiento (exitoso o no),
    mueve el archivo .json a la subcarpeta `PROCESADO` para evitar que sea procesado
    nuevamente y mantener limpio el directorio de entrada.

Este script asegura que solo datos válidos, enriquecidos y no duplicados sean
cargados a la base de datos, completando el ciclo de extracción y carga (ETL).
"""
import os
import json
import re
import shutil
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import Dict, Any, Optional
from pathlib import Path

# --- Configuración ---
load_dotenv()

SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: Las variables de entorno SUPABASE_URL o SUPABASE_KEY no están configuradas.")
    exit(1)

SUPABASE_TABLE_NAME: str = 'autos_detalles_diarios'
SCRIPT_DIR = Path(__file__).resolve().parent
JSON_INPUT_FOLDER: Path = SCRIPT_DIR / 'results_json'
PROCESSED_FOLDER: Path = JSON_INPUT_FOLDER / 'PROCESADO'

# --- Inicialización de Supabase Client ---
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Conexión con Supabase establecida exitosamente.")
except Exception as e:
    print(f"Error al conectar con Supabase: {e}")
    exit(1)

# --- Funciones de Procesamiento ---

def extraer_datos_de_url(url: str) -> Dict[str, Optional[str]]:
    """Intenta extraer Marca, Modelo y Año de una URL de Neoauto."""
    datos_url = {'Marca': None, 'Modelo': None, 'Año de fabricación': None}
    
    match = re.search(r'/(?:seminuevo|usado)/([^/]+)-([^/]+)-(\d{4})-\d+', url)
    
    if match:
        datos_url['Marca'] = match.group(1).replace('-', ' ').title() if match.group(1) else None
        datos_url['Modelo'] = match.group(2).replace('-', ' ').title() if match.group(2) else None
        datos_url['Año de fabricación'] = match.group(3) if match.group(3) else None
        
        if datos_url['Modelo'] and 'auto' in datos_url['Modelo'].lower():
            datos_url['Modelo'] = None
        if datos_url['Marca'] and 'auto' in datos_url['Marca'].lower():
            datos_url['Marca'] = None

    return datos_url

def validar_y_extraer_datos(json_data: Dict[str, Any], filename: str) -> Optional[Dict[str, Any]]:
    """Valida y extrae los datos para Supabase."""
    metadata = json_data.get('metadata', {})
    datos_vehiculo = json_data.get('datos_vehiculo', {})
    especificaciones = datos_vehiculo.get('especificaciones', {})
    ubicacion = datos_vehiculo.get('ubicacion', {})

    url = metadata.get('url_anuncio')
    price = datos_vehiculo.get('precio_usd')

    if not all([url, price is not None]):
        print(f"Descartando '{filename}': Faltan campos obligatorios (URL o Precio).")
        return None

    make = especificaciones.get('Marca')
    model = especificaciones.get('Modelo')
    year = especificaciones.get('Año de fabricación')

    if not all([make, model, year]):
        datos_de_url = extraer_datos_de_url(url)
        if not make:
            make = datos_de_url['Marca']
        if not model:
            model = datos_de_url['Modelo']
        if not year:
            year = datos_de_url['Año de fabricación']
        
        if not all([make, model, year]):
            print(f"Descartando '{filename}': No se pudo obtener Marca, Modelo o Año.")
            return None

    mapped_data: Dict[str, Any] = {
        'DateTime': metadata.get('fecha_extraccion'),
        'URL': url,
        'Make': make,
        'Model': model,
        'Price': price,
        'Year': year,
        'Kilometers': datos_vehiculo.get('kilometraje_km'),
        'Transmission': datos_vehiculo.get('transmision'),
        'Fuel Type': especificaciones.get('Combustible'),
        'Engine Size': especificaciones.get('Cilindrada'),
        'Model Version': especificaciones.get('Versión'),
        'District': ubicacion.get('distrito'),
        'Province': ubicacion.get('provincia'),
        'Department': ubicacion.get('departamento'),
        'unico_dueno': datos_vehiculo.get('es_unico_dueno', False)
    }

    return mapped_data

def importar_json_a_supabase(filepath: str):
    """
    Procesa un archivo JSON, lo inserta en Supabase y finalmente lo mueve a la carpeta de procesados.
    """
    filename = os.path.basename(filepath)
    print(f"--- Iniciando procesamiento para: {filename} ---")

    try:
        # Paso 1: Leer y validar el archivo JSON
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
        except Exception as e:
            print(f"Error al leer o parsear JSON en '{filename}': {e}")
            return

        data_to_insert = validar_y_extraer_datos(json_data, filename)
        if data_to_insert is None:
            return

        # Paso 2: Verificar si la URL ya existe en Supabase
        url_to_check = data_to_insert['URL']
        try:
            response = supabase.from_(SUPABASE_TABLE_NAME).select("URL").eq("URL", url_to_check).execute()
            if response.data:
                print(f"URL '{url_to_check}' ya existe en Supabase. Omitiendo inserción.")
                return
        except Exception as e:
            print(f"Error al verificar URL duplicada en Supabase para '{filename}': {e}")
            return

        # Paso 3: Insertar los datos en Supabase
        try:
            response = supabase.from_(SUPABASE_TABLE_NAME).insert(data_to_insert).execute()
            if response.data:
                print(f"Datos de '{filename}' insertados exitosamente.")
            else:
                print(f"Inserción de '{filename}' completada (la API no devolvió datos, lo cual es normal).")
        except Exception as e:
            print(f"Error al insertar datos de '{filename}' en Supabase: {e}")

    finally:
        # Paso final: Mover el archivo a la carpeta de procesados
        try:
            destination_path = PROCESSED_FOLDER / filename
            shutil.move(str(filepath), str(destination_path))
            print(f"Archivo '{filename}' movido a '{destination_path}'")
        except Exception as e:
            print(f"¡CRÍTICO! Error al mover el archivo procesado '{filename}': {e}")
            print("El archivo podría ser procesado de nuevo en la siguiente ejecución.")
        print(f"--- Fin del procesamiento para: {filename} ---")

# --- Flujo Principal ---
if __name__ == "__main__":
    if not JSON_INPUT_FOLDER.exists():
        print(f"Error: La carpeta de entrada '{JSON_INPUT_FOLDER}' no existe.")
        exit(1)

    # Crear la carpeta de destino para los archivos procesados si no existe
    PROCESSED_FOLDER.mkdir(exist_ok=True)

    # Obtener la lista de archivos JSON a procesar, asegurándose de que sean archivos y no directorios
    json_files = [
        f for f in os.listdir(JSON_INPUT_FOLDER) 
        if f.endswith('.json') and os.path.isfile(os.path.join(JSON_INPUT_FOLDER, f))
    ]

    if not json_files:
        print(f"No se encontraron archivos JSON nuevos en '{JSON_INPUT_FOLDER}'.")
    else:
        print(f"Se encontraron {len(json_files)} archivo(s) JSON nuevos. Iniciando importación...")
        for json_file in json_files:
            full_filepath = os.path.join(JSON_INPUT_FOLDER, json_file)
            importar_json_a_supabase(full_filepath)
        print("Proceso de importación finalizado.")