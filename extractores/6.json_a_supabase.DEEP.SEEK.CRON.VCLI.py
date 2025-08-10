"""
Importador de Datos JSON a Supabase.

Este script es el puente final entre los datos procesados localmente y la base
de datos central en la nube. Su función es tomar los archivos JSON estructurados
y cargarlos en la tabla de Supabase correspondiente.

Funcionalidad Principal:
1.  **Búsqueda de Archivos JSON**: Escanea el directorio `results_json` en busca
    de nuevos archivos .json que no hayan sido procesados (que no contengan
    `_procesado` en su nombre).

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
    inserción para evitar registros duplicados y mantener la integridad de los datos.

5.  **Inserción de Datos**: Si la validación es exitosa y no es un duplicado,
    inserta el nuevo registro en la tabla `autos_detalles_diarios`.

6.  **Marcado de Archivos Procesados**: Tras una inserción exitosa, renombra el
    archivo .json original, añadiéndole el sufijo `_procesado.json`. Esto
    previene que el mismo archivo sea procesado e insertado múltiples veces en
    futuras ejecuciones.

Este script asegura que solo datos válidos, enriquecidos y no duplicados sean
cargados a la base de datos, completando el ciclo de extracción y carga (ETL).
"""
import os
import json
import re
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import Dict, Any, Optional

# --- Configuración ---
load_dotenv()

SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: Las variables de entorno SUPABASE_URL o SUPABASE_KEY no están configuradas.")
    exit(1)

SUPABASE_TABLE_NAME: str = 'autos_detalles_diarios'
JSON_INPUT_FOLDER: str = 'results_json' # <-- FIX: Changed from 'extractores/results_json'

# --- Inicialización de Supabase Client ---
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Conexión con Supabase establecida exitosamente.")
except Exception as e:
    print(f"Error al conectar con Supabase: {e}")
    exit(1)

# --- Funciones de Procesamiento ---

def archivo_ya_procesado(nombre_archivo: str) -> bool:
    """Verifica si el archivo ya fue procesado (contiene '_procesado' en el nombre)."""
    return '_procesado' in nombre_archivo

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
    """Procesa un archivo JSON y lo inserta en Supabase."""
    filename = os.path.basename(filepath)
    print(f"Procesando archivo: {filename}")

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
    except Exception as e:
        print(f"Error al leer JSON en '{filename}': {e}")
        return

    data_to_insert = validar_y_extraer_datos(json_data, filename)
    if data_to_insert is None:
        return

    url_to__check = data_to_insert['URL']
    try:
        response = supabase.from_(SUPABASE_TABLE_NAME).select("URL").eq("URL", url_to_check).execute()
        
        if response.data:
            print(f"URL '{url_to_check}' ya existe en Supabase. Omitiendo.")
            return
    except Exception as e:
        print(f"Error al verificar URL duplicada en Supabase para '{filename}': {e}")
        return

    try:
        response = supabase.from_(SUPABASE_TABLE_NAME).insert(data_to_insert).execute()
        if response.data:
            print(f"Datos de '{filename}' insertados exitosamente.")
            
            # Renombrar archivo como procesado
            nuevo_nombre = filename.replace('.json', '_procesado.json')
            nuevo_path = os.path.join(os.path.dirname(filepath), nuevo_nombre)
            os.rename(filepath, nuevo_path)
            print(f"Archivo renombrado a: {nuevo_nombre}")
        else:
            print(f"La inserción de '{filename}' no devolvió datos. Respuesta: {response}")
    except Exception as e:
        print(f"Error al insertar datos de '{filename}' en Supabase: {e}")

# --- Flujo Principal ---
if __name__ == "__main__":
    if not os.path.exists(JSON_INPUT_FOLDER):
        print(f"Error: La carpeta de entrada '{JSON_INPUT_FOLDER}' no existe.")
        exit(1)

    # Filtrar solo archivos JSON no procesados
    json_files = [
        f for f in os.listdir(JSON_INPUT_FOLDER) 
        if f.endswith('.json') and not archivo_ya_procesado(f)
    ]

    if not json_files:
        print(f"No se encontraron archivos JSON nuevos en '{JSON_INPUT_FOLDER}'.")
    else:
        print(f"Se encontraron {len(json_files)} archivo(s) JSON nuevos. Iniciando importación...")
        for json_file in json_files:
            full_filepath = os.path.join(JSON_INPUT_FOLDER, json_file)
            importar_json_a_supabase(full_filepath)
        print("Proceso de importación finalizado.")
