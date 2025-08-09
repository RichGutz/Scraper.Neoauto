# -*- coding: utf-8 -*-
"""
Extractor de Datos de Vehículos para App de Autos Usados (Versión de Procesamiento en Serie).
"""
import json
import re
import os
import unicodedata
import time
import argparse
import math
from typing import Dict, Optional, List, Any

# --- INICIO DE LA SECCIÓN DE CÓDIGO FINAL ---

# 1. Base de Conocimiento Definitiva
DISTRITOS_LIMA_OFICIALES = sorted([
    "Ancón", "Ate", "Barranco", "Breña", "Carabayllo", "Chaclacayo", "Chorrillos",
    "Cieneguilla", "Comas", "El Agustino", "Independencia", "Jesús María", "La Molina",
    "La Victoria", "Lima", "Lince", "Los Olivos", "Lurigancho", "Lurín",
    "Magdalena del Mar", "Miraflores", "Pachacámac", "Pucusana", "Pueblo Libre",
    "Puente Piedra", "Punta Hermosa", "Punta Negra", "Rímac", "San Bartolo",
    "San Borja", "San Isidro", "San Juan de Lurigancho", "San Juan de Miraflores",
    "San Luis", "San Martín de Porres", "San Miguel", "Santa Anita",
    "Santa María del Mar", "Santa Rosa", "Santiago de Surco", "Surquillo",
    "Villa El Salvador", "Villa María del Triunfo"
], key=len, reverse=True) # Ordenado de más largo a más corto para evitar ambigüedades

def archivo_ya_procesado(nombre_archivo: str) -> bool:
    """Verifica si el archivo ya fue procesado (contiene '_procesado' en el nombre)."""
    return '_procesado' in nombre_archivo

def normalizar_texto(texto: str) -> str:
    """Normaliza un texto para búsqueda (quita acentos, convierte a minúsculas)."""
    if not isinstance(texto, str):
        return ""
    return unicodedata.normalize('NFD', texto.lower()).encode('ascii', 'ignore').decode('utf-8')

def extraer_ubicacion_final(texto_anuncio: str) -> Dict[str, Optional[str]]:
    """
    Busca y extrae el UBIGEO (distrito, provincia, departamento) de un texto de anuncio,
    priorizando ubicaciones clave y utilizando la presencia de saltos de línea como delimitadores contextuales.
    Esta es la versión optimizada V3.

    Parámetros:
    texto_anuncio (str): El contenido completo de un archivo .txt de NeoAuto.

    Retorna:
    dict: Un diccionario con 'distrito', 'provincia' y 'departamento'.
          Los valores serán None si no se encuentran.
    """
    ubicacion_encontrada = {
        'distrito': None,
        'provincia': None,
        'departamento': None # Default a None, se establecerá a 'Lima' si se encuentra
    }

    json_data = {}
    try:
        # Intentar extraer la parte JSON del texto para acceder a campos estructurados como 'titulo' y 'precio'
        json_start = texto_anuncio.find('{')
        json_end = texto_anuncio.rfind('}')
        if json_start != -1 and json_end != -1 and json_end > json_start:
            json_string = texto_anuncio[json_start : json_end + 1]
            json_data = json.loads(json_string)
    except json.JSONDecodeError:
        # Si falla el parseo JSON, simplemente continuamos y buscaremos en el texto crudo
        pass

    # Definir las áreas de búsqueda en orden de prioridad
    search_areas = []

    # 1. Buscar en el contexto del 'titulo' (si existe y se pudo extraer)
    titulo_val = json_data.get('titulo', '')
    if titulo_val:
        # Buscar el título y capturar un fragmento de texto después de él (hasta 200 caracteres)
        # Se añade re.IGNORECASE para hacer la búsqueda del título insensible a mayúsculas/minúsculas
        match_titulo_context = re.search(re.escape(titulo_val) + r'([\s\S]{0,200})', texto_anuncio, re.IGNORECASE)
        if match_titulo_context:
            search_areas.append(match_titulo_context.group(0))

    # 2. Buscar en el contexto del 'precio' (si existe y se pudo extraer)
    precio_val = json_data.get('precio')
    if precio_val is not None:
        try:
            # Manejar casos donde precio_val es NaN, infinito o no numérico
            if isinstance(precio_val, (int, float)) and not math.isnan(precio_val) and not math.isinf(precio_val):
                precio_str_clean = str(int(precio_val)) # Solo el número entero para la búsqueda
                
                # Buscar el precio y capturar un fragmento de texto después de él (hasta 200 caracteres)
                match_precio_context = re.search(r'(?:Precio:)?(?:US\$)?\s*' + re.escape(precio_str_clean) + r'([\s\S]{0,200})', texto_anuncio, re.IGNORECASE)
                if match_precio_context:
                    search_areas.append(match_precio_context.group(0))
        except (ValueError, TypeError):
            pass

    # 3. Si el campo 'ubicacion' del JSON tiene contenido útil (no "Infinity")
    ubicacion_field_content = json_data.get('ubicacion', '')
    if ubicacion_field_content and "infinity" not in str(ubicacion_field_content).lower():
        search_areas.append(ubicacion_field_content)

    # 4. En el campo 'descripcion' del JSON
    descripcion_field_content = json_data.get('descripcion', '')
    if descripcion_field_content:
        search_areas.append(descripcion_field_content)

    # 5. Como último recurso, buscar en todo el texto del anuncio
    search_areas.append(texto_anuncio)

    # Iterar sobre las áreas de búsqueda en orden de prioridad
    for text_to_search_in in search_areas:
        if not text_to_search_in: # Saltar si el área de búsqueda está vacía
            continue

        # Usar la función normalizar_texto existente en el script principal
        normalized_text_to_search_in = normalizar_texto(text_to_search_in)
        
        # Dividir el texto en líneas para procesar cada línea que pueda contener UBIGEO
        lines = normalized_text_to_search_in.split('\n')

        for line in lines:
            # Paso 1: Buscar si la línea contiene "lima, lima" (o "lima, lima, lima") como ancla
            if re.search(r'\blima\b(?:,\s*\blima\b){1,2}', line):
                # Paso 2: Dentro de esta línea, intentar encontrar un distrito específico
                for distrito_oficial in DISTRITOS_LIMA_OFICIALES:
                    # Usar la función normalizar_texto existente
                    normalized_distrito = normalizar_texto(distrito_oficial)
                    
                    # Buscar el distrito seguido de "lima, lima" en la misma línea
                    if re.search(r'\b' + re.escape(normalized_distrito) + r'\b.*?\blima\b(?:,\s*\blima\b){1,2}', line):
                        ubicacion_encontrada['distrito'] = distrito_oficial
                        ubicacion_encontrada['provincia'] = 'Lima'
                        ubicacion_encontrada['departamento'] = 'Lima' 
                        print(f"UBIGEO encontrado (Distrito, Lima, Lima) mediante búsqueda por línea y contexto: {ubicacion_encontrada}")
                        return ubicacion_encontrada # Retornar al encontrar la mejor coincidencia

        # Fallback general: Si después de las búsquedas contextuales por línea no se encontró un distrito específico,
        # buscar "Lima, Lima" en todo el texto del área actual.
        match_city_department = re.search(r'\blima\b(?:,\s*\blima\b){1,2}', normalized_text_to_search_in)
        if match_city_department:
            # Solo asignamos si no hemos encontrado ya un distrito específico en esta ejecución
            if not ubicacion_encontrada['distrito']: 
                ubicacion_encontrada['distrito'] = 'Lima' # Por defecto a 'Lima' si solo se encuentra 'Lima, Lima'
                ubicacion_encontrada['provincia'] = 'Lima'
                ubicacion_encontrada['departamento'] = 'Lima' 
                print(f"Ubicación general 'Lima, Lima' encontrada como fallback, sin distrito específico. Asignado a 'Lima' distrito. {ubicacion_encontrada}")
                return ubicacion_encontrada

    print(f"No se encontró UBIGEO detallado en el texto.")
    # Asegurarse de que los valores predeterminados se devuelvan incluso si no se imprime nada.
    if ubicacion_encontrada['distrito'] is None:
        ubicacion_encontrada['provincia'] = None
        ubicacion_encontrada['departamento'] = None 
    return ubicacion_encontrada

# --- FIN DE LA SECCIÓN DE CÓDIGO FINAL ---

def archivo_ya_procesado(nombre_archivo: str) -> bool:
    """Verifica si el archivo ya fue procesado (contiene '_procesado' en el nombre)."""
    return '_procesado' in nombre_archivo

def normalizar_texto(texto: str) -> str:
    """Normaliza un texto para búsqueda (quita acentos, convierte a minúsculas)."""
    if not isinstance(texto, str):
        return ""
    return unicodedata.normalize('NFD', texto.lower()).encode('ascii', 'ignore').decode('utf-8')

def extraer_ubicacion_final(texto_anuncio: str) -> Dict[str, Optional[str]]:
    """
    Busca y extrae el UBIGEO (distrito, provincia, departamento) de un texto de anuncio,
    priorizando ubicaciones clave y utilizando la presencia de saltos de línea como delimitadores contextuales.
    Esta es la versión optimizada V3.

    Parámetros:
    texto_anuncio (str): El contenido completo de un archivo .txt de NeoAuto.

    Retorna:
    dict: Un diccionario con 'distrito', 'provincia' y 'departamento'.
          Los valores serán None si no se encuentran.
    """
    ubicacion_encontrada = {
        'distrito': None,
        'provincia': None,
        'departamento': None # Default a None, se establecerá a 'Lima' si se encuentra
    }

    json_data = {}
    try:
        # Intentar extraer la parte JSON del texto para acceder a campos estructurados como 'titulo' y 'precio'
        json_start = texto_anuncio.find('{')
        json_end = texto_anuncio.rfind('}')
        if json_start != -1 and json_end != -1 and json_end > json_start:
            json_string = texto_anuncio[json_start : json_end + 1]
            json_data = json.loads(json_string)
    except json.JSONDecodeError:
        # Si falla el parseo JSON, simplemente continuamos y buscaremos en el texto crudo
        pass

    # Definir las áreas de búsqueda en orden de prioridad
    search_areas = []

    # 1. Buscar en el contexto del 'titulo' (si existe y se pudo extraer)
    titulo_val = json_data.get('titulo', '')
    if titulo_val:
        # Buscar el título y capturar un fragmento de texto después de él (hasta 200 caracteres)
        # Se añade re.IGNORECASE para hacer la búsqueda del título insensible a mayúsculas/minúsculas
        match_titulo_context = re.search(re.escape(titulo_val) + r'([\s\S]{0,200})', texto_anuncio, re.IGNORECASE)
        if match_titulo_context:
            search_areas.append(match_titulo_context.group(0))

    # 2. Buscar en el contexto del 'precio' (si existe y se pudo extraer)
    precio_val = json_data.get('precio')
    if precio_val is not None:
        try:
            # Manejar casos donde precio_val es NaN, infinito o no numérico
            if isinstance(precio_val, (int, float)) and not math.isnan(precio_val) and not math.isinf(precio_val):
                precio_str_clean = str(int(precio_val)) # Solo el número entero para la búsqueda
                
                # Buscar el precio y capturar un fragmento de texto después de él (hasta 200 caracteres)
                match_precio_context = re.search(r'(?:Precio:)?(?:US\$)?\s*' + re.escape(precio_str_clean) + r'([\s\S]{0,200})', texto_anuncio, re.IGNORECASE)
                if match_precio_context:
                    search_areas.append(match_precio_context.group(0))
        except (ValueError, TypeError):
            pass

    # 3. Si el campo 'ubicacion' del JSON tiene contenido útil (no "Infinity")
    ubicacion_field_content = json_data.get('ubicacion', '')
    if ubicacion_field_content and "infinity" not in str(ubicacion_field_content).lower():
        search_areas.append(ubicacion_field_content)

    # 4. En el campo 'descripcion' del JSON
    descripcion_field_content = json_data.get('descripcion', '')
    if descripcion_field_content:
        search_areas.append(descripcion_field_content)

    # 5. Como último recurso, buscar en todo el texto del anuncio
    search_areas.append(texto_anuncio)

    # Iterar sobre las áreas de búsqueda en orden de prioridad
    for text_to_search_in in search_areas:
        if not text_to_search_in: # Saltar si el área de búsqueda está vacía
            continue

        # Usar la función normalizar_texto existente en el script principal
        normalized_text_to_search_in = normalizar_texto(text_to_search_in)
        
        # Dividir el texto en líneas para procesar cada línea que pueda contener UBIGEO
        lines = normalized_text_to_search_in.split('\n')

        for line in lines:
            # Paso 1: Buscar si la línea contiene "lima, lima" (o "lima, lima, lima") como ancla
            if re.search(r'\blima\b(?:,\s*\blima\b){1,2}', line):
                # Paso 2: Dentro de esta línea, intentar encontrar un distrito específico
                for distrito_oficial in DISTRITOS_LIMA_OFICIALES:
                    # Usar la función normalizar_texto existente
                    normalized_distrito = normalizar_texto(distrito_oficial)
                    
                    # Buscar el distrito seguido de "lima, lima" en la misma línea
                    if re.search(r'\b' + re.escape(normalized_distrito) + r'\b.*?\blima\b(?:,\s*\blima\b){1,2}', line):
                        ubicacion_encontrada['distrito'] = distrito_oficial
                        ubicacion_encontrada['provincia'] = 'Lima'
                        ubicacion_encontrada['departamento'] = 'Lima' 
                        print(f"UBIGEO encontrado (Distrito, Lima, Lima) mediante búsqueda por línea y contexto: {ubicacion_encontrada}")
                        return ubicacion_encontrada # Retornar al encontrar la mejor coincidencia

        # Fallback general: Si después de las búsquedas contextuales por línea no se encontró un distrito específico,
        # buscar "Lima, Lima" en todo el texto del área actual.
        match_city_department = re.search(r'\blima\b(?:,\s*\blima\b){1,2}', normalized_text_to_search_in)
        if match_city_department:
            # Solo asignamos si no hemos encontrado ya un distrito específico en esta ejecución
            if not ubicacion_encontrada['distrito']: 
                ubicacion_encontrada['distrito'] = 'Lima' # Por defecto a 'Lima' si solo se encuentra 'Lima, Lima'
                ubicacion_encontrada['provincia'] = 'Lima'
                ubicacion_encontrada['departamento'] = 'Lima' 
                print(f"Ubicación general 'Lima, Lima' encontrada como fallback, sin distrito específico. Asignado a 'Lima' distrito. {ubicacion_encontrada}")
                return ubicacion_encontrada

    print(f"No se encontró UBIGEO detallado en el texto.")
    # Asegurarse de que los valores predeterminados se devuelvan incluso si no se imprime nada.
    if ubicacion_encontrada['distrito'] is None:
        ubicacion_encontrada['provincia'] = None
        ubicacion_encontrada['departamento'] = None 
    return ubicacion_encontrada

# --- FIN DE LA SECCIÓN DE CÓDIGO FINAL ---

def cargar_frases_clave(ruta_archivo: str) -> List[str]:
    default_phrases = [ "unico dueño", "unica dueña", "primer dueño" ]
    if not os.path.exists(ruta_archivo): return default_phrases
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            datos = json.load(f)
        return datos.get('frases_clave', default_phrases)
    except (json.JSONDecodeError, IOError): return default_phrases

def es_unico_dueno(texto: str, frases_clave: List[str]) -> bool:
    texto_normalizado = normalizar_texto(texto)
    return any(normalizar_texto(frase) in texto_normalizado for frase in frases_clave)

def extraer_precio(texto: str) -> Optional[float]:
    patrones = [r'Precio\s*US\$\s*([\d,\.]+)', r'US\$\s*([\d,\.s]+)']
    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            try: return float(match.group(1).replace(',', '').replace('s', ''))
            except (ValueError, AttributeError): continue
    return None

def extraer_kilometraje(texto: str) -> Optional[int]:
    match = re.search(r'Kilometraje\s*([\d,]+)\s*km', texto, re.IGNORECASE | re.MULTILINE)
    if match:
        try: return int(match.group(1).replace(',', ''))
        except (ValueError, AttributeError): return None
    return None

def extraer_transmision(texto: str) -> Optional[str]:
    texto_procesado = texto.replace('\\n', '\n')
    match = re.search(r'^Transmisión\n(.*?)$', texto_procesado, re.IGNORECASE | re.MULTILINE)
    if match: return match.group(1).strip().capitalize()
    return None

def extraer_especificaciones(texto: str) -> Dict[str, str]:
    texto_procesado = texto.replace('\\n', '\n')
    especificaciones: Dict[str, str] = {}
    seccion_match = re.search(r'Especificaciones técnicas(.*?)Equipamiento', texto_procesado, re.DOTALL | re.IGNORECASE)
    if not seccion_match: return especificaciones
    contenido_seccion = seccion_match.group(1)
    claves_esperadas = ['Marca', 'Modelo', 'Año de fabricación', 'Número de puertas', 'Tracción', 'Color', 'Número cilindros', 'Placa', 'Combustible', 'Cilindrada', 'Categoría', 'Versión']
    lineas = [line.strip() for line in contenido_seccion.split('\n') if line.strip()]
    lineas_iter = iter(lineas)
    for linea in lineas_iter:
        if linea in claves_esperadas:
            try:
                valor = next(lineas_iter)
                especificaciones[linea] = valor
            except StopIteration: break
    return especificaciones

def leer_contenido_txt(ruta_archivo: str) -> str:
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        raise e

def generar_output_formato_estandar(datos_originales: Dict[str, Any], input_path: str) -> Dict[str, Any]:
    """
    Transforma los datos extraídos al formato estándar solicitado.
    No modifica ninguna función existente, solo reorganiza los datos.
    """
    dv = datos_originales['datos_vehiculo']
    especificaciones = dv.get('especificaciones', {})
    
    return {
        'DateTime': None,  # Se asignará automáticamente luego
        'URL': input_path,  # Usamos el path como placeholder para URL
        'Make': especificaciones.get('Marca', None),
        'Model': especificaciones.get('Modelo', None),
        'Price': dv.get('precio_usd', None),
        'Year': especificaciones.get('Año de fabricación', None),
        'Kilometers': dv.get('kilometraje_km', None),
        'Transmission': dv.get('transmision', None),
        'Fuel Type': especificaciones.get('Combustible', None),
        'Engine Size': especificaciones.get('Cilindrada', None),
        'Model Version': especificaciones.get('Versión', None),
        'District': dv['ubicacion'].get('distrito', None) if dv.get('ubicacion') else None,
        'Province': dv['ubicacion'].get('provincia', None) if dv.get('ubicacion') else None,
        'Department': dv['ubicacion'].get('departamento', None) if dv.get('ubicacion') else None,
        'id': None,  # Se asignará automáticamente luego
        'unico_dueno': dv.get('es_unico_dueno', False)
    }

def process_single_file(input_path: str, output_path: str, reglas_path: str):
    try:
        texto_anuncio = leer_contenido_txt(input_path)
        url_anuncio_extraida = None
        match_url = re.search(r'"url":\s*"(.*?)"', texto_anuncio)
        if match_url:
            url_anuncio_extraida = match_url.group(1)
        else:
            print(f"No se pudo extraer la URL del anuncio del TXT.")
    except Exception as e:
        print(f"Omitiendo archivo {os.path.basename(input_path)}. Causa: {e}")
        return

    datos_extraidos_originales = {
        'precio_usd': extraer_precio(texto_anuncio),
        'ubicacion': extraer_ubicacion_final(texto_anuncio),
        'kilometraje_km': extraer_kilometraje(texto_anuncio),
        'transmision': extraer_transmision(texto_anuncio),
        'es_unico_dueno': es_unico_dueno(texto_anuncio, cargar_frases_clave(reglas_path)),
        'especificaciones': extraer_especificaciones(texto_anuncio)
    }
    resultado_final_original = {
        'metadata': {'fuente_original': input_path, 'url_anuncio': url_anuncio_extraida,'fecha_extraccion': time.strftime("%Y-%m-%d %H:%M:%S"), 'version_script': 'FINAL_v4'},
        'datos_vehiculo': datos_extraidos_originales
    }
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(resultado_final_original, f, indent=4, ensure_ascii=False)
        print(f"Resultados guardados en: {output_path}")
        
        # Renombrar el archivo .txt de origen para marcarlo como procesado
        try:
            processed_txt_path = input_path.replace('.txt', '_procesado.txt')
            os.rename(input_path, processed_txt_path)
            print(f"Archivo de texto de origen renombrado a: {os.path.basename(processed_txt_path)}")
        except OSError as e:
            print(f"Error al renombrar el archivo {input_path}: {e}")

    except IOError as e:
        print(f"Error al guardar el archivo de salida: {e}")

def process_directory(input_dir: str, output_dir: str, reglas_path: str):
    """
    Busca y procesa en serie todos los archivos .txt en un directorio.
    """
    print(f"Buscando archivos .txt en la carpeta: '{input_dir}'")
    try:
        files_to_process = [f for f in os.listdir(input_dir) if f.endswith('.txt') and not archivo_ya_procesado(f)]
    except FileNotFoundError:
        print(f"El directorio de entrada '{input_dir}' no existe.")
        return
    if not files_to_process:
        print(f"No se encontraron archivos .txt nuevos para procesar en '{input_dir}'.")
        return
    file_count = len(files_to_process)
    print(f"Se encontraron {file_count} archivo(s) nuevos. Iniciando procesamiento...")
    os.makedirs(output_dir, exist_ok=True)
    for i, filename in enumerate(files_to_process):
        input_file_path = os.path.join(input_dir, filename)
        output_filename = os.path.splitext(filename)[0] + '.json'
        output_file_path = os.path.join(output_dir, output_filename)
        try:
            process_single_file(input_file_path, output_file_path, reglas_path)
        except Exception as e:
            print(f"Error procesando archivo {filename}: {str(e)}")
            continue
            
        if i < file_count - 1:
            print("\n" + "-" * 60)

if __name__ == "__main__":
    print(f"--- Extractor Automático de Datos de Vehículos (VERSIÓN FORENSE FINAL) ---")
    input_dir = "results_txt"  # Carpeta de entrada (TXT)
    output_dir = "results_json"  # Carpeta de salida (JSON)
    reglas_path = "reglas_unico_dueno.json"
    print("\n" + "="*50)
    print(f"Se consumirán datos de: {input_dir}")
    print(f"Los resultados se guardarán en: {output_dir}")
    print("="*50 + "\n")
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo crear la carpeta de salida: {e}")
    process_directory(input_dir, output_dir, reglas_path)