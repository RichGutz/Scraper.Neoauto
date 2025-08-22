
# Documentación: `5.DIARIO.SEMANAL.Procesador_txt.a.json.DEEPSEEK_VCLI.py`

## Propósito

Procesador de Archivos TXT a Formato JSON Estructurado.

Este script actúa como un transformador de datos. Toma los archivos de texto (.txt) crudos generados por el scraper (`4.DIARIO...`) y los convierte en archivos JSON bien estructurados, extrayendo y limpiando la información relevante.

## Funcionalidad Principal

1.  **Búsqueda de Archivos**: Escanea el directorio `results_txt` en busca de nuevos archivos .txt que no hayan sido procesados previamente (es decir, que no contengan `_procesado` en su nombre).

2.  **Lectura de Contenido**: Lee el contenido completo de cada archivo .txt.

3.  **Extracción de Datos por Regex y Lógica**: Utiliza una combinación de expresiones regulares (regex) y lógica de análisis de texto para extraer campos específicos. Las funciones de extracción están diseñadas para ser resilientes a las variaciones en el formato del texto:
    - `extraer_precio`
    - `extraer_kilometraje`
    - `extraer_transmision`
    - `extraer_especificaciones`: Extrae un bloque de especificaciones técnicas.
    - `es_unico_dueno`: Determina si el anuncio menciona "único dueño" basándose en un archivo de reglas (`reglas_unico_dueno.json`).
    - `extraer_ubicacion_final`: Una función sofisticada que intenta localizar el distrito, provincia y departamento, buscando en áreas priorizadas del texto (cerca del título, del precio, etc.) para mayor precisión.

4.  **Estructuración de Datos**: Ensambla todos los datos extraídos en un diccionario de Python con una estructura anidada y limpia, incluyendo metadatos como la fuente original y la fecha de procesamiento.

5.  **Escritura de Archivos JSON**: Guarda el diccionario resultante como un archivo .json en la carpeta `results_json`. El nombre del archivo JSON se corresponde con el del archivo TXT original.

6.  **Marcado de Archivos Procesados**: Una vez que el archivo JSON se ha guardado con éxito, renombra el archivo .txt original añadiéndole el sufijo `_procesado.txt`. Esto asegura que cada archivo de texto se procese una sola vez.

Este script es un paso crucial para pasar de datos no estructurados (texto libre) a un formato estructurado (JSON) que puede ser fácilmente consultado y cargado en una base de datos.
