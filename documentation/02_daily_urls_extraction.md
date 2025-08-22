
# Documentación: `2.DIARIO.daily_urls_extraction.VCLI.py`

## Propósito

Extractor de URLs Diarias de NeoAuto.

Este script se especializa en raspar (scrape) la primera capa de información de NeoAuto: las URLs de los anuncios de vehículos usados publicados en el día.

## Funcionalidad Principal

1.  **Conexión a Supabase**: Se inicializa una conexión segura con la base de datos de Supabase para leer y escribir información.
2.  **Carga de Mapeo de Marcas**: Lee un archivo JSON (`marcas_y_sinonimos.json`) para estandarizar los nombres de las marcas de vehículos extraídas de las URLs.
3.  **Obtención de URLs Existentes**: Consulta la tabla `urls_autos_diarios` en Supabase para obtener un conjunto de todas las URLs ya registradas, con el fin de evitar la duplicación de datos.
4.  **Scraping de Páginas**: Navega a través de las páginas de resultados de NeoAuto que filtran por anuncios "publicado=hoy". Extrae el enlace (`href`) de cada anuncio.
5.  **Validación y Enriquecimiento**: Por cada URL nueva, extrae la marca, la valida contra el mapeo cargado y, si es válida, la prepara para ser insertada.
6.  **Inserción en Base de Datos**: Inserta el lote de URLs nuevas y validadas en la tabla `urls_autos_diarios` de Supabase, junto con la marca estandarizada, la fecha de extracción y un booleano `procesado` inicializado en `False`.

Este script está diseñado para ser el primer paso del pipeline diario, alimentando la cola de URLs que serán procesadas en detalle por scripts posteriores.
