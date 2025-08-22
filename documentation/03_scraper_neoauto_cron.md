
# Documentación: `4.DIARIO.SEMANAL.SCRAPER.NEOAUTO.SUPABASE.PARA.CRON.py`

## Propósito

Scraper Detallado de Anuncios de Vehículos en NeoAuto.

Este script es el núcleo del proceso de extracción de datos. Utiliza Playwright para controlar un navegador headless, permitiendo una interacción avanzada con las páginas de anuncios de NeoAuto, que dependen en gran medida de JavaScript.

## Funcionalidad Principal

1.  **Obtención de URLs**: Consulta dos tablas en Supabase para obtener las URLs que necesita procesar:
    - `urls_autos_diarios`: URLs nuevas del día, marcadas como no procesadas.
    - `urls_autos_random`: Una selección de URLs históricas para re-scrapeo y seguimiento, priorizando las que no han sido visitadas recientemente.

2.  **Navegación Humanizada**: Emula un comportamiento de usuario para evitar ser detectado como un bot. Esto incluye:
    - Rotación de User-Agents.
    - Navegación a través de una serie de pasos lógicos (p. ej., página principal -> categoría -> anuncio) en lugar de acceder directamente a la URL.
    - Manejo robusto de pop-ups (cookies, suscripciones, encuestas).
    - Simulación de scroll para asegurar que todo el contenido dinámico de la página se cargue correctamente antes de la extracción.

3.  **Extracción de Datos Detallada**: Una vez en la página del anuncio, extrae información clave como:
    - Título, precio, kilometraje, ubicación, año, transmisión.
    - La descripción completa del anuncio.

4.  **Detección de "Único Dueño"**: Analiza la descripción del vehículo utilizando un conjunto de reglas predefinidas (`reglas_unico_dueno.json`) para determinar si el vendedor lo ha listado como de "único dueño".

5.  **Almacenamiento Local**: Guarda toda la información extraída de un anuncio en un archivo de texto (`.txt`) único en la carpeta `results_txt`. El nombre del archivo se genera con un hash de la URL y un timestamp para evitar colisiones.

6.  **Actualización de Estado en Supabase**: Tras procesar exitosamente una URL, actualiza su estado en la tabla correspondiente de Supabase para marcarla como `procesado: True` (en `urls_autos_diarios`) o actualizar su fecha de última visita (`last_scraped` en `urls_autos_random`).

Este script está diseñado para ser robusto y resiliente, manejando errores de red y particularidades del sitio web de NeoAuto.
