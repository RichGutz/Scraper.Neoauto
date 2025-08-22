
# Documentación: `main_daily.py`

## Propósito

Orquestador Principal del Proceso Diario de Scraping y Análisis de NeoAuto.

Este script actúa como el punto de entrada para la ejecución diaria de todas las tareas relacionadas con la recopilación, procesamiento y análisis de datos de vehículos de segunda mano de NeoAuto.

## Secuencia de Ejecución

1.  **Lanzamiento de VPN**: Inicia ProtonVPN para asegurar una conexión anónima y evitar bloqueos de IP. Espera un tiempo prudencial para que la conexión se establezca.
2.  **Ejecución de Scripts Secuenciales**: Invoca una serie de scripts, cada uno responsable de una etapa específica del pipeline de datos. Si un script falla, el orquestador detiene la ejecución para evitar errores en cascada.

## Pipeline de Scripts

- **`2.DIARIO.daily_urls_extraction.VCLI.py`**: Extrae las URLs de los anuncios publicados "hoy" en NeoAuto y las guarda en Supabase.
- **`4.DIARIO.SEMANAL.SCRAPER.NEOAUTO.SUPABASE.PARA.CRON.py`**: Realiza el scraping detallado de las URLs obtenidas, guardando los resultados en archivos .txt.
- **`5.DIARIO.SEMANAL.Procesador_txt.a.json.DEEPSEEK_VCLI.py`**: Procesa los .txt, extrae la información estructurada y la convierte a formato .json.
- **`6.json_a_supabase.DEEP.SEEK.CRON.VCLI.py`**: Sube los datos de los archivos .json a la tabla principal de detalles de autos en Supabase.
- **`main.py`**: Ejecuta el análisis de mercado completo sobre los datos históricos, genera los reportes HTML (principal y por modelo) y filtra los leads atractivos del día.
- **`gmail_sender.py`**: Envía el reporte de leads atractivos por correo electrónico a los destinatarios configurados.

## Logging

El script utiliza un sistema de logging centralizado en `main_daily.log` para registrar el progreso y los errores de toda la operación.
