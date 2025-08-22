
# Documentación: `6.json_a_supabase.DEEP.SEEK.CRON.VCLI.py`

## Propósito

Importador de Datos JSON a Supabase.

Este script es el puente final entre los datos procesados localmente y la base de datos central en la nube. Su función es tomar los archivos JSON estructurados y cargarlos en la tabla de Supabase correspondiente.

## Funcionalidad Principal

1.  **Búsqueda de Archivos JSON**: Escanea el directorio `results_json` en busca de nuevos archivos .json que no hayan sido procesados (que no contengan `_procesado` en su nombre).

2.  **Conexión a Supabase**: Establece una conexión con el cliente de Supabase para poder realizar operaciones en la base de datos.

3.  **Validación y Mapeo de Datos**: Antes de la inserción, cada archivo JSON es validado para asegurar que contiene los campos mínimos requeridos (como URL y precio). Si faltan datos cruciales como Marca, Modelo o Año, intenta extraerlos de la propia URL del anuncio como un mecanismo de fallback. Luego, mapea los datos del JSON a la estructura de columnas de la tabla `autos_detalles_diarios` en Supabase.

4.  **Verificación de Duplicados**: Realiza una consulta a Supabase para verificar si la URL del anuncio ya existe en la tabla. Si ya existe, omite la inserción para evitar registros duplicados y mantener la integridad de los datos.

5.  **Inserción de Datos**: Si la validación es exitosa y no es un duplicado, inserta el nuevo registro en la tabla `autos_detalles_diarios`.

6.  **Marcado de Archivos Procesados**: Tras una inserción exitosa, renombra el archivo .json original, añadiéndole el sufijo `_procesado.json`. Esto previene que el mismo archivo sea procesado e insertado múltiples veces en futuras ejecuciones.

Este script asegura que solo datos válidos, enriquecidos y no duplicados sean cargados a la base de datos, completando el ciclo de extracción y carga (ETL).
