"Motor de Análisis de Mercado y Generación de Reportes.

Este script es el cerebro analítico del proyecto. Consolida todos los datos
recopilados, realiza análisis de mercado, genera visualizaciones y produce
los reportes HTML finales, tanto para análisis general como para la
identificación de oportunidades de compra (leads).

Funcionalidad Principal:
1.  **Carga de Datos Históricos**: Se conecta a Supabase y descarga la totalidad
    de los datos de la tabla `autos_detalles`, que contiene el histórico de
    todos los vehículos procesados.

2.  **Procesamiento y Limpieza de Datos (Data Wrangling)**:
    - Estandariza los nombres de las marcas (`Make`) usando un mapeo predefinido.
    - Estandariza los nombres de los modelos (`Model`) a un "Modelo Base" usando
      un conjunto de reglas complejas desde un CSV (`reglas_modelos_base.csv`).
      Esto agrupa variantes de un mismo modelo (p. ej., "Corolla S", "Corolla LE")
      bajo un único nombre ("Corolla").
    - Limpia y convierte tipos de datos (precios, año a numérico).
    - Calcula la columna `Apariciones_URL_Hist`, que cuenta cuántas veces ha
      sido visto cada anuncio a lo largo del tiempo, un indicador clave de
      cuánto tiempo lleva un vehículo en el mercado.

3.  **Cálculo de Métricas de Mercado**:
    - Agrupa los datos por marca y modelo base para calcular métricas clave:
      - `unique_listings`: Número de anuncios únicos.
      - `median_price`: El precio mediano.
      - `mean_price`: El precio promedio.
      - `mean_year`: El año de fabricación promedio.
      - `fast_selling_ratio` (FSR): Una métrica que estima qué proporción de
        anuncios para un modelo se venden rápidamente (aparecen solo una vez).

4.  **Filtrado de Leads Atractivos**:
    - Aísla los anuncios de la última sesión de scraping (últimas 48 horas).
    - Compara el precio de estos anuncios recientes con la mediana de precio
      histórica de su respectivo modelo.
    - Identifica y filtra aquellos anuncios cuyo precio está significativamente
      por debajo de la media del mercado, marcándolos como "leads atractivos".

5.  **Generación de Reportes HTML**:
    - **Reporte Principal (`index.semanal.html`)**: Crea una página HTML con un
      gráfico de burbujas interactivo que visualiza el mercado. Cada burbuja
      representa un modelo y su tamaño, color y posición dependen de las
      métricas calculadas (precio, volumen, FSR).
    - **Páginas de Detalle por Modelo**: Para cada modelo, genera una página HTML
      individual que muestra un gráfico de dispersión de su historial de precios
      y una tabla con los anuncios activos (leads) de ese modelo.
    - **Reporte de Leads Atractivos**: Genera dos archivos HTML:
        - Un reporte completo y estilizado (`attractive_leads_report_...html`)
          para visualización en navegador.
        - Una versión simplificada con estilos en línea (`gmail_attractive_leads_...html`)
          optimizada para ser enviada por correo electrónico.

Al finalizar, abre automáticamente los reportes principales en el navegador web.
