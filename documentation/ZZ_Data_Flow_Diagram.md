
# Diagrama de Flujo de Datos del Proyecto

Este documento contiene un diagrama de flujo de datos detallado que describe todo el proceso, desde la ejecución del primer script hasta el envío final del correo electrónico.

El diagrama está escrito en sintaxis de **Mermaid**. Puede copiar y pegar el siguiente bloque de código en un visor en línea (como [mermaid.live](https://mermaid.live)) o usar una extensión en su editor de código para generar una imagen visual del flujo.

```mermaid
graph TD
    subgraph "Etapa 0: Orquestación"
        P1["(1) main_daily.py<br>Orquestador Principal"]
    end

    subgraph "Etapa 1: Extracción de URLs y Scraping Detallado"
        P2["(2) 2.DIARIO.daily_urls_extraction.VCLI.py<br>Extrae URLs del día"]
        P3["(3) 4.DIARIO.SEMANAL.SCRAPER.NEOAUTO.SUPABASE.PARA.CRON.py<br>Scraper Detallado"]
        
        D_NeoAuto{{"NeoAuto.com<br>Fuente de Datos"}}
        F_MarcasJson([marcas_y_sinonimos.json])
        DB_UrlsDiarios[(Base de Datos<br>Tabla: urls_autos_diarios)]
        
        F_ReglasDuenoJson([reglas_unico_dueno.json])
        DB_UrlsRandom[(Base de Datos<br>Tabla: urls_autos_random)]
        F_TxtResults[/Resultados TXT<br>results_txt/*.txt/];
    end

    subgraph "Etapa 2: Procesamiento de Datos (ETL)"
        P4["(4) 5.DIARIO.SEMANAL.Procesador_txt.a.json.DEEPSEEK_VCLI.py<br>Procesa TXT a JSON"]
        P5["(5) 6.json_a_supabase.DEEP.SEEK.CRON.VCLI.py<br>Carga JSON a Supabase"]
        
        F_JsonResults[/Resultados JSON<br>results_json/*.json/]
        DB_DetallesDiarios[(Base de Datos<br>Tabla: autos_detalles_diarios)]
    end

    subgraph "Etapa 3: Análisis de Mercado y Generación de Reportes"
        P6["(6) main.py<br>Motor de Análisis y Reportes"]
        
        DB_DetallesHist[(Base de Datos<br>Tabla: autos_detalles)]
        F_ReglasModelosCsv([Core/reglas_modelos_base.csv])
        
        F_ReportIndexHtml[/outputs/index.semanal.html/]
        F_ReportModelosHtml[/model_pages/semanal/*.html/]
        F_ReportLeadsHtml[/outputs/attractive_leads_...html/]
        F_ReportGmailHtml[/outputs/gmail_attractive_leads_...html/]
    end

    subgraph "Etapa 4: Distribución"
        P7["(7) gmail_sender.py<br>Envía Reporte por Email"]
        
        F_Destinatarios([destinatarios.txt])
        F_CredsJson([credentials.json / token.json])
        S_GmailApi{{"API de Gmail"}}
        U_User((Usuario Final))
    end

    %% Conexiones del Flujo
    P1 --> |ejecuta| P2
    P2 --> |ejecuta| P3
    P3 --> |ejecuta| P4
    P4 --> |ejecuta| P5
    P5 --> |ejecuta| P6
    P6 --> |ejecuta| P7

    %% Flujo Detallado
    D_NeoAuto -- "scrapea URLs de hoy" --> P2
    F_MarcasJson -- "lee reglas de marcas" --> P2
    DB_UrlsDiarios -- "lee URLs existentes" --> P2
    P2 -- "escribe nuevas URLs" --> DB_UrlsDiarios

    DB_UrlsDiarios -- "lee URLs a procesar" --> P3
    DB_UrlsRandom -- "lee URLs para re-scrapeo" --> P3
    D_NeoAuto -- "scrapea datos detallados" --> P3
    F_ReglasDuenoJson -- "lee reglas 'único dueño'" --> P3
    P3 -- "crea archivos de texto" --> F_TxtResults
    P3 -- "actualiza estado 'procesado'" --> DB_UrlsDiarios

    F_TxtResults -- "lee archivos de texto" --> P4
    P4 -- "crea archivos JSON" --> F_JsonResults
    P4 -- "renombra *.txt a *_procesado.txt" --> F_TxtResults

    F_JsonResults -- "lee archivos JSON" --> P5
    DB_DetallesDiarios -- "verifica duplicados por URL" --> P5
    P5 -- "inserta datos" --> DB_DetallesDiarios
    P5 -- "renombra *.json a *_procesado.json" --> F_JsonResults

    DB_DetallesHist -- "lee TODO el histórico" --> P6
    F_ReglasModelosCsv -- "lee reglas de modelos" --> P6
    P6 -- "genera reporte principal" --> F_ReportIndexHtml
    P6 -- "genera páginas de modelos" --> F_ReportModelosHtml
    P6 -- "genera reporte de leads" --> F_ReportLeadsHtml
    P6 -- "genera HTML para email" --> F_ReportGmailHtml

    F_ReportGmailHtml -- "lee cuerpo del email" --> P7
    F_Destinatarios -- "lee destinatarios" --> P7
    F_CredsJson -- "lee credenciales de API" --> P7
    P7 -- "envía email" --> S_GmailApi
    S_GmailApi -- "entrega reporte" --> U_User

    %% Estilos
    classDef process fill:#cde4ff,stroke:#004085,stroke-width:2px
    classDef file fill:#e2e3e5,stroke:#383d41
    classDef db fill:#d4edda,stroke:#155724
    classDef external fill:#fff3cd,stroke:#856404
    classDef user fill:#f8d7da,stroke:#721c24

    class P1,P2,P3,P4,P5,P6,P7 process
    class F_MarcasJson,F_ReglasDuenoJson,F_TxtResults,F_JsonResults,F_ReglasModelosCsv,F_ReportIndexHtml,F_ReportModelosHtml,F_ReportLeadsHtml,F_ReportGmailHtml,F_Destinatarios,F_CredsJson file
    class DB_UrlsDiarios,DB_UrlsRandom,DB_DetallesDiarios,DB_DetallesHist db
    class D_NeoAuto,S_GmailApi external
    class U_User user
```
