# main.py (Versión con Re-arquitectura Final)
import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import shutil
from datetime import datetime, timedelta
import webbrowser
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import re
from plotly.express.colors import sample_colorscale, hex_to_rgb

# --- CONFIGURACIÓN (Sin Cambios) ---
log_dir = Path(__file__).parent
log_file_path = log_dir / "app_main.log"
root_logger = logging.getLogger()
if root_logger.hasHandlers(): root_logger.handlers.clear()
root_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
root_logger.addHandler(console_handler)
file_handler = logging.FileHandler(log_file_path, encoding='utf-8', mode='w')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
root_logger.addHandler(file_handler)
logger = logging.getLogger(__name__)
logging.getLogger('matplotlib').setLevel(logging.WARNING)
logging.getLogger('plotly').setLevel(logging.WARNING)
core_module_path = Path(__file__).parent / "Core"
if str(core_module_path) not in sys.path: sys.path.append(str(core_module_path))
from Core.bubbles_chart import generar_grafico_burbujas
from Core.charts_module import generar_graficos_individuales
from Core.page_generator import crear_html_pagina_modelo
from Core.main_report_builder import generar_index_html_reporte_principal
from Core.lead_filter import filter_attractive_leads
current_script_dir = Path(__file__).resolve().parent
dotenv_path = current_script_dir / "Core" / ".env"
RULES_CSV_PATH = current_script_dir / "Core" / "reglas_modelos_base.csv"
TABLE_NAME = "autos_detalles"
LOADED_MODEL_RULES = None
TARGET_MAKE_MAPPING = {
    'mercedes benz': 'mercedes', 'mercedes-benz': 'mercedes', 'mercedes': 'mercedes',
    'vw': 'volkswagen', 'volkswagen': 'volkswagen', 'toyota': 'toyota', 'bmw': 'bmw',
    'nissan': 'nissan', 'hyundai': 'hyundai', 'subaru': 'subaru', 'mazda': 'mazda',
    'ford': 'ford', 'kia': 'kia', 'jeep': 'jeep', 'audi': 'audi', 'honda': 'honda',
    'chevrolet': 'chevrolet', 'mitsubishi': 'mitsubishi', 'suzuki': 'suzuki', 'volvo': 'volvo'
}
STANDARDIZED_TARGET_MAKES = sorted(list(set(TARGET_MAKE_MAPPING.values())))

# --- FUNCIONES DE PROCESAMIENTO (Con correcciones) ---

def load_rules():
    global LOADED_MODEL_RULES
    if LOADED_MODEL_RULES is None:
        try:
            if RULES_CSV_PATH.exists():
                LOADED_MODEL_RULES = pd.read_csv(RULES_CSV_PATH)
                LOADED_MODEL_RULES['make_rule_match'] = LOADED_MODEL_RULES['make_rule_match'].astype(str).str.lower().str.strip()
                LOADED_MODEL_RULES['model_pattern_input_lower'] = LOADED_MODEL_RULES['model_pattern_input_lower'].astype(str).str.lower().str.strip()
                LOADED_MODEL_RULES.sort_values(by=['priority', 'pattern_length'], ascending=[False, False], inplace=True)
        except Exception as e:
            logger.error(f"Error cargando reglas: {e}")
            LOADED_MODEL_RULES = pd.DataFrame()

def fetch_all_data(client: Client) -> pd.DataFrame:
    logger.info(f"Iniciando descarga robusta de la tabla: '{TABLE_NAME}'")
    all_data = []
    offset = 0
    limit = 1000
    while True:
        try:
            response = client.from_(TABLE_NAME).select('*').order('id', desc=True).range(offset, offset + limit - 1).execute()
            if not response.data: break
            all_data.extend(response.data)
            logger.info(f"Descargadas {len(response.data)} filas (total: {len(all_data)})...")
            if len(response.data) < limit: break
            offset += limit
        except Exception as e:
            logger.error(f"Error durante la descarga: {e}", exc_info=True)
            return pd.DataFrame()
    logger.info(f"Descarga completada. Total de filas recuperadas: {len(all_data)}")
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

def get_model_base(model_name: str, make_name: str) -> str:
    if pd.isna(model_name) or model_name == "Desconocido": return "Desconocido"
    model_lower = model_name.lower().strip()
    make_lower = make_name.lower().strip()
    if LOADED_MODEL_RULES is not None and not LOADED_MODEL_RULES.empty:
        rules_for_make = LOADED_MODEL_RULES[LOADED_MODEL_RULES['make_rule_match'] == make_lower]
        for _, rule in rules_for_make.iterrows():
            if (rule['match_type'] == 'exact' and model_lower == rule['model_pattern_input_lower']) or \
               (rule['match_type'] == 'startswith' and model_lower.startswith(rule['model_pattern_input_lower'])) or \
               (rule['match_type'] == 'contains' and rule['model_pattern_input_lower'] in model_lower):
                return rule['model_base_target']
    return model_name

def process_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Iniciando procesamiento de {len(df_raw)} filas.")
    df = df_raw.copy()
    
    # Estandarización
    desconocido_str = "Desconocido"
    df['Make'] = df['Make'].fillna(desconocido_str).astype(str).str.strip().str.lower().map(TARGET_MAKE_MAPPING).fillna(df['Make'].str.lower()).str.title()
    df['Model'] = df['Model'].fillna(desconocido_str).astype(str).str.strip()
    df['Model_Base'] = df.apply(lambda row: get_model_base(row['Model'], row['Make']), axis=1)
    df['slug'] = (df['Make'] + ' ' + df['Model_Base']).str.lower().str.replace(r'[^a-z0-9\s-]', '', regex=True).str.replace(r'\s+', '-', regex=True)

    # Filtro de Marcas
    df_filtered = df[df['Make'].str.lower().isin(STANDARDIZED_TARGET_MAKES)].copy()

    # Limpieza final
    df_clean = df_filtered.copy()
    for col in ['Price', 'Year']:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    df_clean.drop_duplicates(subset=['URL', 'DateTime'], inplace=True)
    indispensable_cols = ['URL', 'DateTime', 'Make', 'Model', 'Model_Base', 'Price', 'Year']
    df_clean.dropna(subset=indispensable_cols, inplace=True)
    if not df_clean.empty:
        df_clean['Year'] = df_clean['Year'].astype(int)

    # ===================================================================
    # CÁLCULO DE APARICIONES CENTRALIZADO (LA NUEVA ARQUITECTURA)
    # ===================================================================
    logger.info("Calculando apariciones por URL y añadiéndolo al DataFrame principal...")
    # Usamos transform('size') para contar las filas por grupo y mantener la forma del DF original
    df_clean['Apariciones_URL_Hist'] = df_clean.groupby('URL')['URL'].transform('size')
    logger.info("Columna 'Apariciones_URL_Hist' creada y añadida con éxito.")
    # ===================================================================

    return df_clean

def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Calculando métricas de mercado para {len(df)} filas.")
    if df.empty: return pd.DataFrame()
    grouped = df.groupby(['Make', 'Model_Base', 'slug'], as_index=False)
    metrics = grouped.agg(unique_listings=('URL', 'nunique'), median_price=('Price', 'median'), mean_price=('Price', 'mean'), mean_year=('Year', 'mean'))
    
    # ===================================================================
    # CÁLCULO DE FSR CORREGIDO (LÓGICA ORIGINAL RESTAURADA)
    # ===================================================================
    fsr = df.groupby('slug')['URL'].apply(lambda g: (g.value_counts() == 1).sum() / g.nunique() if g.nunique() > 0 else 0).reset_index(name='fast_selling_ratio')
    # ===================================================================

    final_metrics = pd.merge(metrics, fsr, on='slug', how='left')
    final_metrics.rename(columns={'Make': 'make_original_case', 'Model_Base': 'model_original_case'}, inplace=True)
    return final_metrics

# --- FUNCIÓN PRINCIPAL DE EJECUCIÓN ---

def run_market_analysis():
    logger.info("=" * 60 + f"\nINICIANDO ANÁLISIS UNIFICADO ({datetime.now()})\n" + "=" * 60)
    
    load_dotenv(dotenv_path)
    load_rules()
    
    supabase_client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    df_raw = fetch_all_data(supabase_client)
    if df_raw.empty:
        logger.critical("No se descargaron datos. Abortando."); return

    # Ahora process_data devuelve el DF con la columna 'Apariciones_URL_Hist' ya incluida
    df_processed = process_data(df_raw)
    if df_processed.empty:
        logger.critical("No quedaron datos después del procesamiento y filtrado. Abortando."); return

    df_metrics = calculate_metrics(df_processed)
    if df_metrics.empty:
        logger.warning("No se pudieron calcular las métricas."); return

    dashboard_df = df_metrics.copy()
    
    df_processed['DateTime'] = pd.to_datetime(df_processed['DateTime'], errors='coerce', utc=True)
    max_timestamp = df_processed['DateTime'].dropna().max()
    df_leads_latest_session = df_processed[df_processed['DateTime'] >= (max_timestamp - timedelta(hours=48))].copy()
    logger.info(f"Se aislaron {len(df_leads_latest_session)} leads de la última sesión.")

    # Lógica de estilo para el gráfico de burbujas
    if 'unico_dueno' in df_leads_latest_session.columns:
        mask_unico_dueno = df_leads_latest_session['unico_dueno'].astype(str).str.lower() == 'true'
        modelos_con_unico_dueno = set(df_leads_latest_session[mask_unico_dueno]['Model_Base'].unique())
        dashboard_df['tiene_unico_dueno'] = dashboard_df['model_original_case'].isin(modelos_con_unico_dueno)
    else:
        dashboard_df['tiene_unico_dueno'] = False
    
    border_colors, border_widths, fill_colors_rgba = [], [], []
    fsr_values = dashboard_df['fast_selling_ratio']
    if not fsr_values.dropna().empty:
        min_fsr, max_fsr = fsr_values.min(), fsr_values.max()
        fsr_range = max_fsr - min_fsr if max_fsr > min_fsr else 1.0
        for _, row in dashboard_df.iterrows():
            if row['tiene_unico_dueno']:
                border_colors.append('red'); border_widths.append(2.5)
            else:
                border_colors.append('white'); border_widths.append(1.0)
            fsr = row['fast_selling_ratio']
            if pd.isna(fsr):
                fill_colors_rgba.append('rgba(200, 200, 200, 0.7)')
            else:
                norm_val = (fsr - min_fsr) / fsr_range if fsr_range > 0 else 0.5
                sampled_color = sample_colorscale('Viridis', norm_val)[0]
                rgb = [int(n) for n in re.findall(r'\d+', sampled_color)] if sampled_color.startswith('rgb') else hex_to_rgb(sampled_color)
                fill_colors_rgba.append(f'rgba({rgb[0]}, {rgb[1]}, {rgb[2]}, 0.7)')
    dashboard_df['marker_fill_color'] = fill_colors_rgba
    dashboard_df['marker_border_color'] = border_colors
    dashboard_df['marker_border_width'] = border_widths
    
    models_in_dashboard = set(dashboard_df['model_original_case'].dropna().unique())
    models_with_leads = set(df_leads_latest_session['Model_Base'].dropna().unique())
    models_for_charts = sorted(list(models_in_dashboard.union(models_with_leads)))
    
    # YA NO SE NECESITA CALCULAR NI PASAR url_counts_df POR SEPARADO
    analysis_results_dict = generar_graficos_individuales(
        df_historic=df_processed,
        df_leads=df_leads_latest_session,
        filtered_model_names_pascal_case=models_for_charts
    )
    
    leads_counts_by_model = {k: len(v['leads_df']) for k, v in analysis_results_dict.items()}
    dashboard_df['leads_count'] = dashboard_df['model_original_case'].map(leads_counts_by_model).fillna(0).astype(int)
    dashboard_df.rename(columns={'model_original_case': 'Model', 'make_original_case': 'Make'}, inplace=True)
    
    # Generación de Reportes HTML
    base_output_dir = Path(__file__).parent
    main_report_html_path = base_output_dir / "outputs" / "index.semanal.html"
    model_pages_output_dir = base_output_dir / "model_pages" / "semanal"
    
    if model_pages_output_dir.exists(): shutil.rmtree(model_pages_output_dir)
    model_pages_output_dir.mkdir(parents=True, exist_ok=True)
    main_report_html_path.parent.mkdir(parents=True, exist_ok=True)
    
    fig_bubble_obj = generar_grafico_burbujas(dashboard_df)
    if fig_bubble_obj:
        logger.info("Generando páginas de detalle...")
        for model_name_base in models_for_charts:
            model_results = analysis_results_dict.get(model_name_base, {})
            model_dashboard_data = dashboard_df[dashboard_df['Model'] == model_name_base]
            if not model_dashboard_data.empty:
                datos_modelo_row = model_dashboard_data.iloc[0]
            elif model_results and not model_results.get('leads_df', pd.DataFrame()).empty:
                first_lead = model_results['leads_df'].iloc[0]
                datos_modelo_row = pd.Series({'Model': model_name_base, 'Make': first_lead.get('Make'), 'slug': first_lead.get('slug')})
            else: continue
            html_content = crear_html_pagina_modelo(
                datos_modelo=datos_modelo_row,
                df_leads=model_results.get('leads_df', pd.DataFrame()),
                figura_plotly_obj=model_results.get('figura_plotly', None)
            )
            (model_pages_output_dir / f"{datos_modelo_row['slug']}.html").write_text(html_content, encoding="utf-8")

        generar_index_html_reporte_principal(fig_bubble_obj=fig_bubble_obj, summary_opportunities_df=dashboard_df, output_file_path=main_report_html_path)

    # ===================================================================
    # NUEVO: FILTRAR Y GENERAR REPORTES DE LEADS ATRACTIVOS
    # ===================================================================
    logger.info("Iniciando el filtrado de leads atractivos...")
    
    df_attractive_leads_raw = filter_attractive_leads(
        df_leads=df_leads_latest_session,
        df_metrics=dashboard_df
    )

    if not df_attractive_leads_raw.empty:
        # --- 1. Preparar DataFrame para los reportes ---
        df_report = df_attractive_leads_raw.copy()
        df_report.sort_values(by='Oportunidad_Precio', ascending=True, inplace=True)

        # Formateo de columnas
        df_report['Oportunidad_Precio'] = df_report['Oportunidad_Precio'].map('{:.2%}'.format)
        df_report['Price'] = df_report['Price'].map('{:,.0f}'.format)
        df_report['Kilometers'] = pd.to_numeric(df_report['Kilometers'], errors='coerce').fillna(0).astype(int).map('{:,.0f}'.format)
        
        # Crear enlaces
        df_report['URL'] = df_report['URL'].apply(lambda x: f'<a href="{x}">Ver Anuncio</a>')
        df_report['Análisis de Modelo'] = df_report['slug'].apply(lambda x: f'<a href="../model_pages/semanal/{x}.html">Ver Análisis</a>')

        columns_to_display = ['Make', 'Model', 'Year', 'Price', 'Kilometers', 'Oportunidad_Precio', 'URL', 'Análisis de Modelo']
        df_report_final = df_report[columns_to_display]

        # --- 2. Generar Reporte HTML Completo ---
        today_str = datetime.now().strftime("%Y-%m-%d")
        attractive_leads_report_path = base_output_dir / "outputs" / f"attractive_leads_report_{today_str}.html"
        logger.info(f"Generando reporte de leads atractivos en: {attractive_leads_report_path}")
        report_css = """
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 0; background-color: #f0f2f5; color: #333; }
            .container { width: 95%; max-width: 1800px; margin: 20px auto; }
            header { text-align: left; margin-bottom: 20px; border-bottom: 2px solid #e0e0e0; padding-bottom: 10px;}
            header h1 { color: #1a237e; margin: 0; font-size: 1.8em; }
            .styled-table { width: 100%; border-collapse: collapse; margin-top: 20px; font-size: 0.9em; }
            .styled-table th, .styled-table td { border: 1px solid #ddd; padding: 8px 10px; text-align: left; }
            .styled-table th { background-color: #343a40; color: white; }
            .styled-table tr:nth-child(even) { background-color: #f8f9fa; }
            .styled-table a { font-weight: bold; }
        </style>
        """
        full_html = f'''
        <!DOCTYPE html>
        <html lang="es">
        <head>
            <meta charset="UTF-8">
            <title>Reporte de Leads Atractivos ({today_str})</title>
            {report_css}
        </head>
        <body>
            <div class="container">
                <header><h1>Reporte de Leads Atractivos</h1></header>
                <main>{df_report_final.to_html(index=False, escape=False, classes='styled-table')}</main>
            </div>
        </body>
        </html>'''
        attractive_leads_report_path.write_text(full_html, encoding="utf-8")
        logger.info("Reporte de leads atractivos generado con éxito.")
        webbrowser.open(attractive_leads_report_path.as_uri())

        # --- 3. Generar HTML para Gmail ---
        gmail_report_path = base_output_dir / "outputs" / f"gmail_attractive_leads_{today_str}.html"
        logger.info(f"Generando HTML para Gmail en: {gmail_report_path}")
        
        # Usar estilos en línea para máxima compatibilidad con Gmail
        gmail_html = df_report_final.to_html(index=False, escape=False, border=0)
        gmail_html = gmail_html.replace('<table', '<table style="border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;"')
        gmail_html = gmail_html.replace('<thead>', '<thead style="background-color: #4CAF50; color: white;">')
        gmail_html = gmail_html.replace('<th>', '<th style="padding: 8px; text-align: left; border-bottom: 1px solid #ddd;">')
        gmail_html = gmail_html.replace('<td>', '<td style="padding: 8px; text-align: left; border-bottom: 1px solid #ddd;">')

        gmail_report_path.write_text(gmail_html, encoding="utf-8")
        logger.info("HTML para Gmail generado con éxito.")

    else:
        logger.info("No se encontraron leads atractivos que cumplan con los criterios.")
    # ===================================================================
    
    logger.info("="*60 + "\nPROCESO FINALIZADO\n" + "="*60)
    webbrowser.open(main_report_html_path.as_uri())

if __name__ == "__main__":
    run_market_analysis()