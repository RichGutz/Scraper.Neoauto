# Core/charts_module.py (Versión con Re-arquitectura Final)
import pandas as pd
import numpy as np
import logging
import plotly.graph_objects as go
from scipy.interpolate import make_interp_spline

logger = logging.getLogger(__name__)

def _plot_price_trend(fig, model_name: str, trend_col: str, label_text: str, yearly_stats: pd.DataFrame):
    if yearly_stats.empty or trend_col not in yearly_stats.columns or 'Year' not in yearly_stats.columns:
        return
    trend_data = yearly_stats[['Year', trend_col]].dropna().sort_values(by='Year')
    if len(trend_data) > 1:
        x_years, y_prices = trend_data['Year'].values, trend_data[trend_col].values
        try:
            k_val = min(len(x_years) - 1, 3)
            if k_val >= 1:
                x_smooth = np.linspace(x_years.min(), x_years.max(), 100)
                spl = make_interp_spline(x_years, y_prices, k=k_val)
                fig.add_trace(go.Scatter(x=x_smooth, y=spl(x_smooth), mode='lines', name=label_text, line=dict(dash='dash')))
        except Exception as e:
            logger.warning(f"Error spline para {model_name} ({label_text}): {e}.")
            fig.add_trace(go.Scatter(x=x_years, y=y_prices, mode='lines+markers', name=label_text, marker=dict(size=4)))

def generar_graficos_individuales(
    df_historic: pd.DataFrame,
    df_leads: pd.DataFrame,
    filtered_model_names_pascal_case: list
):
    """
    Genera los gráficos individuales. Ya no necesita recibir 'url_datetime_counts'
    porque la información de apariciones ya viene en 'df_leads'.
    """
    logger.info(f"CHARTS_MODULE INIT: Iniciando generación de gráficos para {len(filtered_model_names_pascal_case)} modelos.")
    output_dict = {}

    df_historic['Year'] = pd.to_numeric(df_historic['Year'], errors='coerce')
    df_historic['Price'] = pd.to_numeric(df_historic['Price'], errors='coerce')
    all_yearly_stats = df_historic.dropna(subset=['Model_Base', 'Year', 'Price']).groupby(['Model_Base', 'Year'])['Price'].agg(['median', 'mean']).reset_index()

    for model_name_base in filtered_model_names_pascal_case:
        
        model_yearly_stats = all_yearly_stats[all_yearly_stats['Model_Base'] == model_name_base]
        
        # El DataFrame de leads ya contiene toda la información necesaria
        leads_for_model = df_leads[df_leads['Model_Base'] == model_name_base].copy()
        
        logger.info(f"CHARTS_MODULE LOOP ({model_name_base}): Leads a graficar: {len(leads_for_model)}.")
        
        fig = go.Figure()
        
        if not model_yearly_stats.empty:
            _plot_price_trend(fig, model_name_base, 'median', 'Mediana Histórica', model_yearly_stats)
            _plot_price_trend(fig, model_name_base, 'mean', 'Promedio Histórico', model_yearly_stats)

        leads_trace_index = -1
        if not leads_for_model.empty:
            # La columna 'Apariciones_URL_Hist' ya existe, solo nos aseguramos de que no haya nulos
            leads_for_model['Apariciones_URL_Hist'] = leads_for_model['Apariciones_URL_Hist'].fillna(1).astype(int)
            
            leads_to_plot = leads_for_model.dropna(subset=['Year', 'Price', 'URL']).reset_index().rename(columns={'index': 'lead_index'})
            
            if not leads_to_plot.empty:
                if 'unico_dueno' not in leads_to_plot.columns:
                    leads_to_plot['unico_dueno'] = False
                leads_to_plot['unico_dueno'] = leads_to_plot['unico_dueno'].fillna(False).astype(bool)
                colors = leads_to_plot['unico_dueno'].map({True: '#d62728', False: '#2ca02c'})

                leads_trace_index = len(fig.data)
                fig.add_trace(go.Scatter(
                    x=leads_to_plot['Year'],
                    y=leads_to_plot['Price'],
                    mode='markers',
                    name=f'Leads',
                    marker=dict(color=colors, size=10),
                    customdata=np.stack((leads_to_plot['URL'], leads_to_plot['lead_index']), axis=-1),
                    hovertemplate='<b>Lead</b><br>Año: %{x}<br>Precio: $%{y:,.0f}<br><i>Click para ver anuncio</i><extra></extra>'
                ))
        
        fig.update_layout(
            title=f"{model_name_base} - Tendencia y Leads",
            xaxis_title="Año del Modelo", yaxis_title="Precio",
            yaxis_tickprefix='$', yaxis_tickformat=',.0f',
            legend_title="Leyenda", template="plotly_white",
            meta={'leads_trace_index': leads_trace_index}
        )
        
        output_dict[model_name_base] = {
            'leads_df': leads_for_model,
            'figura_plotly': fig
        }

    logger.info("--- CHARTS_MODULE: Generación de gráficos completada ---")
    return output_dict