# Core/bubbles_chart.py
import pandas as pd
import plotly.graph_objects as go
import logging

logger = logging.getLogger(__name__)

def generar_grafico_burbujas(summary_df: pd.DataFrame):
    """
    Genera un objeto de figura de Plotly para el gráfico de burbujas.
    Esta versión es agnóstica al estilo; simplemente dibuja los estilos
    que vienen pre-calculados en el DataFrame de entrada.
    """
    if summary_df is None or summary_df.empty:
        logger.warning("BUBBLES_CHART: DataFrame de entrada está vacío. No se generará gráfico.")
        return None
    
    required_cols = [
        'unique_listings', 'fast_selling_ratio', 'Model', 'slug', 
        'marker_fill_color', 'marker_border_color', 'marker_border_width'
    ]
    if not all(col in summary_df.columns for col in required_cols):
        logger.error(f"BUBBLES_CHART: Faltan columnas de estilo pre-calculadas. Se necesitan: {required_cols}")
        return None

    logger.info("BUBBLES_CHART: Generando gráfico de burbujas con estilos centralizados.")

    trace = go.Scatter(
        x=summary_df['unique_listings'],
        y=summary_df['fast_selling_ratio'],
        mode='markers+text',
        
        text=summary_df['Model'],
        textposition='top center',
        textfont=dict(size=9, color='black'),
        
        marker=dict(
            size=summary_df['unique_listings'],
            sizemode='area',
            sizeref=2.*max(summary_df['unique_listings'].replace(0, 1))/(45.**2), # Evitar división por cero
            color=summary_df['marker_fill_color'],
            line=dict(
                color=summary_df['marker_border_color'],
                width=summary_df['marker_border_width']
            )
        ),
        
        customdata=summary_df['slug'],
        hovertemplate="<b>%{text}</b><br>FSR: %{y:.1%}<br>Anuncios: %{x:.0f}<extra></extra>"
    )

    fig = go.Figure(data=[trace])

    fig.update_layout(
        title='Oportunidades de Mercado de Autos Usados',
        xaxis=dict(title='Anuncios Únicos (escala logarítmica)', type='log'),
        yaxis=dict(title='Ratio de Venta Rápida', tickformat=',.0%'),
        height=700,
        showlegend=False,
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(family="Arial, sans-serif")
    )
    
    return fig