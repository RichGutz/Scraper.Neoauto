# Core/lead_filter.py
import pandas as pd
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)

def filter_attractive_leads(df_leads: pd.DataFrame, df_metrics: pd.DataFrame) -> pd.DataFrame:
    """
    Filters for the most attractive leads and prepares the data for reporting.

    Args:
        df_leads (pd.DataFrame): DataFrame containing the latest leads.
        df_metrics (pd.DataFrame): DataFrame containing market metrics per model.

    Returns:
        pd.DataFrame: A raw DataFrame with attractive leads and calculated opportunity metrics.
    """
    logger.info(f"LEAD_FILTER: Iniciando el filtrado de leads atractivos. Leads de entrada: {len(df_leads)}")

    if df_leads.empty or df_metrics.empty:
        return pd.DataFrame()

    # Time filter
    df_leads['DateTime'] = pd.to_datetime(df_leads['DateTime'], errors='coerce', utc=True)
    df_leads.dropna(subset=['DateTime'], inplace=True)
    max_timestamp = df_leads['DateTime'].max()
    one_day_ago = max_timestamp - timedelta(days=1)
    leads_last_day = df_leads[df_leads['DateTime'] >= one_day_ago].copy()
    logger.info(f"LEAD_FILTER: {len(leads_last_day)} leads encontrados en las últimas 24 horas.")

    if leads_last_day.empty: return pd.DataFrame()

    # Year filter
    leads_filtered_year = leads_last_day[leads_last_day['Year'] >= 2010].copy()
    logger.info(f"LEAD_FILTER: {len(leads_filtered_year)} leads encontrados desde el año 2010.")
    
    if leads_filtered_year.empty: return pd.DataFrame()

    # Single owner filter
    leads_unico_dueno = leads_filtered_year[leads_filtered_year['unico_dueno'].astype(str).str.lower() == 'true'].copy()
    logger.info(f"LEAD_FILTER: {len(leads_unico_dueno)} leads encontrados de un único dueño.")

    if leads_unico_dueno.empty: return pd.DataFrame()

    # Merge with metrics
    metrics_for_join = df_metrics[['Make', 'Model', 'mean_price', 'mean_year']].copy()
    leads_with_metrics = pd.merge(leads_unico_dueno, metrics_for_join, on=['Make', 'Model'], how='left')

    # Price filter
    attractive_leads = leads_with_metrics[leads_with_metrics['Price'] < leads_with_metrics['mean_price']].copy()
    logger.info(f"LEAD_FILTER: Se encontraron {len(attractive_leads)} leads atractivos después de filtrar por precio.")

    if attractive_leads.empty: return pd.DataFrame()

    # Calculate Opportunity Indicator
    attractive_leads['Oportunidad_Precio'] = (attractive_leads['Price'] - attractive_leads['mean_price']) / attractive_leads['mean_price']
    
    # Ensure Kilometers column exists
    if 'Kilometers' not in attractive_leads.columns:
        if 'kilometers' in attractive_leads.columns:
            attractive_leads.rename(columns={'kilometers': 'Kilometers'}, inplace=True)
        else:
            attractive_leads['Kilometers'] = 'N/A'

    return attractive_leads