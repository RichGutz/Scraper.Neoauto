# Core/market_analyzer.py
import pandas as pd
import numpy as np
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

def to_snake_case(name: str) -> str:
    if not isinstance(name, str) or not name: return name
    name = re.sub('([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    name = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', name)
    name = name.replace('-', '_').replace(' ', '_')
    return name.lower()

def slugify_text(text: str) -> str:
    if not isinstance(text, str): text = str(text)
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s-]+', '-', text)
    text = re.sub(r'^-+|-+$', '', text)
    return text if text else "sin-slug"

class MarketAnalyzer:
    def __init__(self, df: pd.DataFrame, ratio_min: float,
                 listings_min: int, criteria_opportunities: str):
        logger.info("MA INIT: Iniciando MarketAnalyzer.")
        if not isinstance(df, pd.DataFrame) or df.empty:
            self.df_cleaned = pd.DataFrame()
            self.market_metrics_df = pd.DataFrame()
            return

        self.df_cleaned = self._clean_data(df.copy()) 

        if not self.df_cleaned.empty:
            self.market_metrics_df = self._calculate_model_metrics(self.df_cleaned)
        else:
            self.market_metrics_df = pd.DataFrame()
        
        logger.info("MA INIT: MarketAnalyzer inicializado y procesado.")

    def _clean_data(self, df_input: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"MA _clean_data: Iniciando. Filas de entrada: {len(df_input)}")
        df = df_input.copy()
        df.columns = [to_snake_case(col) for col in df.columns]
        
        df.drop_duplicates(subset=['url', 'date_time'], inplace=True, keep='first')
        
        for col in ['price', 'year']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        indispensable_cols = ['url', 'date_time', 'make', 'model', 'model_base', 'price', 'year']
        original_rows = len(df)
        df.dropna(subset=indispensable_cols, inplace=True)
        logger.info(f"MA _clean_data: Después de dropna en campos indispensables, quedaron {len(df)} de {original_rows} filas.")
        
        if df.empty: return pd.DataFrame()

        df['year'] = df['year'].astype(int)
        df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce', utc=True)
        df.dropna(subset=['date_time'], inplace=True)
        
        df['slug'] = df['model_base'].apply(slugify_text)

        rename_map = {
            'url': 'URL', 'date_time': 'DateTime', 'price': 'Price', 'year': 'Year',
            'model': 'Model', 'make': 'Make', 'model_base': 'Model_Base',
            'kilometers': 'Kilometers', 'district': 'District', 'unico_dueno': 'unico_dueno'
        }
        return df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    def _calculate_model_metrics(self, df_cleaned_input: pd.DataFrame) -> pd.DataFrame:
        if df_cleaned_input.empty: return pd.DataFrame()
        logger.info(f"MA CALC_METRICS: Calculando métricas para {len(df_cleaned_input)} filas.")

        grouped_main_metrics = df_cleaned_input.groupby(
            ['Make', 'Model_Base', 'slug'], as_index=False 
        )
        
        agg_funcs = { 'URL': pd.Series.nunique, 'Price': ['median', 'mean', 'min', 'max'] }
        market_metrics_df = grouped_main_metrics.agg(agg_funcs)
        
        # Aplanar los nombres de columna generados por .agg()
        market_metrics_df.columns = ['_'.join(col).strip() for col in market_metrics_df.columns.values]
        
        # Renombrar las columnas aplanadas a los nombres finales deseados
        market_metrics_df.rename(columns={
            'Make_': 'make_original_case',
            'Model_Base_': 'model_original_case',
            'slug_': 'slug',
            'URL_nunique': 'unique_listings',
            'Price_median': 'median_price',
            'Price_mean': 'mean_price',
            'Price_min': 'min_price',
            'Price_max': 'max_price'
        }, inplace=True)

        def calculate_fsr(group):
            if group.nunique() == 0: return np.nan
            return (group.value_counts() == 1).sum() / group.nunique()

        fsr_series = df_cleaned_input.groupby('slug')['URL'].apply(calculate_fsr)
        fsr_df = fsr_series.reset_index(name='fast_selling_ratio')
        
        return pd.merge(market_metrics_df, fsr_df, on='slug', how='left')