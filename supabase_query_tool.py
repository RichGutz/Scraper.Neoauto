import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from pathlib import Path
import argparse

def query_supabase_table(url_to_check: str = None):
    """
    Connects to Supabase and queries the 'autos_detalles_diarios' table.
    If a URL is provided, it checks for that specific URL. Otherwise, it returns the last 10 entries.

    Args:
        url_to_check (str, optional): The URL to check for in the database. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the query results, or an empty DataFrame on error.
    """
    try:
        # --- 1. Cargar variables de entorno ---
        current_script_dir = Path(__file__).resolve().parent
        dotenv_path = current_script_dir / "Core" / ".env"
        load_dotenv(dotenv_path)

        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            print("Error: Las variables de entorno SUPABASE_URL y SUPABASE_KEY no están definidas.")
            print(f"Asegúrate de que el archivo .env se encuentra en: {dotenv_path}")
            return pd.DataFrame()

        # --- 2. Crear cliente de Supabase ---
        print("Conectando a Supabase...")
        supabase: Client = create_client(supabase_url, supabase_key)
        print("Conexión exitosa.")

        # --- 3. Consultar la tabla ---
        table_name = "autos_detalles_diarios"
        
        if url_to_check:
            print(f"Buscando la URL: '{url_to_check}' en la tabla '{table_name}'...")
            response = supabase.from_(table_name).select("*").eq("URL", url_to_check).execute()
        else:
            print(f"Consultando los últimos 10 registros de la tabla: '{table_name}'...")
            response = supabase.from_(table_name).select("*").order("id", desc=True).limit(10).execute()

        # --- 4. Procesar y mostrar los datos ---
        if response.data:
            print(f"Se encontraron {len(response.data)} registros.")
            df = pd.DataFrame(response.data)
            
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 1000)
            
            print("Resultados de la consulta:")
            print(df)
            return df
        else:
            print("No se encontraron datos que coincidan con la consulta.")
            return pd.DataFrame()

    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Herramienta para consultar la base de datos de Supabase.")
    parser.add_argument("--url", type=str, help="La URL específica del anuncio para buscar en la base de datos.")
    args = parser.parse_args()

    query_supabase_table(url_to_check=args.url)