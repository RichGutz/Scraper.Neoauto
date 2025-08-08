from bs4 import BeautifulSoup
import pandas as pd
import logging
from typing import List, Dict, Optional
import sys
from pathlib import Path
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime
import json
import time
import random

# Importaciones de Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extractor.log'),
        logging.StreamHandler()
    ]
)

# Cargar variables de entorno
load_dotenv()

BRAND_CONFIG_FILE = "marcas_y_sinonimos.json" 
BASE_DIR = Path(__file__).parent
CHROMEDRIVER_PATH = BASE_DIR / "chromedriver.exe"

class NeoAutoExtractor:
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
    }

    def __init__(self, base_url_template: str = "https://neoauto.com/venta-de-autos-"):
        self.base_url_template = base_url_template
        self.supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
        self.brand_config = self._load_brand_config()
        self.driver = self._initialize_driver()

    def _initialize_driver(self):
        """Inicializa y devuelve un driver de Selenium."""
        logging.info("Inicializando el driver de Selenium...")
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        # Opcional: Especifica la ruta al binario de Chromium si no está en el PATH
        # options.binary_location = "ruta/a/tu/chromium.exe"
        options.add_argument("start-maximized")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        service = Service(executable_path=str(CHROMEDRIVER_PATH))
        try:
            driver = webdriver.Chrome(service=service, options=options)
            logging.info("Driver inicializado en modo headless.")
            return driver
        except Exception as e:
            logging.error(f"Error al inicializar el driver: {e}")
            sys.exit(1)

    def _load_brand_config(self) -> dict:
        file_path = Path(__file__).parent / BRAND_CONFIG_FILE
        if not file_path.exists():
            logging.error(f"ERROR CRÍTICO: El archivo '{file_path}' no fue encontrado.")
            sys.exit(1)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config
        except Exception as e:
            logging.error(f"Error al cargar la configuración de marcas '{file_path}': {e}")
            sys.exit(1)

    def fetch_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """Obtiene el contenido HTML usando la instancia de driver existente."""
        logging.info(f"Navegando a: {url}")
        XPATH_SELECTOR = "//div[contains(@class, 's-results__count')]"

        try:
            self.driver.get(url)
            wait = WebDriverWait(self.driver, 20)
            wait.until(EC.presence_of_element_located((By.XPATH, XPATH_SELECTOR)))
            
            logging.info(f"Elemento de conteo encontrado. Extrayendo HTML.")
            html_content = self.driver.page_source
            return BeautifulSoup(html_content, 'html.parser')
        except Exception as e:
            logging.warning(f"No se pudo cargar o encontrar el conteo para {url}. Error: {e}")
            return None

    def extract_brands_data(self) -> List[Dict]:
        """Itera sobre cada marca, visita su página y extrae el conteo de autos."""
        all_brands_data = []
        target_brands = list(self.brand_config.keys())
        logging.info(f"Iniciando extracción para {len(target_brands)} marcas.")

        for brand in target_brands:
            brand_url = f"{self.base_url_template}{brand}"
            soup = self.fetch_page_content(brand_url)

            if soup:
                try:
                    count_element = soup.find('div', class_='s-results__count')
                    if count_element:
                        count_text = count_element.text
                        count = int("".join(filter(str.isdigit, count_text)))
                        
                        all_brands_data.append({
                            'marca': brand,
                            'cantidad_autos': count,
                            'fecha_actualizacion': datetime.now().isoformat()
                        })
                        logging.info(f"Marca '{brand}': {count} vehículos encontrados.")
                    else:
                        logging.warning(f"No se encontró el selector de conteo para la marca '{brand}'.")
                except (ValueError, IndexError) as e:
                    logging.error(f"Error extrayendo el número para la marca '{brand}': {e}")
            
            pause_duration = random.uniform(2, 5)
            logging.info(f"Pausa de {pause_duration:.2f} segundos...")
            time.sleep(pause_duration)
        
        return all_brands_data

    def clean_and_save_to_supabase(self, data: List[Dict]) -> bool:
        if not data:
            logging.warning("No hay datos para guardar en Supabase.")
            return False
        try:
            logging.info(f"Limpiando y guardando {len(data)} registros en 'conteo_marcas'...")
            self.supabase.table('conteo_marcas').delete().neq('id', 0).execute()
            response = self.supabase.table('conteo_marcas').insert(data).execute()
            if response.data:
                logging.info(f"Datos guardados en Supabase: {len(response.data)} registros.")
                return True
            logging.error("No se confirmaron datos guardados en Supabase.")
            return False
        except Exception as e:
            logging.error(f"Error al guardar en Supabase: {e}")
            return False

    def close(self):
        """Cierra el driver de Selenium."""
        if self.driver:
            logging.info("Cerrando el driver de Selenium.")
            self.driver.quit()

    def run(self) -> bool:
        logging.info("Iniciando proceso de extracción...")
        try:
            brands_data = self.extract_brands_data()
            if not brands_data:
                logging.error("El proceso terminó sin extraer datos.")
                return False
            if not self.clean_and_save_to_supabase(brands_data):
                return False
            logging.info("Proceso completado exitosamente.")
            return True
        finally:
            self.close()

if __name__ == "__main__":
    extractor = NeoAutoExtractor()
    if not extractor.run():
        sys.exit(1)
    
    try:
        response = extractor.supabase.table('conteo_marcas').select('*').execute()
        df = pd.DataFrame(response.data)
        print("\nResumen de marcas objetivo desde Supabase (ordenado por cantidad):")
        print(df.sort_values('cantidad_autos', ascending=False).to_string(index=False))
    except Exception as e:
        logging.error(f"Error al mostrar resultados: {e}")
        sys.exit(1)
