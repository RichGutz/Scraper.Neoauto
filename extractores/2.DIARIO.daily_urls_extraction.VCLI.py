"""
Extractor de URLs Diarias de NeoAuto.

Este script se especializa en raspar (scrape) la primera capa de información
de NeoAuto: las URLs de los anuncios de vehículos usados publicados en el día.

Funcionalidad Principal:
1.  **Conexión a Supabase**: Se inicializa una conexión segura con la base de
    datos de Supabase para leer y escribir información.
2.  **Carga de Mapeo de Marcas**: Lee un archivo JSON (`marcas_y_sinonimos.json`) 
    para estandarizar los nombres de las marcas de vehículos extraídas de las URLs.
3.  **Obtención de URLs Existentes**: Consulta la tabla `urls_autos_diarios` en
    Supabase para obtener un conjunto de todas las URLs ya registradas, con el
    fin de evitar la duplicación de datos.
4.  **Scraping de Páginas**: Navega a través de las páginas de resultados de
    NeoAuto que filtran por anuncios "publicado=hoy". Extrae el enlace (`href`)
    de cada anuncio.
5.  **Validación y Enriquecimiento**: Por cada URL nueva, extrae la marca, la
    valida contra el mapeo cargado y, si es válida, la prepara para ser
    insertada.
6.  **Inserción en Base de Datos**: Inserta el lote de URLs nuevas y validadas
    en la tabla `urls_autos_diarios` de Supabase, junto con la marca estandarizada, 
    la fecha de extracción y un booleano `procesado` inicializado en `False`.

El script está diseñado para ser el primer paso del pipeline diario, alimentando
la cola de URLs que serán procesadas en detalle por scripts posteriores.
"""
#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import math
from datetime import datetime
from supabase import create_client
import os
from dotenv import load_dotenv
import json
from pathlib import Path
import logging
import re
import time

# Configuración de logging
log_file_path = Path(__file__).parent / 'neoauto_scraper.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class NeoAutoDailyScraper:
    def __init__(self):
        self.BASE_URL = "https://neoauto.com/venta-de-autos-usados?publicado=hoy"
        self.HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.request_delay = 2
        self.supabase = self._init_supabase()
        self.brand_mapping = self._load_brand_mapping()
        logging.info(f"Marcos cargados: {len(self.brand_mapping)}")

    def _init_supabase(self):
        """Inicializa el cliente Supabase."""
        load_dotenv()
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            logging.critical("Variables de Supabase no configuradas")
            raise ValueError("Faltan credenciales de Supabase")
        
        try:
            client = create_client(supabase_url, supabase_key)
            client.table("urls_autos_diarios").select("count", count="exact").execute()
            return client
        except Exception as e:
            logging.critical(f"Fallo conexión Supabase: {str(e)}")
            raise

    def _load_brand_mapping(self) -> dict:
        """Carga el mapeo de marcas."""
        try:
            json_path = Path(__file__).parent / "marcas_y_sinonimos.json"
            with open(json_path, 'r', encoding='utf-8') as f:
                return {k.lower().replace('-', '').replace(' ', ''): v.lower() for k,v in json.load(f).items()}
        except Exception as e:
            logging.critical(f"Error cargando marcas: {str(e)}")
            raise

    def _extract_brand_from_url(self, url: str) -> str:
        """Extrae marca de la URL sin validaciones estrictas."""
        match = re.search(r'/(usado|seminuevo)/([a-zA-Z0-9]+)-', url)
        return match.group(2).lower() if match else ""

    def _get_valid_brand(self, raw_brand: str) -> str:
        """Valida marca contra el mapeo."""
        clean_brand = raw_brand.lower().strip().replace('-', '').replace(' ', '')
        return self.brand_mapping.get(clean_brand, "")

    def _get_existing_urls(self) -> set:
        """Obtiene URLs existentes."""
        try:
            response = self.supabase.table('urls_autos_diarios').select('url').execute()
            return {item['url'] for item in response.data}
        except Exception as e:
            logging.error(f"Error obteniendo URLs existentes: {str(e)}")
            return set()

    def scrape_page(self, page: int) -> list:
        """Extrae URLs de una página."""
        try:
            page_url = f"{self.BASE_URL}&page={page}"
            time.sleep(self.request_delay)
            response = requests.get(page_url, headers=self.HEADERS, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup.select('a.c-results__link[href]')
        except Exception as e:
            logging.error(f"Error en página {page}: {str(e)}")
            return []

    def scrape_and_save(self):
        """Flujo principal simplificado."""
        try:
            existing_urls = self._get_existing_urls()
            new_urls = []
            
            # Procesar primera página para obtener total
            links = self.scrape_page(1)
            if not links:
                raise ValueError("No se encontraron autos en la página 1")
            
            count_text = BeautifulSoup(requests.get(self.BASE_URL, headers=self.HEADERS).text, 'html.parser') \
                       .select_one('div.s-results__count').get_text(strip=True)
            total_pages = math.ceil(int(count_text.split()[0]) / 20)

            # Procesar todas las páginas
            for page in range(1, total_pages + 1):
                for link in self.scrape_page(page):
                    full_url = f"https://neoauto.com/{link['href'].removeprefix('/auto')}"
                    
                    if full_url in existing_urls:
                        continue
                        
                    brand = self._get_valid_brand(self._extract_brand_from_url(link['href']))
                    if brand:
                        new_urls.append({
                            "url": full_url,
                            "marca": brand,
                            "fecha_extraccion": datetime.now().isoformat(),
                            "procesado": False
                        })

            # Guardar resultados
            if new_urls:
                self.supabase.table('urls_autos_diarios').insert(new_urls).execute()
                logging.info(f"Guardadas {len(new_urls)} URLs nuevas")

        except Exception as e:
            logging.critical(f"Error: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        scraper = NeoAutoDailyScraper()
        scraper.scrape_and_save()
    except KeyboardInterrupt:
        logging.warning("Proceso detenido manualmente")
    except Exception as e:
        logging.critical(f"Error: {str(e)}")
        sys.exit(1)
