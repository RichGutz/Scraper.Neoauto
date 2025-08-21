"""
Scraper Detallado de Anuncios de Vehículos en NeoAuto.

Este script es el núcleo del proceso de extracción de datos. Utiliza Playwright
para controlar un navegador headless, permitiendo una interacción avanzada con
las páginas de anuncios de NeoAuto, que dependen en gran medida de JavaScript.

Funcionalidad Principal:
1.  **Obtención de URLs**: Consulta dos tablas en Supabase para obtener las URLs
    que necesita procesar:
    - `urls_autos_diarios`: URLs nuevas del día, marcadas como no procesadas.
    - `urls_autos_random`: Una selección de URLs históricas para re-scrapeo y
      seguimiento, priorizando las que no han sido visitadas recientemente.

2.  **Navegación Humanizada**: Emula un comportamiento de usuario para evitar
    ser detectado como un bot. Esto incluye:
    - Rotación de User-Agents.
    - Navegación a través de una serie de pasos lógicos (p. ej., página
      principal -> categoría -> anuncio) en lugar de acceder directamente a la URL.
    - Manejo robusto de pop-ups (cookies, suscripciones, encuestas).
    - Simulación de scroll para asegurar que todo el contenido dinámico de la
      página se cargue correctamente antes de la extracción.

3.  **Extracción de Datos Detallada**: Una vez en la página del anuncio, extrae
    información clave como:
    - Título, precio, kilometraje, ubicación, año, transmisión.
    - La descripción completa del anuncio.

4.  **Detección de "Único Dueño"**: Analiza la descripción del vehículo utilizando
    un conjunto de reglas predefinidas (`reglas_unico_dueno.json`) para determinar
    si el vendedor lo ha listado como de "único dueño".

5.  **Almacenamiento Local**: Guarda toda la información extraída de un anuncio
    en un archivo de texto (`.txt`) único en la carpeta `results_txt`.
    El nombre del archivo se genera con un hash de la URL y un timestamp para
    evitar colisiones.

6.  **Actualización de Estado en Supabase**: Tras procesar exitosamente una URL,
    actualiza su estado en la tabla correspondiente de Supabase para marcarla
    como `procesado: True` (en `urls_autos_diarios`) o actualizar su fecha de
    última visita (`last_scraped` en `urls_autos_random`).

Este script está diseñado para ser robusto y resiliente, manejando errores de
red y particularidades del sitio web de NeoAuto.
"""
#!/usr/bin/env python3
# scraper_neoauto_cron.py - Versión optimizada exclusivamente para CRON

import random
import time
from playwright.sync_api import sync_playwright
import json
import re
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from pathlib import Path
from datetime import datetime
import hashlib
import sys

# ----------------------------
# CONFIGURACIÓN INICIAL
# ----------------------------

load_dotenv()  # Cargar variables de entorno al inicio

# Configuración de rutas y directorios
def ensure_results_dir():
    """Crea la carpeta 'results_txt' si no existe."""
    results_dir = Path(__file__).parent / "results_txt"
    results_dir.mkdir(exist_ok=True)
    return results_dir

def generate_output_filename(url: str, origen: str = "auto"):
    """Genera un nombre de archivo único para cada resultado con prefijo de origen."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    url_hash = hashlib.md5(url.encode()).hexdigest()[:6]
    return f"{origen}_result_{timestamp}_{url_hash}.txt"

# User Agents para rotación
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.134 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
]

# Carga de reglas para la detección de "único dueño"
try:
    rules_path = Path(__file__).parent / 'reglas_unico_dueno.json'
    with open(rules_path, 'r', encoding='utf-8') as f:
        rules = json.load(f)
        UNIQUE_OWNER_PHRASES = rules.get('frases_clave', [])
        EXCLUSION_PHRASES = rules.get('exclusiones', [])
        print(f"Reglas cargadas: {len(UNIQUE_OWNER_PHRASES)} frases clave y {len(EXCLUSION_PHRASES)} exclusiones")
except Exception as e:
    print(f"Error cargando reglas: {e}")
    UNIQUE_OWNER_PHRASES = ["único dueño", "un solo dueño", "dueño único", "1 dueño"]
    EXCLUSION_PHRASES = ["no único dueño", "varios dueños", "segundo dueño"]

# ----------------------------
# FUNCIONES DE AYUDA
# ----------------------------

def check_unique_owner(descripcion: str) -> bool:
    """Verifica si la descripción cumple con las reglas de único dueño."""
    if not descripcion:
        return False
    
    desc_lower = descripcion.lower()
    
    if any(excl.lower() in desc_lower for excl in EXCLUSION_PHRASES):
        return False
    
    return any(phrase.lower() in desc_lower for phrase in UNIQUE_OWNER_PHRASES)

def handle_cookie_popup(page):
    """Intenta cerrar el popup de cookies si aparece."""
    try:
        consent_button = page.wait_for_selector(
            "button:has-text('Consentir'), button:has-text('Aceptar'), button#truste-consent-button",
            timeout=5000
        )
        consent_button.click()
        print("Popup de cookies cerrado")
        time.sleep(0.5)
    except Exception as e:
        print("No apareció popup de cookies")   

def handle_neopopups(page):
    """Cierra popups tardíos con selectores específicos."""
    for attempt in range(2):
        try:
            page.click("button[class*='close'] >> svg >> xpath=./ancestor::button", timeout=1000)
            print(f"Popup de satisfacción cerrado (intento {attempt+1})")
            return
        except Exception as e:
            print(f"Intento {attempt+1}: {str(e)}")
            time.sleep(1)

def handle_all_popups(page):
    """Maneja todos los popups emergentes de NeoAuto."""
    try:
        later_buttons = [
            "button:has-text('LATER')",
            "button:has-text('AHORA NO')",
            "button:has-text('DESPUÉS')",
            "xpath=//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'later')]"
        ]
        
        for selector in later_buttons:
            try:
                page.click(selector, timeout=2000)
                print("Cerrado popup 'Suscribirse'")
                break
            except:
                continue

        satisfaction_selectors = [
            "div[class*='satisfaction'] button[class*='close']",
            "div[class*='survey'] button[aria-label='Cerrar']",
            "xpath=//div[contains(@class, 'question')]//button[contains(@class, 'close')",
            "xpath=//div[contains(text(), 'satisfech')]/ancestor::div//button[1]"
        ]
        
        for selector in satisfaction_selectors:
            try:
                page.click(selector, timeout=2000)
                print("Cerrado popup 'Satisfacción'")
                break
            except:
                continue

    except Exception as e:
        print(f"Error manejando popups: {e}")

def handle_satisfaction_popup(page):
    """Cierra específicamente el popup de satisfacción usando el elemento exacto."""
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            page.click("div.ps-pnf-trigger-arrow", timeout=2000)
            print(f"Popup de satisfacción minimizado (intento {attempt+1})")
            return True
            
        except Exception as e:
            try:
                container = page.wait_for_selector("div.ps-pnf-container", timeout=2000)
                arrow = container.wait_for_selector("div.ps-pnf-trigger-arrow", timeout=2000)
                arrow.click()
                print(f"Popup cerrado via contenedor padre (intento {attempt+1})")
                return True
            except:
                print(f"Intento {attempt+1}: Popup no encontrado")
                time.sleep(1)
    
    print("No se pudo cerrar el popup de satisfacción")
    return False

# ----------------------------
# FUNCIÓN PRINCIPAL DE SCRAPING
# ----------------------------

def advanced_scraping(url: str, page):
    """
    Función MODIFICADA con nuevo flujo:
    1. Scroll INMEDIATO al llegar a target
    2. Luego manejo de popups
    3. Finalmente extracción
    """
    print(f"\nIniciando extracción para: {url}")
    
    try:
        print("Iniciando navegación...")
        
        base_url = "https://neoauto.com/"
        caminos_posibles = []

        marca = None
        try:
            url_slug = url.split("/auto/")[1].split("/", 1)[1]
            marca = url_slug.split('-')[0].lower()
            print(f"Marca extraída: '{marca}'")
        except Exception:
            print("No se pudo extraer la marca")

        if "/seminuevo/" in url:
            caminos_posibles.append([
                ("Página Principal", base_url),
                ("Categoría 'Venta de Autos'", f"{base_url}venta-de-autos"),
                ("URL Final", url)
            ])
            caminos_posibles.append([
                ("Página Principal", base_url),
                ("Categoría 'Seminuevos'", f"{base_url}venta-de-autos-seminuevos"),
                ("URL Final", url)
            ])

        if marca:
            caminos_posibles.append([
                ("Página Principal", base_url),
                ("Categoría 'Usados'", f"{base_url}venta-de-autos-usados"),
                (f"Filtro por Marca '{marca.upper()}'", f"{base_url}venta-de-autos-usados-{marca}"),
                ("URL Final", url)
            ])
        
        if not caminos_posibles:
            print("Navegando directamente a la URL final.")
            page.goto(url, timeout=90000, wait_until="domcontentloaded")
            time.sleep(1)

            # ==== SCROLL INICIAL OBLIGATORIO ====
            print("Scroll INICIAL para activar contenido...")
            for _ in range(3):  # 3 ciclos completos
                page.mouse.wheel(0, 800)  # Scroll down
                time.sleep(1.2)
                page.mouse.wheel(0, -400)  # Scroll up
                time.sleep(0.8)

            # ==== AHORA MANEJAR POPUPS ====
            handle_cookie_popup(page)
            handle_neopopups(page)
            handle_all_popups(page)
            handle_satisfaction_popup(page)
            print("Popups manejados POST-scroll")

        else:
            camino_elegido = random.choice(caminos_posibles)
            print(f"Camino elegido ({len(camino_elegido)} pasos).")
            
            for i, (descripcion_paso, url_paso) in enumerate(camino_elegido):
                print(f"Paso {i+1}/{len(camino_elegido)}: {descripcion_paso}...")
                wait_option = "domcontentloaded"
                page.goto(url_paso, timeout=90000, wait_until=wait_option)
                
                if i == len(camino_elegido) - 1:  # Si es URL target
                    # ==== SCROLL INICIAL OBLIGATORIO ====
                    print("Scroll INICIAL para activar contenido...")
                    for _ in range(3):  # 3 ciclos completos
                        page.mouse.wheel(0, 800)  # Scroll down
                        time.sleep(1.2)
                        page.mouse.wheel(0, -400)  # Scroll up
                        time.sleep(0.8)

                    # ==== AHORA MANEJAR POPUPS ====
                    handle_cookie_popup(page)
                    handle_neopopups(page)
                    handle_all_popups(page)
                    handle_satisfaction_popup(page)
                    print("Popups manejados POST-scroll")
                else:
                    print("Navegación rápida (sin acciones)")

        # Scroll adicional para asegurar carga completa
        print("Scroll adicional de confirmación...")
        for _ in range(2):
            page.mouse.wheel(0, 500)
            time.sleep(0.7)
            page.mouse.wheel(0, -200)
            time.sleep(0.5)

        # Extracción de datos
        data = {
            'url': url,
            'titulo': None,
            'precio': None,
            'kilometraje': None,
            'ubicacion': None,
            'transmision': None,
            'anio': None,
            'descripcion': None,
            'unico_dueno': False,
            'fecha_extraccion': time.strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            data['titulo'] = page.evaluate('''() => {
                return document.querySelector('h1')?.innerText.trim() || document.title;
            }''')
            if data['titulo']:
                year_match = re.search(r'(20\d{2}|\d{4})$', data['titulo'])
                data['anio'] = int(year_match.group(0)) if year_match else None
            print(f"Título: {data['titulo']}")
        except Exception as e:
            print(f"Error con título: {str(e)}")

        try:
            data['precio'] = page.evaluate('''() => {
                const priceText = document.querySelector('span.text-title-x-large')?.innerText.trim();
                return priceText ? parseFloat(priceText.replace(/[^\d.]/g, '')) : null;
            }''')
            if data['precio']:
                print(f"Precio: S/ {data['precio']:,.2f}")
        except Exception as e:
            print(f"Error con precio: {str(e)}")

        try:
            data['kilometraje'] = page.evaluate('''() => {
                const kmElement = Array.from(document.querySelectorAll('div')).find(el => 
                    el.textContent.includes('km')
                );
                return kmElement ? parseInt(kmElement.textContent.replace(/[^\d]/g, '')) : null;
            }''')
            if data['kilometraje']:
                print(f"Kilometraje: {data['kilometraje']:,} km")
        except Exception as e:
            print(f"Error con kilometraje: {str(e)}")

        try:
            data['ubicacion'] = page.evaluate('''() => {
                const locationDiv = Array.from(document.querySelectorAll('div')).find(el => 
                    el.innerHTML.includes('M12 12.658')
                );
                return locationDiv?.innerText.trim();
            }''')
            print(f"Ubicación: {data['ubicacion']}" if data['ubicacion'] else "Sin ubicación")
        except Exception as e:
            print(f"Error con ubicación: {str(e)}")

        try:
            data['transmision'] = page.evaluate('''() => {
                const transmissionDiv = Array.from(document.querySelectorAll('div')).find(el => 
                    el.textContent.includes('Transmisión')
                );
                return transmissionDiv?.nextElementSibling?.textContent.trim();
            }''')
            if data['transmision']:
                print(f"Transmisión: {data['transmision']}")
        except Exception as e:
            print(f"Error con transmisión: {str(e)}")
        
        try:
            data['descripcion'] = page.evaluate('''() => {
                const sections = Array.from(document.querySelectorAll('section, div'));
                const descSection = sections.find(el => 
                    el.textContent.trim().toLowerCase().includes('descripción')
                );
                return descSection?.textContent.replace(/Descripción/gi, '').trim();
            }''')
            if data['descripcion']:
                data['unico_dueno'] = check_unique_owner(data['descripcion'])
                print(f"Descripción: {data['descripcion'][:100]}...")
                print(f"Único dueño: {'Sí' if data['unico_dueno'] else 'No'}")
        except Exception as e:
            print(f"Error con descripción: {str(e)}")

        return data
        
    except Exception as e:
        print(f"Error en scraping: {str(e)}")
        return None

# ----------------------------
# FUNCIONES DE SUPABASE
# ----------------------------

def get_supabase_client():
    """Crea y retorna un cliente Supabase."""
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        raise ValueError("Faltan credenciales de Supabase en .env")

    return create_client(supabase_url, supabase_key)

def get_next_url(table_name: str, sort_desc: bool) -> str | None:
    """
    Obtiene la siguiente URL no procesada de forma atómica usando un RPC de Supabase.
    Marca la URL como procesada y la devuelve.
    """
    supabase = get_supabase_client()
    try:
        params = {
            'p_table_name': table_name,
            'p_sort_desc': sort_desc
        }
        response = supabase.rpc('get_next_unprocessed_url', params).execute()
        
        # La respuesta de un RPC exitoso con datos está en response.data
        if response.data:
            url = response.data
            print(f"URL obtenida y bloqueada desde {table_name}: {url}")
            return url
        else:
            # Si no hay data, puede ser que no haya más URLs o un error no lanzado
            print(f"No se encontraron más URLs no procesadas en {table_name}.")
            return None
            
    except Exception as e:
        print(f"Error llamando al RPC get_next_unprocessed_url para {table_name}: {e}")
        return None

def get_unprocessed_url_count(table_name: str) -> int:
    """
    Obtiene el número de URLs no procesadas de una tabla específica.
    """
    supabase = get_supabase_client()
    try:
        params = {
            'p_table_name': table_name
        }
        response = supabase.rpc('get_unprocessed_count', params).execute()

        if response.data is not None:
            count = response.data
            return count
        else:
            print(f"No se pudo obtener el conteo de URLs no procesadas para {table_name}.")
            return 0
    except Exception as e:
        print(f"Error llamando al RPC get_unprocessed_count para {table_name}: {e}")
        return 0

# ----------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------

def run_scraping_session(table_name: str, sort_desc: bool = False):
    """
    Ejecuta scraping pidiendo URLs una por una de forma atómica.
    El bucle termina cuando el RPC no devuelve más URLs.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768}
        )
        page = context.new_page()
        output_dir = ensure_results_dir()

        processed_count = 0
        while True:
            # Mostrar el conteo de URLs restantes al inicio de cada iteración
            remaining_count = get_unprocessed_url_count(table_name)
            print(f"URLs restantes en {table_name}: {remaining_count}")

            if remaining_count == 0:
                print(f"No hay más URLs para procesar en {table_name}. Finalizando.")
                break

            url = get_next_url(table_name, sort_desc)

            if not url:
                # This case should ideally be caught by remaining_count == 0,
                # but keeping it for robustness if RPC returns None unexpectedly.
                print(f"No hay más URLs para procesar en {table_name}. Finalizando.")
                break

            if not url.startswith('https://neoauto.com/'):
                print(f"URL inválida obtenida del RPC: {url}")
                continue

            try:
                data = advanced_scraping(url, page)
                
                if data:
                    # Guardar resultados
                    origen = "diario" if table_name == "urls_autos_diarios" else "semanal"
                    output_file = output_dir / generate_output_filename(url, origen)
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=4, ensure_ascii=False)
                    print(f"Datos guardados en: {output_file}")
                    processed_count += 1
                else:
                    # Si advanced_scraping devuelve None, es un fallo. La URL ya está
                    # marcada como 'procesado', que es el comportamiento que se quiere.
                    print(f"Scraping falló para {url}. Se considera procesada.")

                # Espera aleatoria entre requests
                time.sleep(random.uniform(2, 5))

            except Exception as e:
                print(f"Error mayor en el bucle de procesamiento para {url}: {e}")
                # La URL ya está marcada como procesada, así que podemos continuar.
                continue
        
        print(f"\nSesión para {table_name} completada. Total de URLs procesadas: {processed_count}")
        page.close()
        context.close()
        browser.close()

def main():
    """Función principal para ejecución automática desde CRON."""
    print("\n" + "="*50)
    print(f"Iniciando scraping automático - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50 + "\n")

    # --- Argument Parsing ---
    process_most_recent = False
    if len(sys.argv) > 1 and sys.argv[1] == '1':
        process_most_recent = True
        print("\n>>> MODO: PROCESANDO URLS MÁS RECIENTES PRIMERO <<<")
    else:
        print("\n>>> MODO: PROCESANDO URLS MENOS RECIENTES PRIMERO <<<")
    print("\n")
    # --- End Argument Parsing ---

    # Procesamos URLs diarias (siempre las más antiguas primero)
    print("\n>>> Procesando URLs DIARIAS <<<")
    run_scraping_session(table_name="urls_autos_diarios", sort_desc=False)

    # Procesamos URLs semanales/random
    print("\n>>> Procesando URLs SEMANALES <<<")
    run_scraping_session(table_name="urls_autos_random", sort_desc=process_most_recent)

    print("\n" + "="*50)
    print("Proceso completado exitosamente")
    print("="*50 + "\n")



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERROR CRÍTICO: {e}")
        sys.exit(1)
