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
            "xpath=//div[contains(@class, 'question')]//button[contains(@class, 'close')]",
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

def get_urls_from_supabase(table_name: str, limit: int = None) -> list:
    """Obtiene URLs desde Supabase."""
    supabase = get_supabase_client()
    
    try:
        query = supabase.table(table_name).select("url")
        
        if table_name == "urls_autos_diarios":
            query = query.eq('procesado', False)
        elif table_name == "urls_autos_random":
            query = query.order('last_scraped', ascending=True)  # Prioriza URLs menos recientes
            
        if limit:
            query = query.limit(limit)
            
        response = query.execute()
        
        urls = [item['url'] for item in response.data if item and 'url' in item]
        print(f"Obtenidas {len(urls)} URLs desde {table_name}")
        return urls
    except Exception as e:
        print(f"Error obteniendo URLs de {table_name}: {e}")
        return []

def update_supabase_status(table_name: str, url: str):
    """Actualiza el estado en Supabase."""
    supabase = get_supabase_client()
    
    try:
        if table_name == "urls_autos_diarios":
            response = supabase.table(table_name).update(
                {'procesado': True}
            ).eq('url', url).execute()
        elif table_name == "urls_autos_random":
            response = supabase.table(table_name).update(
                {'last_scraped': datetime.now().isoformat()}
            ).eq('url', url).execute()
            
        if len(response.data) > 0:
            print(f"Actualizado Supabase ({table_name}): {url}")
        else:
            print(f"URL no encontrada en {table_name}: {url}")
            
    except Exception as e:
        print(f"Error actualizando {table_name}: {e}")

# ----------------------------
# EJECUCIÓN PRINCIPAL
# ----------------------------

def run_scraping_session(urls: list, table_name: str):
    """Ejecuta scraping para un lote de URLs y actualiza Supabase."""
    if not urls:
        print(f"No hay URLs para procesar de {table_name}")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768}
        )
        page = context.new_page()
        output_dir = ensure_results_dir()

        for url in urls:
            if not url.startswith('https://neoauto.com/'):
                print(f"URL inválida: {url}")
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

                    # Actualizar Supabase
                    update_supabase_status(table_name, url)
                    
                # Espera aleatoria entre requests
                time.sleep(random.uniform(2, 5))

            except Exception as e:
                print(f"Error procesando {url}: {e}")
                continue

        page.close()
        context.close()
        browser.close()

def main():
    """Función principal para ejecución automática desde CRON."""
    print("\n" + "="*50)
    print(f"Iniciando scraping automático - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50 + "\n")

    # Primero procesamos URLs diarias (no procesadas)
    print("\n>>> Procesando URLs DIARIAS (no procesadas) <<<")
    diarias_urls = get_urls_from_supabase("urls_autos_diarios")
    run_scraping_session(diarias_urls, "urls_autos_diarios")

    # Luego procesamos URLs semanales (priorizando las menos recientes)
    print("\n>>> Procesando URLs SEMANALES (menos recientes) <<<")
    semanales_urls = get_urls_from_supabase("urls_autos_random", limit=20)  # Límite para no sobrecargar
    run_scraping_session(semanales_urls, "urls_autos_random")

    print("\n" + "="*50)
    print("Proceso completado exitosamente")
    print("="*50 + "\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERROR CRÍTICO: {e}")
        sys.exit(1)