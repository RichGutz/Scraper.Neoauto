# orchestrator.py
import os
import subprocess
import time
from pathlib import Path
from colorama import Fore, Style, init

# Configuración inicial
init(autoreset=True)
SCRIPTS = [
    "script1_navegacion.py",
    "script2_descarga.py",
    "script3_conversion.py"
]
URL_FEED = "URL_FEED.txt"
OUTPUT_FOLDERS = ["resultados_txt", "resultados_json"]

def verificar_estructura():
    """Valida que existan todos los archivos necesarios"""
    errores = []
    
    # Verificar scripts
    for script in SCRIPTS:
        if not Path(script).exists():
            errores.append(f"Script faltante: {script}")
    
    # Verificar URL_FEED
    if not Path(URL_FEED).exists():
        errores.append(f"Archivo {URL_FEED} no encontrado")
    
    # Verificar contenido de URL_FEED
    try:
        with open(URL_FEED, 'r') as f:
            if len(f.readlines()) < 1:
                errores.append(f"{URL_FEED} está vacío")
    except Exception as e:
        errores.append(f"Error leyendo {URL_FEED}: {str(e)}")
    
    return errores

def limpiar_outputs():
    """Elimina los resultados anteriores"""
    for folder in OUTPUT_FOLDERS:
        try:
            for file in Path(folder).glob("*"):
                file.unlink()
            print(f"{Fore.YELLOW}🗑️  Carpeta {folder} limpiada")
        except Exception as e:
            print(f"{Fore.RED}⚠️  Error limpiando {folder}: {e}")

def ejecutar_script(script_name):
    """Ejecuta un script y muestra su output en tiempo real"""
    print(f"\n{Fore.CYAN}⚡ Ejecutando {script_name}...{Style.RESET_ALL}")
    
    try:
        proceso = subprocess.Popen(
            ["python", script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Mostrar output en tiempo real
        while True:
            output = proceso.stdout.readline()
            if output == '' and proceso.poll() is not None:
                break
            if output:
                print(output.strip())
        
        # Verificar errores
        if proceso.returncode != 0:
            print(f"{Fore.RED}❌ Error en {script_name}:")
            print(proceso.stderr.read())
            return False
        return True
    
    except Exception as e:
        print(f"{Fore.RED}🚨 Error ejecutando {script_name}: {e}")
        return False

def mostrar_resultados():
    """Muestra estadísticas de los resultados"""
    print(f"\n{Fore.GREEN}📊 RESULTADOS FINALES")
    print("="*40)
    
    for folder in OUTPUT_FOLDERS:
        archivos = list(Path(folder).glob("*"))
        print(f"{Fore.MAGENTA}📂 {folder}:")
        print(f"{Fore.CYAN}   • Archivos generados: {len(archivos)}")
        
        # Mostrar primeros 3 archivos como ejemplo
        for i, archivo in enumerate(archivos[:3]):
            print(f"     {i+1}. {archivo.name}")
        
        if len(archivos) > 3:
            print(f"     ... y {len(archivos)-3} más")

def main():
    print(f"\n{Fore.YELLOW}=== ORQUESTADOR DE SCRAPING ===")
    print(f"{Style.DIM}Scripts: {', '.join(SCRIPTS)}{Style.RESET_ALL}\n")
    
    # Paso 1: Verificar estructura
    errores = verificar_estructura()
    if errores:
        print(f"{Fore.RED}❌ Errores previos a la ejecución:")
        for error in errores:
            print(f" • {error}")
        return
    
    # Paso 2: Limpiar outputs anteriores
    print(f"{Fore.YELLOW}🛠️  Preparando entorno...")
    limpiar_outputs()
    
    # Paso 3: Ejecutar scripts en serie
    for script in SCRIPTS:
        if not ejecutar_script(script):
            print(f"{Fore.RED}⛔ Deteniendo orquestación debido a errores")
            return
        time.sleep(2)  # Pequeña pausa entre scripts
    
    # Paso 4: Mostrar resumen
    mostrar_resultados()
    print(f"\n{Fore.GREEN}✅ Proceso completado exitosamente!")

if __name__ == "__main__":
    main()