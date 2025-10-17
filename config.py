"""
Configuración global del proyecto de optimización energética.
Este módulo centraliza las rutas, constantes y configuraciones del proyecto.
MODIFICADO PARA FUNCIONAR TANTO EN DESARROLLO COMO EN EJECUTABLE
"""

import os
import sys
import platform
import logging
from pathlib import Path

# ==================== DETECTAR SI ESTAMOS EN UN EJECUTABLE ====================
def get_base_path():
    """
    Detecta si estamos corriendo desde un ejecutable de PyInstaller
    o desde el código fuente, y retorna el directorio base correcto.
    """
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        # Corriendo desde un ejecutable de PyInstaller
        # sys._MEIPASS es el directorio temporal donde PyInstaller extrae los archivos
        base_path = Path(sys._MEIPASS)
        print(f"🔧 Modo: EJECUTABLE (PyInstaller)")
        print(f"📁 Base path: {base_path}")
    else:
        # Corriendo desde código fuente
        base_path = Path(__file__).parent.resolve()
        print(f"🔧 Modo: DESARROLLO")
        print(f"📁 Base path: {base_path}")
    
    return base_path

# Obtener el directorio base
BASE_DIR = get_base_path()

# ==================== CONFIGURACIÓN DE LOGGING ====================
# Crear archivo de log en el directorio del usuario para que sea persistente
LOG_DIR = Path.home() / "OptimizacionEnergia" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "energia_pyomo.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.info(f"Iniciando aplicación desde: {BASE_DIR}")

# ==================== RUTAS DEL PROYECTO ====================
# En ejecutable, crear carpetas en el directorio del usuario
if getattr(sys, 'frozen', False):
    # Modo ejecutable: usar directorio del usuario
    USER_DIR = Path.home() / "OptimizacionEnergia"
    DATA_DIR = USER_DIR / "data"
    OFERTAS_DIR = USER_DIR / "OFERTAS"
    OUTPUT_DIR = USER_DIR / "output"
    
    # Crear carpetas si no existen
    USER_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    OFERTAS_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    logger.info(f"Carpetas de trabajo en: {USER_DIR}")
else:
    # Modo desarrollo: usar carpetas relativas al proyecto
    DATA_DIR = BASE_DIR / "data"
    OFERTAS_DIR = BASE_DIR / "OFERTAS"
    OUTPUT_DIR = BASE_DIR / "output"
    
    # Asegurar que los directorios existan
    DATA_DIR.mkdir(exist_ok=True)
    OFERTAS_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)

# Archivos principales
DATOS_INICIALES = DATA_DIR / "datos_iniciales.xlsx"
RESULTADO_OFERTAS = OUTPUT_DIR / "resultado_ofertas.xlsx"
ESTADISTICAS_OFERTAS = OUTPUT_DIR / "estadisticas_ofertas.xlsx"

# ==================== CONFIGURACIÓN DEL SOLVER CBC ====================
SYSTEM = platform.system()

def find_cbc_solver():
    """
    Busca el solver CBC en varios lugares posibles.
    Prioridad:
    1. En la carpeta CBC del ejecutable/proyecto
    2. En el PATH del sistema
    3. En ubicaciones estándar del sistema operativo
    """
    # Opción 1: Carpeta CBC en el proyecto/ejecutable
    if SYSTEM == "Windows":
        cbc_in_project = BASE_DIR / "CBC" / "cbc.exe"
    else:
        cbc_in_project = BASE_DIR / "CBC" / "cbc"
    
    if cbc_in_project.exists():
        logger.info(f"✅ CBC encontrado en proyecto: {cbc_in_project}")
        return cbc_in_project
    
    # Opción 2: Buscar en el PATH del sistema
    import shutil
    cbc_in_path = shutil.which("cbc")
    if cbc_in_path:
        logger.info(f"✅ CBC encontrado en PATH: {cbc_in_path}")
        return Path(cbc_in_path)
    
    # Opción 3: Ubicaciones estándar según el sistema operativo
    if SYSTEM == "Darwin":  # macOS
        standard_paths = [
            Path("/usr/local/bin/cbc"),
            Path("/opt/homebrew/bin/cbc"),
            Path("/opt/local/bin/cbc")
        ]
    elif SYSTEM == "Linux":
        standard_paths = [
            Path("/usr/bin/cbc"),
            Path("/usr/local/bin/cbc")
        ]
    else:  # Windows
        standard_paths = []
    
    for path in standard_paths:
        if path.exists():
            logger.info(f"✅ CBC encontrado en ubicación estándar: {path}")
            return path
    
    # No se encontró
    logger.warning("⚠️ No se encontró el solver CBC")
    return None

CBC_PATH = find_cbc_solver()

# ==================== CONSTANTES DEL MODELO ====================
DEFAULT_K_FACTOR = 1.5  # Factor k por defecto para la evaluación de ofertas

def get_solver_path():
    """
    Retorna la ruta al solver CBC.
    
    Returns:
        str: Ruta al ejecutable de CBC
        
    Raises:
        FileNotFoundError: Si no se encuentra el solver
    """
    if CBC_PATH and CBC_PATH.exists():
        return str(CBC_PATH)
    
    # Intentar una última vez en el PATH
    import shutil
    cbc_in_path = shutil.which("cbc")
    if cbc_in_path:
        return cbc_in_path
    
    # Error: no se encontró
    raise FileNotFoundError(
        "No se pudo encontrar el solver CBC.\n\n"
        "Soluciones:\n"
        "1. Instale CBC y agregue su ubicación al PATH del sistema\n"
        "2. Coloque cbc.exe en la carpeta CBC del programa\n"
        "3. Para Windows: Descargue desde https://www.coin-or.org/download/binary/Cbc/\n"
        "4. Para macOS: brew install cbc\n"
        "5. Para Linux: apt-get install coinor-cbc"
    )

# ==================== INFORMACIÓN DE VERSIÓN ====================
VERSION = "1.0.0"
APP_NAME = "Sistema de Optimización Energética"

# Imprimir información de inicio
logger.info(f"{'='*60}")
logger.info(f"{APP_NAME} v{VERSION}")
logger.info(f"Sistema Operativo: {SYSTEM}")
logger.info(f"Python: {sys.version}")
logger.info(f"Directorio Base: {BASE_DIR}")
logger.info(f"Directorio de Datos: {DATA_DIR}")
logger.info(f"Directorio de Salida: {OUTPUT_DIR}")
logger.info(f"Solver CBC: {CBC_PATH if CBC_PATH else 'NO ENCONTRADO'}")
logger.info(f"{'='*60}")