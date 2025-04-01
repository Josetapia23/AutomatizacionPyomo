"""
Configuración global del proyecto de optimización energética.
Este módulo centraliza las rutas, constantes y configuraciones del proyecto.
"""

import os
import platform
import logging
from pathlib import Path

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("energia_pyomo.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Rutas del proyecto (independientes del sistema operativo)
BASE_DIR = Path(__file__).parent.resolve()
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

# Detectar sistema operativo y configurar el solver de Pyomo
SYSTEM = platform.system()
if SYSTEM == "Windows":
    CBC_PATH = BASE_DIR / "CBC" / "cbc.exe"
elif SYSTEM == "Darwin":  # macOS
    # Usar homebrew installation path como predeterminado
    CBC_PATH = Path("/usr/local/bin/cbc")
    if not CBC_PATH.exists():
        # Alternativas comunes en macOS
        alternatives = [
            Path("/opt/homebrew/bin/cbc"),
            Path("/opt/local/bin/cbc")
        ]
        for alt in alternatives:
            if alt.exists():
                CBC_PATH = alt
                break
else:  # Linux y otros
    CBC_PATH = Path("/usr/bin/cbc")

# Verificar existencia del solver
if not CBC_PATH.exists():
    logger.warning(f"No se encontró el solver CBC en {CBC_PATH}. "
                  f"Por favor, instálelo manualmente y configure la ruta correcta.")
    
# Constantes del modelo
DEFAULT_K_FACTOR = 1.5  # Factor k por defecto para la evaluación de ofertas

def get_solver_path():
    """Retorna la ruta al solver CBC, considerando el sistema operativo."""
    if CBC_PATH.exists():
        return str(CBC_PATH)
    else:
        # Intentar encontrar el solver en el PATH del sistema
        import shutil
        cbc_in_path = shutil.which("cbc")
        if cbc_in_path:
            return cbc_in_path
        else:
            raise FileNotFoundError(f"No se pudo encontrar el solver CBC. Por favor, instálelo y configure la ruta correcta en config.py")