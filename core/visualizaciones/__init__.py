"""
Módulo de visualizaciones para el sistema de optimización energética.
"""

# Importar funciones de los submódulos
from .basicas import *
from .avanzadas import *
from .reportes import *
from .utils import *

# Importar específicamente para compatibilidad
from .reportes import generar_reporte_completo_mejorado

# Función de compatibilidad para main.py
def generar_reporte_completo(resultados_dict, ofertas_df, archivo_salida):
    """
    Función legada para mantener compatibilidad con main.py.
    Redirige a la implementación actual en el submódulo.
    
    Args:
        resultados_dict (dict): Resultados de la optimización
        ofertas_df (DataFrame): DataFrame con ofertas originales
        archivo_salida (str): Archivo base para generar el reporte
        
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    print("Generando visualizaciones usando el nuevo sistema...")
    return generar_reporte_completo_mejorado(resultados_dict, ofertas_df, archivo_salida)