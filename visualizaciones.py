"""
Módulo para generar visualizaciones del sistema de optimización energética.
Este archivo principal importa las funciones específicas de los submódulos.
"""

# Importar funciones específicas para mantener compatibilidad con main.py
from core.visualizaciones.reportes import generar_reporte_completo_mejorado

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