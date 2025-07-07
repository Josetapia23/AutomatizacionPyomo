"""
Módulo de visualizaciones para el sistema de optimización energética.
"""

# Importar función principal de reportes
from .reportes import generar_reporte_completo_mejorado

# Importar funciones básicas
from .basicas import (
    crear_grafica_principal_energia_asignada,
    crear_grafica_resumen_general,
    crear_grafica_torta_adjudicacion
)

# Importar funciones avanzadas
from .avanzadas import (
    crear_mapa_calor_mensual,
    crear_distribucion_por_agente,
    crear_tabla_energia_faltante_horaria  
)

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
    print("📊 Generando visualizaciones usando el sistema mejorado...")
    return generar_reporte_completo_mejorado(resultados_dict, ofertas_df, archivo_salida)

# Exportar funciones principales
__all__ = [
    'generar_reporte_completo',
    'generar_reporte_completo_mejorado',
    'crear_grafica_principal_energia_asignada',
    'crear_grafica_resumen_general',
    'crear_grafica_torta_adjudicacion',
    'crear_mapa_calor_mensual',
    'crear_distribucion_por_agente',
    'crear_tabla_energia_faltante_horaria' 
]