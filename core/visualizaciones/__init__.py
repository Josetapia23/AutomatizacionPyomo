"""
Módulo de visualizaciones para el sistema de optimización energética.
FUSIONADO: Incluye todas las gráficas existentes + nuevas gráficas por oferta + tabla energía faltante horaria + tablas resumen anual.
"""

# Importar función principal de reportes (FUSIONADA)
from .reportes import generar_reporte_completo_mejorado

# Importar funciones básicas
from .basicas import (
    crear_grafica_principal_energia_asignada,
    crear_grafica_resumen_general,
    crear_grafica_torta_adjudicacion
)

# Importar funciones avanzadas (MANTENER todas las del sistema actual)
from .avanzadas import (
    crear_mapa_calor_mensual,
    crear_distribucion_por_agente,
    crear_tabla_energia_faltante_horaria  # 🔄 MANTENER esta función
)

# 🆕 NUEVO: Importar funciones por oferta (TODAS las nuevas funciones)
from .por_oferta import (
    crear_graficas_por_oferta,
    crear_grafica_oferta_individual,
    extraer_datos_para_grafica_oferta,
    generar_reporte_consolidado_ofertas,
    crear_grafica_consolidada_ofertas,        
    crear_graficas_por_oferta_completo        
)

# 🆕 NUEVO: Importar funciones de tablas resumen anual
from .tablas_anuales import (
    generar_tablas_resumen_anual,
    extraer_datos_mensuales,
    calcular_tablas_resumen,
    crear_figura_tabla_individual,
    crear_html_con_scroll_individual
)

# Función de compatibilidad para main.py (ACTUALIZADA)
def generar_reporte_completo(resultados_dict, ofertas_df, archivo_salida):
    """
    Función legada para mantener compatibilidad con main.py.
    Redirige a la implementación FUSIONADA en el submódulo.
    
    Args:
        resultados_dict (dict): Resultados de la optimización
        ofertas_df (DataFrame): DataFrame con ofertas originales
        archivo_salida (str): Archivo base para generar el reporte
        
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    print("📊 Generando visualizaciones usando el sistema FUSIONADO completo...")
    return generar_reporte_completo_mejorado(resultados_dict, ofertas_df, archivo_salida)

# Exportar funciones principales (LISTA COMPLETA)
__all__ = [
    # 🔄 FUNCIÓN PRINCIPAL (compatibilidad con main.py)
    'generar_reporte_completo',
    'generar_reporte_completo_mejorado',
    
    # 📊 GRÁFICAS BÁSICAS
    'crear_grafica_principal_energia_asignada',
    'crear_grafica_resumen_general',
    'crear_grafica_torta_adjudicacion',
    
    # 🔬 GRÁFICAS AVANZADAS
    'crear_mapa_calor_mensual',
    'crear_distribucion_por_agente',
    'crear_tabla_energia_faltante_horaria',  
    
    # 🆕 GRÁFICAS POR OFERTA (NUEVAS FUNCIONES)
    'crear_graficas_por_oferta',
    'crear_grafica_oferta_individual',
    'extraer_datos_para_grafica_oferta',
    'generar_reporte_consolidado_ofertas',
    'crear_grafica_consolidada_ofertas',
    'crear_graficas_por_oferta_completo',
    
    
    'generar_tablas_resumen_anual',
    'extraer_datos_mensuales',
    'calcular_tablas_resumen',
    'crear_figura_tabla_individual',
    'crear_html_con_scroll_individual'
]