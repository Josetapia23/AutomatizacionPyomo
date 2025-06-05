"""
Visualizaciones avanzadas para el sistema de optimización energética.
VERSIÓN SIMPLIFICADA: Solo las funciones básicas por ahora.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging

from .utils import (
    format_number, convert_to_gwh, extract_dates_from_results,
    extract_hours_from_results, extract_offers_from_results,
    extract_years_months_from_dates, format_date_for_display,
    generate_color_scale
)

# Configurar logging
logger = logging.getLogger(__name__)

def crear_grafica_distribucion_horaria(resultados_dict):
    """
    GRÁFICA AUXILIAR: Distribución de energía por hora (versión simplificada).
    
    Similar a la gráfica principal pero con un enfoque diferente.
    Por ahora mantiene la funcionalidad existente.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Figura Plotly con la gráfica de distribución horaria
    """
    print("🔍 Creando gráfica de distribución horaria (versión simplificada)...")
    
    # Extraer datos por hora
    horas = list(range(1, 25))
    gwh_asignados = [0] * 24
    gwh_no_asignados = [0] * 24
    porcentaje_no_asignado = [0] * 24
    
    # Procesar demanda faltante para obtener datos por hora
    if "DEMANDA_FALTANTE" in resultados_dict:
        df_faltante = resultados_dict["DEMANDA_FALTANTE"]
        
        # Inicializar arrays para almacenar los totales
        demanda_total_por_hora = [0] * 24
        demanda_faltante_por_hora = [0] * 24
        
        # Sumar los valores para cada hora a través de todas las fechas
        for hora in horas:
            for _, row in df_faltante.iterrows():
                if hora in row and not pd.isna(row[hora]):
                    demanda_faltante_por_hora[hora-1] += row[hora]
        
        # Calcular la energía asignada por hora (todas las ofertas, todas las iteraciones)
        energia_asignada_por_hora = [0] * 24
        
        for key, df in resultados_dict.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                if "DEMANDA ASIGNADA" in key and "_COMPRAR" in key:
                    for hora in horas:
                        if hora in df.columns:
                            energia_asignada_por_hora[hora-1] += df[hora].sum()
        
        # Calcular la demanda total por hora (asignada + faltante)
        for hora in range(24):
            demanda_total_por_hora[hora] = energia_asignada_por_hora[hora] + demanda_faltante_por_hora[hora]
            
            # Convertir a GWh
            gwh_asignados[hora] = convert_to_gwh(energia_asignada_por_hora[hora])
            gwh_no_asignados[hora] = convert_to_gwh(demanda_faltante_por_hora[hora])
            
            # Calcular porcentaje de no asignación
            if demanda_total_por_hora[hora] > 0:
                porcentaje_no_asignado[hora] = (demanda_faltante_por_hora[hora] / demanda_total_por_hora[hora]) * 100
            else:
                porcentaje_no_asignado[hora] = 0
    
    # Crear figura
    fig = go.Figure()
    
    # Añadir barras apiladas para energía asignada y no asignada
    fig.add_trace(
        go.Bar(
            x=horas,
            y=gwh_asignados,
            name='GWh Asignados',
            marker_color='#14213D',  # Azul oscuro
            hovertemplate='Hora %{x}<br>GWh Asignados: %{y:.2f}<extra></extra>'
        )
    )
    
    fig.add_trace(
        go.Bar(
            x=horas,
            y=gwh_no_asignados,
            name='GWh No Asignado',
            marker_color='rgba(230, 230, 230, 0.7)',  # Gris claro
            hovertemplate='Hora %{x}<br>GWh No Asignado: %{y:.2f}<extra></extra>'
        )
    )
    
    # Añadir línea para el porcentaje de no asignación
    fig.add_trace(
        go.Scatter(
            x=horas,
            y=porcentaje_no_asignado,
            name='% No Asignado',
            yaxis='y2',
            line=dict(color='#48CAE4', width=3),  # Azul
            mode='lines+markers+text',
            marker=dict(size=8, symbol='circle', color='#48CAE4', line=dict(color='white', width=1)),
            text=[f"{p:.1f}%" for p in porcentaje_no_asignado],
            textposition='top center',
            textfont=dict(color='#48CAE4'),
            hovertemplate='Hora %{x}<br>% No Asignado: %{y:.1f}%<extra></extra>'
        )
    )
    
    # Actualizar diseño
    fig.update_layout(
        title_text="<b>DISTRIBUCIÓN HORARIA DE ENERGÍA</b>",
        barmode='stack',
        height=600,
        font=dict(family="Arial, sans-serif"),
        hoverlabel=dict(bgcolor="white"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        # Configurar ejes X e Y
        xaxis=dict(
            title="HORAS",
            tickvals=horas,
            gridcolor='rgba(0,0,0,0.1)'
        ),
        yaxis=dict(
            title="GWh",
            gridcolor='rgba(0,0,0,0.1)',
            side='left'
        ),
        # Configurar segundo eje Y para porcentaje
        yaxis2=dict(
            title="% No Asignado",
            overlaying='y',
            side='right',
            range=[0, max(porcentaje_no_asignado) * 1.2 if max(porcentaje_no_asignado) > 0 else 5],
            tickformat='.1f',
            ticksuffix='%',
            gridcolor='rgba(0,0,0,0)'
        )
    )
    
    print("✅ Gráfica de distribución horaria creada")
    return fig

def crear_mapa_calor_mensual(resultados_dict):
    """
    FUNCIÓN PLACEHOLDER: Mapa de calor mensual.
    
    Por ahora retorna None para evitar errores.
    Se implementará en la siguiente iteración.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        None: Por ahora no implementada
    """
    logger.info("Mapa de calor mensual no implementado en esta versión")
    return None

def crear_grafica_distribucion_por_agente(resultados_dict):
    """
    FUNCIÓN PLACEHOLDER: Distribución por agente.
    
    Por ahora retorna None para evitar errores.
    Se implementará en la siguiente iteración.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        None: Por ahora no implementada
    """
    logger.info("Distribución por agente no implementada en esta versión")
    return None

def crear_grafica_exposicion_bolsa(resultados_dict):
    """
    FUNCIÓN PLACEHOLDER: Exposición en bolsa.
    
    Por ahora retorna None para evitar errores.
    Se implementará en la siguiente iteración.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        None: Por ahora no implementada
    """
    logger.info("Exposición en bolsa no implementada en esta versión")
    return None