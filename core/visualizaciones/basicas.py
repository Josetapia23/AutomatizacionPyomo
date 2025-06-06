"""
Funciones básicas para la generación de visualizaciones del sistema de optimización energética.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import logging
from datetime import datetime
from pathlib import Path

from .utils import (
    format_number, convert_to_gwh, extract_dates_from_results,
    extract_hours_from_results, extract_offers_from_results,
    format_date_for_display
)

logger = logging.getLogger(__name__)

def crear_grafica_principal_energia_asignada(resultados_dict):
    """
    Crea la gráfica principal de ENERGÍA ASIGNADA Y NO ASIGNADA por horas.
    Esta es la gráfica principal solicitada por el cliente.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        
    Returns:
        plotly.graph_objects.Figure: Figura de Plotly con la gráfica
    """
    logger.info("Creando gráfica principal de energía asignada y no asignada")
    
    try:
        # Inicializar arrays para almacenar datos por hora
        horas = list(range(1, 25))
        gwh_asignados = [0] * 24
        gwh_no_asignados = [0] * 24
        
        # Procesar todas las hojas de resultados
        for clave, df in resultados_dict.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            
            # Identificar tipo de hoja
            es_demanda_asignada = "DEMANDA ASIGNADA" in clave and "_COMPRAR" in clave
            es_energia_no_asignada = "DEMANDA ASIGNADA" in clave and "_NO_COMPRADA" in clave
            es_demanda_faltante = clave == "DEMANDA_FALTANTE"
            
            if es_demanda_asignada or es_energia_no_asignada or es_demanda_faltante:
                # Procesar cada fila del DataFrame
                for _, row in df.iterrows():
                    # Sumar valores por hora (columnas 1-24)
                    for hora in range(1, 25):
                        if hora in row and pd.notna(row[hora]):
                            valor_kwh = float(row[hora])
                            valor_gwh = convert_to_gwh(valor_kwh)
                            
                            if es_demanda_asignada:
                                gwh_asignados[hora-1] += valor_gwh
                            elif es_energia_no_asignada or es_demanda_faltante:
                                gwh_no_asignados[hora-1] += valor_gwh
        
        # Calcular porcentajes
        porcentajes_no_asignado = []
        for i in range(24):
            total = gwh_asignados[i] + gwh_no_asignados[i]
            if total > 0:
                porcentaje = (gwh_no_asignados[i] / total) * 100
            else:
                porcentaje = 0
            porcentajes_no_asignado.append(porcentaje)
        
        # Crear la figura con subplots para eje secundario
        fig = make_subplots(
            specs=[[{"secondary_y": True}]],
            subplot_titles=["ENERGÍA ASIGNADA Y NO ASIGNADA"]
        )
        
        # Agregar barras de GWh Asignados
        fig.add_trace(
            go.Bar(
                x=horas,
                y=gwh_asignados,
                name="GWh Asignados",
                marker_color="#1f4e79",  # Azul oscuro
                text=[f"{val:.2f}" for val in gwh_asignados],
                textposition="inside",
                textfont=dict(color="white", size=10),
                showlegend=True
            ),
            secondary_y=False
        )
        
        # Agregar barras de GWh No Asignado
        fig.add_trace(
            go.Bar(
                x=horas,
                y=gwh_no_asignados,
                name="GWh No Asignado",
                marker_color="#a8c8ec",  # Azul claro
                text=[f"{val:.2f}" for val in gwh_no_asignados],
                textposition="inside",
                textfont=dict(color="black", size=10),
                showlegend=True
            ),
            secondary_y=False
        )
        
        # Agregar línea de porcentaje no asignado
        fig.add_trace(
            go.Scatter(
                x=horas,
                y=porcentajes_no_asignado,
                mode="lines+markers",
                name="% No Asignado",
                line=dict(color="#2ecc71", width=3),  # Verde
                marker=dict(
                    size=8,
                    color="#2ecc71",
                    line=dict(color="white", width=2)
                ),
                text=[f"{val:.1f}%" for val in porcentajes_no_asignado],
                textposition="top center",
                textfont=dict(color="#2ecc71", size=11, family="Arial Black"),
                showlegend=True
            ),
            secondary_y=True
        )
        
        # Configurar eje Y principal (GWh)
        fig.update_yaxes(
            title_text="GWh",
            title_font=dict(size=14, color="#1f4e79"),
            tickfont=dict(size=12),
            showgrid=True,
            gridcolor="lightgray",
            secondary_y=False
        )
        
        # Configurar eje Y secundario (%)
        fig.update_yaxes(
            title_text="% No Asignado",
            title_font=dict(size=14, color="#2ecc71"),
            tickfont=dict(size=12),
            ticksuffix="%",
            showgrid=False,
            secondary_y=True
        )
        
        # Configurar eje X
        fig.update_xaxes(
            title_text="HORAS",
            title_font=dict(size=14),
            tickfont=dict(size=12),
            tickmode="linear",
            tick0=1,
            dtick=1,
            showgrid=True,
            gridcolor="lightgray"
        )
        
        # Configurar layout general
        fig.update_layout(
            title={
                'text': "ENERGÍA ASIGNADA Y NO ASIGNADA",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'color': '#1f4e79', 'family': 'Arial Black'}
            },
            width=1200,
            height=600,
            barmode='stack',  # Barras apiladas
            plot_bgcolor='white',
            paper_bgcolor='white',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.2,
                xanchor="center",
                x=0.5,
                font=dict(size=12)
            ),
            margin=dict(l=80, r=80, t=100, b=120),
            showlegend=True
        )
        
        # Agregar anotaciones para explicar las siglas (como solicitó el cliente)
        fig.add_annotation(
            x=0.02, y=0.98,
            xref="paper", yref="paper",
            text="<b>DA:</b> Demanda Asignada | <b>ENA:</b> Energía No Asignada",
            showarrow=False,
            font=dict(size=12, color="#666666"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#cccccc",
            borderwidth=1
        )
        
        logger.info("Gráfica principal creada exitosamente")
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear la gráfica principal: {e}")
        raise

def crear_grafica_resumen_general(resultados_dict):
    """
    Crea la gráfica de resumen general solicitada por el cliente:
    - Energía adjudicada total
    - Precio ponderado de adjudicación  
    - Precio máximo adjudicado
    - Precio mínimo adjudicado
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Figura con el resumen general
    """
    logger.info("Creando gráfica de resumen general")
    
    try:
        # Verificar si existe el resumen ejecutivo
        if "RESUMEN EJECUTIVO" not in resultados_dict:
            logger.warning("No se encontró RESUMEN EJECUTIVO en los resultados")
            return None
        
        resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
        
        # Calcular métricas globales
        energia_total = 0
        precios = []
        costo_total = 0
        
        for _, row in resumen_df.iterrows():
            for col in resumen_df.columns:
                if "CANTIDAD (KWh)" in col:
                    cantidad = row[col] if pd.notna(row[col]) else 0
                    energia_total += cantidad
                    
                    # Buscar precio correspondiente
                    oferta = col.replace(" CANTIDAD (KWh)", "")
                    precio_col = f"{oferta} PRECIO INDEXADO ($/KWh)"
                    
                    if precio_col in resumen_df.columns:
                        precio = row[precio_col] if pd.notna(row[precio_col]) else 0
                        if precio > 0 and cantidad > 0:
                            precios.append(precio)
                            costo_total += cantidad * precio
        
        # Calcular estadísticas
        energia_gwh = convert_to_gwh(energia_total)
        precio_ponderado = costo_total / energia_total if energia_total > 0 else 0
        precio_min = min(precios) if precios else 0
        precio_max = max(precios) if precios else 0
        
        # Crear gráfica de barras
        categorias = ['Energía Total\n(GWh)', 'Precio Ponderado\n($/kWh)', 'Precio Mínimo\n($/kWh)', 'Precio Máximo\n($/kWh)']
        valores = [energia_gwh, precio_ponderado, precio_min, precio_max]
        colores = ['#1f4e79', '#2ecc71', '#e74c3c', '#f39c12']
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=categorias,
            y=valores,
            marker_color=colores,
            text=[f"{val:.2f}" for val in valores],
            textposition="outside",
            textfont=dict(size=14, color="black")
        ))
        
        fig.update_layout(
            title={
                'text': "RESUMEN GENERAL DE ADJUDICACIÓN",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'color': '#1f4e79'}
            },
            width=800,
            height=500,
            plot_bgcolor='white',
            paper_bgcolor='white',
            showlegend=False,
            margin=dict(l=60, r=60, t=80, b=100)
        )
        
        fig.update_yaxes(showgrid=True, gridcolor="lightgray")
        fig.update_xaxes(tickfont=dict(size=12))
        
        logger.info("Gráfica de resumen general creada exitosamente")
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear gráfica de resumen general: {e}")
        return None

def crear_grafica_torta_adjudicacion(resultados_dict):
    """
    Crea gráfica de torta para mostrar % adjudicado respecto al total ofertado.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Figura con gráfica de torta
    """
    logger.info("Creando gráfica de torta de adjudicación")
    
    try:
        # Calcular totales de energía asignada y no asignada
        total_asignado = 0
        total_no_asignado = 0
        
        for clave, df in resultados_dict.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
                
            if "DEMANDA ASIGNADA" in clave and "_COMPRAR" in clave:
                # Sumar energía asignada
                for _, row in df.iterrows():
                    for hora in range(1, 25):
                        if hora in row and pd.notna(row[hora]):
                            total_asignado += float(row[hora])
            
            elif "DEMANDA ASIGNADA" in clave and "_NO_COMPRADA" in clave:
                # Sumar energía no asignada
                for _, row in df.iterrows():
                    for hora in range(1, 25):
                        if hora in row and pd.notna(row[hora]):
                            total_no_asignado += float(row[hora])
        
        # Convertir a GWh
        total_asignado_gwh = convert_to_gwh(total_asignado)
        total_no_asignado_gwh = convert_to_gwh(total_no_asignado)
        total_gwh = total_asignado_gwh + total_no_asignado_gwh
        
        # Calcular porcentajes
        pct_asignado = (total_asignado_gwh / total_gwh * 100) if total_gwh > 0 else 0
        pct_no_asignado = (total_no_asignado_gwh / total_gwh * 100) if total_gwh > 0 else 0
        
        # Crear gráfica de torta
        fig = go.Figure(data=[go.Pie(
            labels=['Energía Adjudicada', 'Energía No Adjudicada'],
            values=[total_asignado_gwh, total_no_asignado_gwh],
            hole=0.3,  # Donut chart
            marker_colors=['#1f4e79', '#a8c8ec'],
            textinfo='label+percent+value',
            texttemplate='%{label}<br>%{percent}<br>%{value:.1f} GWh',
            textfont=dict(size=12)
        )])
        
        fig.update_layout(
            title={
                'text': "DISTRIBUCIÓN DE ENERGÍA ADJUDICADA",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'color': '#1f4e79'}
            },
            width=600,
            height=500,
            plot_bgcolor='white',
            paper_bgcolor='white',
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.05
            )
        )
        
        logger.info("Gráfica de torta creada exitosamente")
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear gráfica de torta: {e}")
        return None