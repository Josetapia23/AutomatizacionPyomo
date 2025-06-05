"""
Visualizaciones básicas para el sistema de optimización energética.
Incluye gráficas de resumen, tortas y otras visualizaciones sencillas.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging

from .utils import format_number, convert_to_gwh, format_date_for_display

# Configurar logging
logger = logging.getLogger(__name__)

def calcular_totales_energia(resultados_dict):
    """
    Calcula los totales de energía asignada y no asignada de todo el ejercicio.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        tuple: (total_asignada_gwh, total_no_asignada_gwh, demanda_faltante_gwh)
    """
    total_asignada = 0
    total_no_asignada = 0
    demanda_faltante = 0
    
    # Procesar todas las hojas de resultados
    for key, df in resultados_dict.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            # Para hojas de demanda asignada (DA) - Lo que SÍ se compró
            if "DEMANDA ASIGNADA" in key and "_COMPRAR" in key:
                # Sumar todas las columnas numéricas (horas 1-24)
                for col in df.columns:
                    if isinstance(col, int) and 1 <= col <= 24:
                        total_asignada += df[col].sum()
            
            # Para demanda faltante
            elif key == "DEMANDA_FALTANTE":
                for col in df.columns:
                    if isinstance(col, int) and 1 <= col <= 24:
                        demanda_faltante += df[col].sum()
    
    # Convertir a GWh
    total_asignada_gwh = convert_to_gwh(total_asignada)
    demanda_faltante_gwh = convert_to_gwh(demanda_faltante)
    
    # La energía no asignada es la demanda faltante
    total_no_asignada_gwh = demanda_faltante_gwh
    
    return total_asignada_gwh, total_no_asignada_gwh, demanda_faltante_gwh

def obtener_precios_extremos(resultados_dict):
    """
    Obtiene el precio máximo y mínimo de adjudicación.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        tuple: (precio_min, precio_max, precio_ponderado)
    """
    precio_min = float('inf')
    precio_max = 0
    total_energia = 0
    total_costo = 0
    
    # Obtener datos del resumen ejecutivo si existe
    if "RESUMEN EJECUTIVO" in resultados_dict:
        df_resumen = resultados_dict["RESUMEN EJECUTIVO"]
        
        # Buscar todas las columnas que contienen precios
        for col in df_resumen.columns:
            if "PRECIO INDEXADO" in col:
                precios = df_resumen[col].dropna()
                if not precios.empty:
                    precio_min = min(precio_min, precios.min())
                    precio_max = max(precio_max, precios.max())
            
            # Calcular precio ponderado
            if "CANTIDAD" in col:
                oferta = col.split(" CANTIDAD")[0]
                precio_col = f"{oferta} PRECIO INDEXADO"
                
                if precio_col in df_resumen.columns:
                    for _, row in df_resumen.iterrows():
                        cantidad = row.get(col, 0)
                        precio = row.get(precio_col, 0)
                        
                        if cantidad > 0 and precio > 0:
                            total_energia += cantidad
                            total_costo += cantidad * precio
    
    # Calcular precio ponderado
    precio_ponderado = total_costo / total_energia if total_energia > 0 else 0
    
    # Si no se encontraron precios, establecer valores predeterminados
    if precio_min == float('inf'):
        precio_min = 0
    
    return precio_min, precio_max, precio_ponderado

def crear_grafica_resumen_adjudicacion(resultados_dict):
    """
    Crea una gráfica interactiva que muestra el resumen de energía adjudicada,
    precio ponderado, máximo y mínimo, según lo solicitado por el cliente.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Figura Plotly con el resumen
    """
    # Calcular totales de energía
    total_asignada, total_no_asignada, _ = calcular_totales_energia(resultados_dict)
    total_energia = total_asignada + total_no_asignada
    
    # Obtener precios
    precio_min, precio_max, precio_ponderado = obtener_precios_extremos(resultados_dict)
    
    # Crear figura con dos subplots: barras a la izquierda, indicadores a la derecha
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "bar"}, {"type": "domain"}]],
        column_widths=[0.6, 0.4],
        subplot_titles=("Energía Asignada vs. No Asignada (GWh)", "Precios de Adjudicación ($/KWh)")
    )
    
    # Gráfica de barras para energía
    categorias = ['Energía Asignada', 'Energía No Asignada', 'Total Demanda']
    valores = [total_asignada, total_no_asignada, total_energia]
    colores = ['#2E86AB', '#E63946', '#42B883']
    
    # Calcular porcentajes
    porcentaje_asignado = (total_asignada / total_energia * 100) if total_energia > 0 else 0
    
    # Añadir barra con porcentaje
    texto_barras = [
        f'<b>{total_asignada:.2f} GWh</b><br>({porcentaje_asignado:.1f}%)',
        f'<b>{total_no_asignada:.2f} GWh</b><br>({100-porcentaje_asignado:.1f}%)',
        f'<b>{total_energia:.2f} GWh</b><br>(100%)'
    ]
    
    fig.add_trace(
        go.Bar(
            x=categorias, 
            y=valores, 
            marker_color=colores,
            text=texto_barras,
            textposition='outside',
            hovertemplate='%{x}<br>%{text}<extra></extra>'
        ),
        row=1, col=1
    )
    
    # Indicadores para precios
    # 1. Precio Ponderado
    fig.add_trace(
        go.Indicator(
            mode="number+gauge+delta",
            value=precio_ponderado,
            title={"text": "<b>Precio Ponderado</b>"},
            number={"prefix": "$", "suffix": "/KWh", "valueformat": ".2f"},
            gauge={
                'axis': {'range': [None, precio_max * 1.2]},
                'bar': {'color': "#2E86AB"},
                'steps': [
                    {'range': [0, precio_min], 'color': '#D3F8E2'},
                    {'range': [precio_min, precio_max], 'color': '#A9DEF9'}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 2},
                    'thickness': 0.75,
                    'value': precio_max
                }
            }
        ),
        row=1, col=2
    )
    
    # Añadir anotaciones para precio mínimo y máximo
    fig.add_annotation(
        x=0.95, y=0.8,
        text=f"<b>Precio Máximo:</b> ${precio_max:.2f}/KWh",
        showarrow=False,
        xref="paper", yref="paper",
        align="right",
        bgcolor="#E63946",
        font=dict(color="white", size=14),
        bordercolor="#E63946",
        borderwidth=2,
        borderpad=6,
        row=1, col=2
    )
    
    fig.add_annotation(
        x=0.95, y=0.2,
        text=f"<b>Precio Mínimo:</b> ${precio_min:.2f}/KWh",
        showarrow=False,
        xref="paper", yref="paper",
        align="right",
        bgcolor="#2E86AB",
        font=dict(color="white", size=14),
        bordercolor="#2E86AB",
        borderwidth=2,
        borderpad=6,
        row=1, col=2
    )
    
    # Actualizar diseño
    fig.update_layout(
        title_text="<b>Resumen de Adjudicación de Energía</b>",
        height=500,
        showlegend=False,
        plot_bgcolor='rgba(250,250,250,0.9)',
        font=dict(family="Arial, sans-serif", size=12),
        hoverlabel=dict(bgcolor="white", font_size=14),
        margin=dict(t=100, b=80, l=80, r=80)
    )
    
    # Personalizar ejes
    fig.update_yaxes(title_text="GWh", gridcolor='rgba(0,0,0,0.1)', row=1, col=1)
    fig.update_xaxes(title_text="", tickangle=-15, gridcolor='rgba(0,0,0,0.1)', row=1, col=1)
    
    return fig

def crear_grafica_distribucion_porcentual(resultados_dict):
    """
    Crea una gráfica de torta mostrando la distribución porcentual de energía asignada vs no asignada.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Figura Plotly con la gráfica de torta
    """
    # Calcular totales
    total_asignada, total_no_asignada, _ = calcular_totales_energia(resultados_dict)
    total_energia = total_asignada + total_no_asignada
    
    # Calcular porcentajes
    pct_asignada = (total_asignada / total_energia * 100) if total_energia > 0 else 0
    pct_no_asignada = (total_no_asignada / total_energia * 100) if total_energia > 0 else 0
    
    # Crear figura
    fig = go.Figure()
    
    # Añadir gráfica de torta
    fig.add_trace(
        go.Pie(
            labels=['Energía Asignada', 'Energía No Asignada'],
            values=[total_asignada, total_no_asignada],
            marker=dict(colors=['#2E86AB', '#E63946']),
            hoverinfo='label+percent+value',
            hovertemplate='%{label}<br>%{value:.2f} GWh<br>%{percent}<extra></extra>',
            textinfo='percent+label',
            texttemplate='%{percent:.1f}%<br>%{label}',
            hole=0.4,
            pull=[0.05, 0],
            insidetextfont=dict(color='white')
        )
    )
    
    # Añadir anotación central
    fig.add_annotation(
        text=f"<b>{total_energia:.1f} GWh</b><br>Total",
        x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=15),
        xref="paper", yref="paper"
    )
    
    # Actualizar diseño
    fig.update_layout(
        title_text="<b>Distribución Porcentual de Energía</b>",
        height=500,
        font=dict(family="Arial, sans-serif", size=12),
        hoverlabel=dict(bgcolor="white", font_size=14),
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
        margin=dict(t=100, b=100, l=80, r=80)
    )
    
    return fig