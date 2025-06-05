"""
Visualizaciones avanzadas para el sistema de optimización energética.
Incluye mapas de calor, distribución horaria, distribución por agente y otras visualizaciones complejas.
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
    Crea una gráfica de barras y líneas mostrando la distribución de energía asignada y
    no asignada por hora del día, como la gráfica que aparece en el correo del cliente.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Figura Plotly con la gráfica de distribución horaria
    """
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
            marker_color='#14213D',  # Azul oscuro como en la imagen
            hovertemplate='Hora %{x}<br>GWh Asignados: %{y:.2f}<extra></extra>'
        )
    )
    
    fig.add_trace(
        go.Bar(
            x=horas,
            y=gwh_no_asignados,
            name='GWh No Asignado',
            marker_color='rgba(230, 230, 230, 0.7)',  # Gris claro como en la imagen
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
            line=dict(color='#48CAE4', width=3),  # Azul como en la imagen
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
        title_text="<b>ENERGÍA ASIGNADA Y NO ASIGNADA</b>",
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
        ),
        # Añadir anotaciones específicas
        annotations=[
            dict(
                x=0.5, y=-0.15,
                text="La gráfica muestra la distribución horaria de la energía asignada (azul oscuro) y no asignada (gris), junto con el porcentaje de energía no asignada (línea azul).",
                showarrow=False,
                xref="paper", yref="paper",
                font=dict(size=11, color="gray")
            )
        ]
    )
    
    return fig

def crear_mapa_calor_mensual(resultados_dict):
    """
    Crea un mapa de calor para mostrar los valores mensuales de precio y demanda,
    similar a la tabla mostrada en el correo del cliente.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Figura Plotly con el mapa de calor
    """
    # Extraer datos del resumen ejecutivo si existe
    if "RESUMEN EJECUTIVO" not in resultados_dict:
        return None
    
    df_resumen = resultados_dict["RESUMEN EJECUTIVO"]
    
    # Crear una matriz para los datos del mapa de calor
    # Filas: meses (1-12)
    # Columnas: años (2025-2030)
    
    # Extraer años únicos de las fechas (formato MM/YYYY)
    años = []
    for fecha in df_resumen["FECHA"]:
        if isinstance(fecha, str) and "/" in fecha:
            año = fecha.split("/")[1]
            if año not in años:
                años.append(año)
    
    # Si no hay años, usar un rango predeterminado
    if not años:
        años = ["2025", "2026", "2027", "2028", "2029", "2030"]
    
    # Ordenar años
    años = sorted(años)
    
    # Crear matriz para los valores
    matriz_valores = np.zeros((12, len(años)))
    matriz_valores.fill(np.nan)  # Inicializar con NaN para celdas sin datos
    
    # Llenar la matriz con los datos
    for idx, row in df_resumen.iterrows():
        fecha = row["FECHA"]
        if isinstance(fecha, str) and "/" in fecha:
            partes = fecha.split("/")
            if len(partes) == 2:
                mes = int(partes[0])
                año = partes[1]
                
                if año in años:
                    col_idx = años.index(año)
                    
                    # Buscar el valor relevante para este mes/año
                    # Usar el precio ponderado promedio si está disponible
                    valor = None
                    
                    # Buscar columnas de precio ponderado
                    for col in row.index:
                        if "PRECIO INDEXADO" in col:
                            valor = row[col]
                            break
                    
                    # Si no hay precio, buscar la demanda no asignada
                    if pd.isna(valor) and "DEMANDA NO ASIGNADA" in row:
                        valor = row["DEMANDA NO ASIGNADA"]
                    
                    if not pd.isna(valor):
                        matriz_valores[mes-1, col_idx] = valor
    
    # Crear figura
    fig = go.Figure()
    
    # Definir escala de colores personalizada
    colorscale = [
        [0, '#E63946'],      # Rojo para valores bajos
        [0.25, '#F4A261'],   # Naranja
        [0.5, '#FFFD82'],    # Amarillo
        [0.75, '#A8DADC'],   # Azul claro
        [1, '#2E86AB']       # Azul para valores altos
    ]
    
    # Añadir mapa de calor
    fig.add_trace(
        go.Heatmap(
            z=matriz_valores,
            x=años,
            y=[f"{i}" for i in range(1, 13)],
            colorscale=colorscale,
            text=[[f"{val:.2f}" if not pd.isna(val) else "" for val in row] for row in matriz_valores],
            hovertemplate='Mes: %{y}<br>Año: %{x}<br>Valor: %{z:.2f}<extra></extra>',
            showscale=True,
            colorbar=dict(
                title="Valor",
                titleside="right",
                tickmode="array",
                tickvals=[matriz_valores.min(), matriz_valores.max()],
                ticktext=[f"{matriz_valores.min():.2f}", f"{matriz_valores.max():.2f}"],
                ticks="outside"
            )
        )
    )
    
    # Añadir anotaciones para mostrar los valores en cada celda
    for i in range(matriz_valores.shape[0]):
        for j in range(matriz_valores.shape[1]):
            valor = matriz_valores[i, j]
            if not pd.isna(valor):
                fig.add_annotation(
                    x=años[j],
                    y=str(i+1),
                    text=f"{valor:.2f}",
                    showarrow=False,
                    font=dict(
                        color="black" if 0.3 <= (valor - matriz_valores.min()) / (matriz_valores.max() - matriz_valores.min()) <= 0.7 else "white",
                        size=10
                    )
                )
    
    # Actualizar diseño
    fig.update_layout(
        title_text="<b>Mapa de Calor de Valores Mensuales</b>",
        height=600,
        font=dict(family="Arial, sans-serif"),
        xaxis=dict(
            title="AÑO",
            tickangle=-45
        ),
        yaxis=dict(
            title="MES",
            autorange="reversed"  # Para que enero esté arriba
        ),
        annotations=[
            dict(
                x=0.5, y=-0.15,
                text="Los valores representan precios indexados o demanda no asignada según disponibilidad.",
                showarrow=False,
                xref="paper", yref="paper",
                font=dict(size=11, color="gray")
            )
        ]
    )
    
    return fig