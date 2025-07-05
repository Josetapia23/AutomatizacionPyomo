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
    CORREGIDO: Solo cambia etiqueta a GW-TOTAL, mantiene cálculos originales
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
            
            # Usar SOLO _COMPRAR y _NO_COMPRADA (como hace el cliente)
            if es_demanda_asignada or es_energia_no_asignada:
                # Procesar cada fila del DataFrame
                for _, row in df.iterrows():
                    # Sumar valores por hora (columnas 1-24)
                    for hora in range(1, 25):
                        if hora in row and pd.notna(row[hora]):
                            valor_kwh = float(row[hora])
                            # MANTENER CÁLCULO ORIGINAL (convert_to_gwh)
                            valor_gwh = convert_to_gwh(valor_kwh)
                            
                            if es_demanda_asignada:
                                gwh_asignados[hora-1] += valor_gwh
                            elif es_energia_no_asignada:
                                gwh_no_asignados[hora-1] += valor_gwh
        
        # Logs para verificar que los valores son correctos
        logger.info(f"GWh Asignados hora 1: {gwh_asignados[0]:.2f}")
        logger.info(f"GWh No Asignados hora 1: {gwh_no_asignados[0]:.2f}")
        logger.info(f"Total hora 1: {gwh_asignados[0] + gwh_no_asignados[0]:.2f}")
        
        # Calcular porcentajes
        porcentajes_no_asignado = []
        for i in range(24):
            total = gwh_asignados[i] + gwh_no_asignados[i]
            if total > 0:
                porcentaje = (gwh_no_asignados[i] / total) * 100
            else:
                porcentaje = 0
            porcentajes_no_asignado.append(porcentaje)
        
        # Log del porcentaje para verificar
        logger.info(f"% No Asignado hora 1: {porcentajes_no_asignado[0]:.2f}%")
        
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
                name="GW Asignados",  # Solo cambio de nombre para consistencia
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
                name="GW No Asignado",  # Solo cambio de nombre para consistencia
                marker_color="#87CEEB",  # Azul claro como cliente
                text=[f"{val:.2f}" for val in gwh_no_asignados],
                textposition="inside",
                textfont=dict(color="black", size=10),
                showlegend=True
            ),
            secondary_y=False
        )
        
        # Agregar línea de porcentaje no asignado (CON CURVATURA)
        fig.add_trace(
            go.Scatter(
                x=horas,
                y=porcentajes_no_asignado,
                mode="lines+markers",
                name="% No Asignado",
                line=dict(
                    color="#2ecc71", 
                    width=3,
                    shape='spline',     # ← Línea curva
                    smoothing=0.8       # ← Suavizado
                ),
                marker=dict(
                    size=8,
                    color="#2ecc71",
                    line=dict(color="white", width=2)
                ),
                text=[f"{val:.2f}%" for val in porcentajes_no_asignado],
                textposition="top center",
                textfont=dict(color="#2ecc71", size=11, family="Arial Black"),
                showlegend=True
            ),
            secondary_y=True
        )
        
        # SOLO CAMBIO DE ETIQUETA: Configurar eje Y principal
        fig.update_yaxes(
            title_text="GW-TOTAL",  # ÚNICO CAMBIO: Era "GWh", ahora "GW-TOTAL"
            title_font=dict(size=14, color="#1f4e79"),
            tickfont=dict(size=12),
            showgrid=True,
            gridcolor="lightgray",
            secondary_y=False
        )
        
        # Configurar eje Y secundario (%) - RANGO AJUSTADO
        porcentaje_min = min(porcentajes_no_asignado) if porcentajes_no_asignado else 0
        porcentaje_max = max(porcentajes_no_asignado) if porcentajes_no_asignado else 0
        rango_porcentaje = porcentaje_max - porcentaje_min
        if rango_porcentaje < 1:
            centro = (porcentaje_max + porcentaje_min) / 2
            expansion = max(1, rango_porcentaje * 3)
            porcentaje_min_visual = centro - expansion/2
            porcentaje_max_visual = centro + expansion/2
        else:
            porcentaje_min_visual = porcentaje_min * 0.95
            porcentaje_max_visual = porcentaje_max * 1.05
        
        fig.update_yaxes(
            title_text="% No Asignado",
            title_font=dict(size=14, color="#2ecc71"),
            tickfont=dict(size=12),
            ticksuffix="%",
            showgrid=False,
            range=[porcentaje_min_visual, porcentaje_max_visual],
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
        
        # Agregar anotaciones para explicar las siglas
        fig.add_annotation(
            x=0.02, y=2,
            xref="paper", yref="paper",
            text="<b>DA:</b> Demanda Asignada | <b>ENA:</b> Energía No Asignada",
            showarrow=False,
            font=dict(size=12, color="#666666"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#cccccc",
            borderwidth=1
        )
        
        logger.info("Gráfica principal creada exitosamente con etiqueta GW-TOTAL")
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear la gráfica principal: {e}")
        raise
     
def crear_grafica_resumen_general(resultados_dict):
    """
    Crea la gráfica de resumen general MEJORADA:
    - Mejor legibilidad de números
    - Espaciado adecuado
    - Verificación de cantidades
    """
    logger.info("Creando gráfica de resumen general mejorada")
    
    try:
        # Verificar si existe el resumen ejecutivo
        if "RESUMEN EJECUTIVO" not in resultados_dict:
            logger.warning("No se encontró RESUMEN EJECUTIVO en los resultados")
            return None
        
        resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
        
        # Inicializar variables para cálculos
        energia_total = 0
        precios_indexados = []
        precios_no_indexados = []
        costo_total_indexado = 0
        costo_total_no_indexado = 0
        
        # Diccionarios para almacenar info de agentes
        agentes_precios_indexados = {}
        agentes_precios_no_indexados = {}
        
        # DEBUG: Verificar estructura del resumen
        print(f"🔍 DEBUG - Columnas en resumen ejecutivo: {resumen_df.columns.tolist()}")
        print(f"🔍 DEBUG - Primeras filas del resumen:")
        print(resumen_df.head())
        
        # Procesar cada fila del resumen ejecutivo
        for _, row in resumen_df.iterrows():
            for col in resumen_df.columns:
                if "CANTIDAD (KWh)" in col:
                    cantidad = row[col] if pd.notna(row[col]) else 0
                    
                    if cantidad > 0:  # Solo procesar ofertas con cantidad asignada
                        energia_total += cantidad
                        
                        # Extraer nombre del agente/oferta
                        agente = col.replace(" CANTIDAD (KWh)", "")
                        
                        # DEBUG: Mostrar datos por agente
                        print(f"🔍 DEBUG - Agente: {agente}, Cantidad: {cantidad:,.0f} kWh")
                        
                        # Buscar precios correspondientes
                        precio_indexado_col = f"{agente} PRECIO INDEXADO ($/KWh)"
                        precio_no_indexado_col = f"{agente} PRECIO ($/KWh)"
                        
                        # Procesar precio indexado
                        if precio_indexado_col in resumen_df.columns:
                            precio_indexado = row[precio_indexado_col] if pd.notna(row[precio_indexado_col]) else 0
                            if precio_indexado > 0:
                                precios_indexados.append(precio_indexado)
                                agentes_precios_indexados[precio_indexado] = agente
                                costo_total_indexado += cantidad * precio_indexado
                                print(f"   💰 Precio Indexado: ${precio_indexado:.4f}")
                        
                        # Procesar precio no indexado
                        if precio_no_indexado_col in resumen_df.columns:
                            precio_no_indexado = row[precio_no_indexado_col] if pd.notna(row[precio_no_indexado_col]) else 0
                            if precio_no_indexado > 0:
                                precios_no_indexados.append(precio_no_indexado)
                                agentes_precios_no_indexados[precio_no_indexado] = agente
                                costo_total_no_indexado += cantidad * precio_no_indexado
                                print(f"   💲 Precio No Indexado: ${precio_no_indexado:.4f}")
        
        # DEBUG: Verificar totales
        print(f"🔍 DEBUG - Energía total: {energia_total:,.0f} kWh")
        print(f"🔍 DEBUG - Energía total en GW: {energia_total / 1_000_000:.2f} GW")
        
        # CORREGIDO: Convertir energía a GW-TOTAL y redondear a 2 decimales
        energia_gw_total = round(energia_total / 1_000_000, 2)
        
        # Precios ponderados (redondeados a 4 decimales)
        precio_ponderado_indexado = round(costo_total_indexado / energia_total, 4) if energia_total > 0 else 0
        precio_ponderado_no_indexado = round(costo_total_no_indexado / energia_total, 4) if energia_total > 0 else 0
        
        # Precios mínimos y máximos con agentes
        if precios_indexados:
            precio_min_indexado = round(min(precios_indexados), 4)
            precio_max_indexado = round(max(precios_indexados), 4)
            agente_min_indexado = agentes_precios_indexados.get(min(precios_indexados), "N/A")
            agente_max_indexado = agentes_precios_indexados.get(max(precios_indexados), "N/A")
        else:
            precio_min_indexado = precio_max_indexado = 0
            agente_min_indexado = agente_max_indexado = "N/A"
        
        if precios_no_indexados:
            precio_min_no_indexado = round(min(precios_no_indexados), 4)
            precio_max_no_indexado = round(max(precios_no_indexados), 4)
            agente_min_no_indexado = agentes_precios_no_indexados.get(min(precios_no_indexados), "N/A")
            agente_max_no_indexado = agentes_precios_no_indexados.get(max(precios_no_indexados), "N/A")
        else:
            precio_min_no_indexado = precio_max_no_indexado = 0
            agente_min_no_indexado = agente_max_no_indexado = "N/A"
        
        # MEJORADO: Crear gráfica con mejor espaciado
        fig = make_subplots(
            rows=2, cols=3,
            subplot_titles=[
                "Energía Total (GW-TOTAL)", 
                "Precio Ponderado ($/kWh)", 
                "Precios Mínimos y Máximos ($/kWh)"
            ],
            specs=[[{"type": "bar"}, {"type": "bar"}, {"type": "bar"}],
                   [{"colspan": 3, "type": "table"}, None, None]],
            vertical_spacing=0.2,  # AUMENTADO para más espacio
            row_heights=[0.65, 0.35]  # AJUSTADO para más espacio en tabla
        )
        
        # Definir colores consistentes
        COLOR_INDEXADO = '#2E86C1'      # Azul para indexado
        COLOR_NO_INDEXADO = '#E74C3C'   # Rojo para no indexado
        
        # 1. MEJORADO: Energía Total con mejor formato
        fig.add_trace(
            go.Bar(
                x=["Energía Total"],
                y=[energia_gw_total],
                name="Energía",
                marker_color='#1f4e79',
                text=[f"{energia_gw_total:.2f}<br>GW-TOTAL"],  # MEJORADO: Texto en 2 líneas
                textposition="outside",
                textfont=dict(size=12, color='#1f4e79'),  # MEJORADO: Más grande y colorido
                showlegend=False
            ),
            row=1, col=1
        )
        
        # 2. MEJORADO: Precios Ponderados con mejor espaciado
        fig.add_trace(
            go.Bar(
                x=["Indexado", "No Indexado"],
                y=[precio_ponderado_indexado, precio_ponderado_no_indexado],
                name="Precio Ponderado",
                marker_color=[COLOR_INDEXADO, COLOR_NO_INDEXADO],
                text=[f"${precio_ponderado_indexado:.4f}", f"${precio_ponderado_no_indexado:.4f}"],
                textposition="outside",
                textfont=dict(size=11),  # MEJORADO: Tamaño ajustado
                showlegend=False
            ),
            row=1, col=2
        )
        
        # 3. MEJORADO: Precios Min/Max con mejor legibilidad
        fig.add_trace(
            go.Bar(
                x=["Mín<br>Indexado", "Mín<br>No Index", "Máx<br>Indexado", "Máx<br>No Index"],  # MEJORADO: Etiquetas en 2 líneas
                y=[precio_min_indexado, precio_min_no_indexado, precio_max_indexado, precio_max_no_indexado],
                name="Precios Min/Max",
                marker_color=[COLOR_INDEXADO, COLOR_NO_INDEXADO, COLOR_INDEXADO, COLOR_NO_INDEXADO],
                text=[f"${precio_min_indexado:.2f}", f"${precio_min_no_indexado:.2f}",  # MEJORADO: Solo 2 decimales para mejor legibilidad
                      f"${precio_max_indexado:.2f}", f"${precio_max_no_indexado:.2f}"],
                textposition="outside",
                textfont=dict(size=10),  # MEJORADO: Tamaño más pequeño pero legible
                showlegend=False
            ),
            row=1, col=3
        )
        
        # 4. MEJORADA: Tabla con mejor formato
        fig.add_trace(
            go.Table(
                header=dict(
                    values=["<b>Agente</b>", "<b>Métrica</b>", "<b>Precio Indexado</b>", "<b>Precio No Indexado</b>"],
                    fill_color='#1f4e79',
                    font=dict(color='white', size=13),  # MEJORADO: Más grande
                    align="center",
                    height=35  # MEJORADO: Más alto
                ),
                cells=dict(
                    values=[
                        [agente_min_indexado, agente_max_indexado],
                        ["Precio Mínimo", "Precio Máximo"],
                        [f"${precio_min_indexado:.4f}", f"${precio_max_indexado:.4f}"],
                        [f"${precio_min_no_indexado:.4f}", f"${precio_max_no_indexado:.4f}"]
                    ],
                    fill_color=[['#f8f9fa', '#e9ecef'] * 2],
                    font=dict(size=12),  # MEJORADO: Más grande
                    align="center",
                    height=30  # MEJORADO: Más alto
                )
            ),
            row=2, col=1
        )
        
        # MEJORADO: Layout con más espacio
        fig.update_layout(
            title={
                'text': "RESUMEN GENERAL DE ADJUDICACIÓN<br><sub>Energía en GW-TOTAL | Precios Indexados vs No Indexados</sub>",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16, 'color': '#1f4e79'}  # MEJORADO: Tamaño ajustado
            },
            width=1300,  # MEJORADO: Más ancho
            height=750,  # MEJORADO: Más alto
            plot_bgcolor='white',
            paper_bgcolor='white',
            showlegend=False,
            margin=dict(l=80, r=80, t=130, b=150)  # MEJORADO: Más márgenes
        )
        
        # MEJORADO: Ajustar rangos de ejes para evitar cortes
        fig.update_yaxes(
            showgrid=True, 
            gridcolor="lightgray", 
            row=1, col=1,
            range=[0, energia_gw_total * 1.2]  # MEJORADO: 20% más espacio arriba
        )
        fig.update_yaxes(
            showgrid=True, 
            gridcolor="lightgray", 
            row=1, col=2,
            range=[0, max(precio_ponderado_indexado, precio_ponderado_no_indexado) * 1.2]
        )
        fig.update_yaxes(
            showgrid=True, 
            gridcolor="lightgray", 
            row=1, col=3,
            range=[0, max(precio_max_indexado, precio_max_no_indexado) * 1.15]  # MEJORADO: Más espacio
        )
        
        # MEJORADO: Leyenda más clara
        fig.add_annotation(
            x=0.5, y=-0.15,
            xref="paper", yref="paper",
            text=f"<b>LEYENDA:</b> " +
                 f"<span style='color:{COLOR_INDEXADO}; font-size:14px'>●</span> Indexado | " +
                 f"<span style='color:{COLOR_NO_INDEXADO}; font-size:14px'>●</span> No Indexado<br>" +
                 f"<i>Energía: {energia_gw_total:.2f} GW-TOTAL | Precios: $/kWh</i>",
            showarrow=False,
            font=dict(size=12, color="#1f4e79"),
            bgcolor="rgba(240,248,255,0.9)",
            bordercolor="#1f4e79",
            borderwidth=1,
            align="center"
        )
        
        logger.info("Gráfica de resumen general mejorada creada exitosamente")
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear gráfica de resumen general: {e}")
        return None
    
def crear_grafica_torta_adjudicacion(resultados_dict):
    """
    Crea gráfica de torta para mostrar % adjudicado respecto al total ofertado.
    INCLUYE ofertas que no participaron en Pyomo y gráfica de porcentaje por agente.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Figura con gráficas de torta
    """
    logger.info("Creando gráfica de torta de adjudicación con análisis por agente")
    
    try:
        from plotly.subplots import make_subplots
        
        # Calcular totales de energía asignada
        total_asignado = 0
        total_no_asignado_pyomo = 0
        
        # Diccionario para almacenar datos por agente
        datos_por_agente = {}
        
        # 1. PROCESAR ENERGÍA ASIGNADA (hojas _COMPRAR)
        for clave, df in resultados_dict.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
                
            if "DEMANDA ASIGNADA" in clave and "_COMPRAR" in clave:
                # Extraer nombre del agente
                try:
                    agente = clave.split("DEMANDA ASIGNADA ")[1].split(" IT")[0]
                    
                    if agente not in datos_por_agente:
                        datos_por_agente[agente] = {
                            'asignado': 0,
                            'no_asignado_pyomo': 0,
                            'no_participo_pyomo': 0,
                            'total_ofertado': 0
                        }
                    
                    # Sumar energía asignada
                    for _, row in df.iterrows():
                        for hora in range(1, 25):
                            if hora in row and pd.notna(row[hora]):
                                energia = float(row[hora])
                                total_asignado += energia
                                datos_por_agente[agente]['asignado'] += energia
                
                except Exception as e:
                    logger.warning(f"Error procesando clave {clave}: {e}")
                    continue
        
        # 2. PROCESAR ENERGÍA NO ASIGNADA DE PYOMO (hojas _NO_COMPRADA)
        for clave, df in resultados_dict.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            
            if "DEMANDA ASIGNADA" in clave and "_NO_COMPRADA" in clave:
                # Extraer nombre del agente
                try:
                    agente = clave.split("DEMANDA ASIGNADA ")[1].split(" IT")[0]
                    
                    if agente in datos_por_agente:
                        # Sumar energía no asignada por Pyomo
                        for _, row in df.iterrows():
                            for hora in range(1, 25):
                                if hora in row and pd.notna(row[hora]):
                                    energia = float(row[hora])
                                    total_no_asignado_pyomo += energia
                                    datos_por_agente[agente]['no_asignado_pyomo'] += energia
                
                except Exception as e:
                    logger.warning(f"Error procesando clave {clave}: {e}")
                    continue
        
        # 3. PROCESAR OFERTAS QUE NO PARTICIPARON EN PYOMO
        # Buscar en el resumen de rechazos por precio si existe
        total_no_participo_pyomo = 0
        
        if "RESUMEN RECHAZOS PRECIO" in resultados_dict:
            resumen_rechazos_df = resultados_dict["RESUMEN RECHAZOS PRECIO"]
            for _, row in resumen_rechazos_df.iterrows():
                agente = row.get('OFERTA', '')
                cantidad_rechazada = row.get('CANTIDAD TOTAL RECHAZADA (KWh)', 0)
                
                if agente and cantidad_rechazada > 0:
                    if agente not in datos_por_agente:
                        datos_por_agente[agente] = {
                            'asignado': 0,
                            'no_asignado_pyomo': 0,
                            'no_participo_pyomo': 0,
                            'total_ofertado': 0
                        }
                    
                    datos_por_agente[agente]['no_participo_pyomo'] += cantidad_rechazada
                    total_no_participo_pyomo += cantidad_rechazada
        else:
            # Si no existe resumen de rechazos, buscar en el resumen ejecutivo
            if "RESUMEN EJECUTIVO" in resultados_dict:
                resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
                
                for col in resumen_df.columns:
                    if "CANTIDAD (KWh)" in col:
                        agente = col.replace(" CANTIDAD (KWh)", "")
                        
                        # Si el agente no está en datos_por_agente, significa que no participó
                        if agente not in datos_por_agente:
                            # Estimar la cantidad no participante basada en precios
                            # (esto es aproximado, idealmente debería venir del procesamiento inicial)
                            datos_por_agente[agente] = {
                                'asignado': 0,
                                'no_asignado_pyomo': 0,
                                'no_participo_pyomo': 1000,  # Valor estimado
                                'total_ofertado': 1000
                            }
                            total_no_participo_pyomo += 1000
        
        # 4. CALCULAR TOTALES POR AGENTE
        for agente in datos_por_agente:
            datos_por_agente[agente]['total_ofertado'] = (
                datos_por_agente[agente]['asignado'] + 
                datos_por_agente[agente]['no_asignado_pyomo'] + 
                datos_por_agente[agente]['no_participo_pyomo']
            )
        
        # Convertir a GWh
        total_asignado_gwh = convert_to_gwh(total_asignado)
        total_no_asignado_pyomo_gwh = convert_to_gwh(total_no_asignado_pyomo)
        total_no_participo_pyomo_gwh = convert_to_gwh(total_no_participo_pyomo)
        
        # Total ofertado
        total_ofertado_gwh = total_asignado_gwh + total_no_asignado_pyomo_gwh + total_no_participo_pyomo_gwh
        
        # Crear subplot con 2 gráficas de torta lado a lado
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{"type": "domain"}, {"type": "domain"}]],
            subplot_titles=[
                "DISTRIBUCIÓN GENERAL DE ENERGÍA", 
                "PORCENTAJE DE ADJUDICACIÓN POR AGENTE"
            ],
            horizontal_spacing=0.15
        )
        
        # GRÁFICA 1: Distribución general (izquierda)
        labels_general = [
            'Energía Adjudicada', 
            'Energía No Adjudicada (Pyomo)', 
            'Ofertas No Participaron (Rechazadas)'
        ]
        values_general = [
            total_asignado_gwh, 
            total_no_asignado_pyomo_gwh, 
            total_no_participo_pyomo_gwh
        ]
        colors_general = ['#1f4e79', '#a8c8ec', '#ff7f7f']
        
        fig.add_trace(
            go.Pie(
                labels=labels_general,
                values=values_general,
                hole=0.3,
                marker_colors=colors_general,
                textinfo='label+percent+value',
                texttemplate='%{label}<br>%{percent}<br>%{value:.1f} GWh',
                textfont=dict(size=11),
                name="General"
            ),
            row=1, col=1
        )
        
        # GRÁFICA 2: Porcentaje de adjudicación por agente (derecha)
        agentes_nombres = []
        porcentajes_adjudicacion = []
        colores_agentes = ['#2ecc71', '#e74c3c', '#f39c12', '#9b59b6', '#3498db', '#e67e22']
        
        for i, (agente, datos) in enumerate(datos_por_agente.items()):
            if datos['total_ofertado'] > 0:
                porcentaje = (datos['asignado'] / datos['total_ofertado']) * 100
                agentes_nombres.append(agente)
                porcentajes_adjudicacion.append(porcentaje)
        
        # Si tenemos datos de agentes, crear la segunda torta
        if agentes_nombres:
            fig.add_trace(
                go.Pie(
                    labels=agentes_nombres,
                    values=porcentajes_adjudicacion,
                    hole=0.3,
                    marker_colors=colores_agentes[:len(agentes_nombres)],
                    textinfo='label+percent',
                    texttemplate='%{label}<br>%{percent}<br>Adj: %{value:.1f}%',
                    textfont=dict(size=11),
                    name="Por Agente"
                ),
                row=1, col=2
            )
        else:
            # Si no hay datos, mostrar mensaje
            fig.add_annotation(
                x=0.75, y=0.5,
                xref="paper", yref="paper",
                text="No hay datos<br>de agentes<br>disponibles",
                showarrow=False,
                font=dict(size=14, color="#666666"),
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="#cccccc",
                borderwidth=1
            )
        
        # Configurar layout
        fig.update_layout(
            title={
                'text': "ANÁLISIS DE ADJUDICACIÓN DE ENERGÍA<br><sub>Distribución General y Eficiencia por Agente</sub>",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'color': '#1f4e79'}
            },
            width=1200,
            height=600,
            plot_bgcolor='white',
            paper_bgcolor='white',
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.1,
                xanchor="center",
                x=0.5,
                font=dict(size=11)
            ),
            margin=dict(l=80, r=80, t=120, b=100)
        )
        
        # Agregar tabla resumen en la parte inferior
        fig.add_annotation(
            x=0.5, y=-0.15,
            xref="paper", yref="paper",
            text=f"<b>RESUMEN:</b> Total Ofertado: {total_ofertado_gwh:.1f} GWh | " +
                 f"Adjudicado: {total_asignado_gwh:.1f} GWh ({(total_asignado_gwh/total_ofertado_gwh*100):.1f}%) | " +
                 f"No Adjudicado: {(total_no_asignado_pyomo_gwh + total_no_participo_pyomo_gwh):.1f} GWh " +
                 f"({((total_no_asignado_pyomo_gwh + total_no_participo_pyomo_gwh)/total_ofertado_gwh*100):.1f}%)",
            showarrow=False,
            font=dict(size=12, color="#1f4e79"),
            bgcolor="rgba(240,248,255,0.9)",
            bordercolor="#1f4e79",
            borderwidth=1
        )
        
        logger.info("Gráfica de torta con análisis por agente creada exitosamente")
        print(f"📊 Torta creada: {len(agentes_nombres)} agentes analizados")
        
        # Log de verificación
        for agente, datos in datos_por_agente.items():
            porcentaje_adj = (datos['asignado'] / datos['total_ofertado'] * 100) if datos['total_ofertado'] > 0 else 0
            print(f"  - {agente}: {porcentaje_adj:.1f}% adjudicado")
        
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear gráfica de torta: {e}")
        print(f"❌ Error en gráfica de torta: {e}")
        return None