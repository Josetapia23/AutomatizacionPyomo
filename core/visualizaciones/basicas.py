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

def crear_grafica_principal_energia_asignada(resultados_dict, ofertas_df=None):
    """
    Crea la gráfica principal de ENERGÍA ASIGNADA Y NO ASIGNADA por horas.
    VERSIÓN CORREGIDA: Elimina duplicación en cálculo de energía no asignada.
    """
    logger.info("Creando gráfica principal de energía asignada y no asignada")
    
    try:
        # Inicializar arrays para almacenar datos por hora
        horas = list(range(1, 25))
        gwh_asignados = [0] * 24
        gwh_no_asignados = [0] * 24
        
        # CORRECCIÓN: Solo usar hojas de resultados_dict, NO sumar ofertas_df adicional
        # Procesar todas las hojas de resultados
        for clave, df in resultados_dict.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            
            # Identificar tipo de hoja
            es_demanda_asignada = "DEMANDA ASIGNADA" in clave and "_COMPRAR" in clave
            es_energia_no_asignada = "DEMANDA ASIGNADA" in clave and "_NO_COMPRADA" in clave
            
            # Usar SOLO _COMPRAR y _NO_COMPRADA
            if es_demanda_asignada or es_energia_no_asignada:
                # Procesar cada fila del DataFrame
                for _, row in df.iterrows():
                    # Sumar valores por hora (columnas 1-24)
                    for hora in range(1, 25):
                        if hora in row and pd.notna(row[hora]):
                            valor_kwh = float(row[hora])
                            valor_gwh = convert_to_gwh(valor_kwh)
                            
                            if es_demanda_asignada:
                                gwh_asignados[hora-1] += valor_gwh
                            elif es_energia_no_asignada:
                                gwh_no_asignados[hora-1] += valor_gwh

        # ELIMINAR la sección problemática que duplicaba:
        # if ofertas_df is not None and not ofertas_df.empty:
        #     # Esta lógica causaba DUPLICACIÓN
        
        # Verificar que tenemos datos
        if all(x == 0 for x in gwh_asignados) and all(x == 0 for x in gwh_no_asignados):
            logger.warning("No se encontraron datos de energía en los resultados")
            return None
        
        # Calcular porcentajes de energía no asignada
        porcentajes_no_asignado = []
        for i in range(24):
            total = gwh_asignados[i] + gwh_no_asignados[i]
            if total > 0:
                porcentaje = (gwh_no_asignados[i] / total) * 100
            else:
                porcentaje = 0
            porcentajes_no_asignado.append(porcentaje)
        
        # Log de verificación
        total_asignado = sum(gwh_asignados)
        total_no_asignado = sum(gwh_no_asignados)
        logger.info(f"Total asignado: {total_asignado:.2f} GWh")
        logger.info(f"Total no asignado: {total_no_asignado:.2f} GWh")
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
                name="GW Asignados",
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
                name="GW No Asignado",
                marker_color="#87CEEB",  # Azul claro
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
                line=dict(
                    color="#2ecc71",  # Verde
                    width=3,
                    shape="spline"  # Curva suave
                ),
                marker=dict(
                    color="#27ae60",
                    size=8,
                    line=dict(color="white", width=2)
                ),
                showlegend=True
            ),
            secondary_y=True
        )
        
        # Configurar ejes Y
        fig.update_yaxes(
            title_text="GW-TOTAL",
            title_font=dict(size=14, color="#1f4e79"),
            tickfont=dict(size=12),
            secondary_y=False,
            showgrid=True,
            gridcolor="lightgray"
        )
        
        fig.update_yaxes(
            title_text="% No Asignado",
            title_font=dict(size=14, color="#2ecc71"),
            tickfont=dict(size=12, color="#2ecc71"),
            range=[0, max(100, max(porcentajes_no_asignado) * 1.1)],
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
        
        logger.info("Gráfica principal creada exitosamente - SIN duplicación")
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear la gráfica principal: {e}")
        raise
      
def crear_grafica_energia_por_anos(resultados_dict, ofertas_df=None):
    """
    Crea la gráfica de ENERGÍA ASIGNADA Y NO ASIGNADA agrupada por AÑOS.
    CORREGIDO: Sin filtros interactivos y anotación reubicada.
    MEJORADO: Incluye ofertas no participantes en energía no asignada.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        ofertas_df (DataFrame, opcional): DataFrame con ofertas originales
        
    Returns:
        plotly.graph_objects.Figure: Figura con la gráfica anual
    """
    logger.info("Creando gráfica de energía asignada y no asignada por AÑOS")
    
    try:
        from plotly.subplots import make_subplots
        
        # Diccionarios para almacenar datos por año
        gwh_asignados_por_ano = {}
        gwh_no_asignados_por_ano = {}
        
        # Procesar todas las hojas de resultados
        for clave, df in resultados_dict.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            
            # Identificar tipo de hoja
            es_demanda_asignada = "DEMANDA ASIGNADA" in clave and "_COMPRAR" in clave
            es_energia_no_asignada = "DEMANDA ASIGNADA" in clave and "_NO_COMPRADA" in clave
            
            if es_demanda_asignada or es_energia_no_asignada:
                # Procesar cada fila del DataFrame
                for _, row in df.iterrows():
                    fecha = row['FECHA']
                    
                    # Extraer año de la fecha
                    if hasattr(fecha, 'year'):
                        ano = fecha.year
                    elif isinstance(fecha, str):
                        try:
                            fecha_dt = pd.to_datetime(fecha)
                            ano = fecha_dt.year
                        except:
                            logger.warning(f"No se pudo procesar la fecha: {fecha}")
                            continue
                    else:
                        logger.warning(f"Formato de fecha no reconocido: {fecha}")
                        continue
                    
                    # Inicializar año si no existe
                    if ano not in gwh_asignados_por_ano:
                        gwh_asignados_por_ano[ano] = 0
                        gwh_no_asignados_por_ano[ano] = 0
                    
                    # Sumar valores por todas las horas (columnas 1-24)
                    total_dia = 0
                    for hora in range(1, 25):
                        if hora in row and pd.notna(row[hora]):
                            valor_kwh = float(row[hora])
                            total_dia += valor_kwh
                    
                    # Convertir a GWh y agregar al año correspondiente
                    valor_gwh = convert_to_gwh(total_dia)
                    
                    if es_demanda_asignada:
                        gwh_asignados_por_ano[ano] += valor_gwh
                    elif es_energia_no_asignada:
                        gwh_no_asignados_por_ano[ano] += valor_gwh

        # NUEVO: Agregar ofertas no participantes por año
        if ofertas_df is not None and not ofertas_df.empty:
            # Identificar ofertas participantes
            ofertas_participantes = set()
            for clave in resultados_dict.keys():
                if "_COMPRAR" in clave or "_NO_COMPRADA" in clave:
                    if "DEMANDA ASIGNADA" in clave:
                        partes = clave.split()
                        for i, parte in enumerate(partes):
                            if parte in ["IT1_COMPRAR", "IT2_COMPRAR", "IT1_NO_COMPRADA", "IT2_NO_COMPRADA"]:
                                if i > 2:
                                    nombre_oferta = " ".join(partes[2:i])
                                    ofertas_participantes.add(nombre_oferta)
                                break
            
            todas_las_ofertas = set(ofertas_df['CÓDIGO OFERTA'].unique())
            ofertas_no_participantes = todas_las_ofertas - ofertas_participantes
            
            # Sumar energía no participante por año
            for _, row in ofertas_df.iterrows():
                oferta = row['CÓDIGO OFERTA']
                if oferta in ofertas_no_participantes:
                    fecha = row.get('FECHA')
                    cantidad = row.get('CANTIDAD', 0)
                    
                    if fecha and pd.notna(cantidad) and cantidad > 0:
                        try:
                            if hasattr(fecha, 'year'):
                                ano = fecha.year
                            elif isinstance(fecha, str):
                                fecha_dt = pd.to_datetime(fecha)
                                ano = fecha_dt.year
                            else:
                                continue
                                
                            if ano not in gwh_no_asignados_por_ano:
                                gwh_no_asignados_por_ano[ano] = 0
                            
                            valor_gwh = convert_to_gwh(cantidad)
                            gwh_no_asignados_por_ano[ano] += valor_gwh
                        except:
                            continue
        
        # Verificar que tenemos datos
        if not gwh_asignados_por_ano and not gwh_no_asignados_por_ano:
            logger.warning("No se encontraron datos para la gráfica por años")
            return None
        
        # Obtener años únicos y ordenarlos
        todos_los_anos = set(gwh_asignados_por_ano.keys()) | set(gwh_no_asignados_por_ano.keys())
        anos_ordenados = sorted(list(todos_los_anos))
        
        # Preparar listas para la gráfica
        anos = []
        gwh_asignados = []
        gwh_no_asignados = []
        
        for ano in anos_ordenados:
            anos.append(str(ano))  # Convertir a string para mejor visualización
            gwh_asignados.append(gwh_asignados_por_ano.get(ano, 0))
            gwh_no_asignados.append(gwh_no_asignados_por_ano.get(ano, 0))
        
        # Calcular porcentajes no asignado por año
        porcentajes_no_asignado = []
        for i in range(len(anos)):
            total = gwh_asignados[i] + gwh_no_asignados[i]
            if total > 0:
                porcentaje = (gwh_no_asignados[i] / total) * 100
            else:
                porcentaje = 0
            porcentajes_no_asignado.append(porcentaje)
        
        # Logs para verificación
        logger.info(f"Datos por año encontrados:")
        for i, ano in enumerate(anos):
            logger.info(f"  {ano}: Asignado={gwh_asignados[i]:.2f} GWh, No Asignado={gwh_no_asignados[i]:.2f} GWh, %No Asignado={porcentajes_no_asignado[i]:.2f}%")
        
        # Crear la figura con subplots para eje secundario
        fig = make_subplots(
            specs=[[{"secondary_y": True}]],
            subplot_titles=["ENERGÍA ASIGNADA Y NO ASIGNADA POR AÑO"]
        )
        
        # Agregar barras de GW Asignados
        fig.add_trace(
            go.Bar(
                x=anos,
                y=gwh_asignados,
                name="GW Asignados",
                marker_color="#1f4e79",  # Azul oscuro (igual que la gráfica por horas)
                text=[f"{val:.1f}" for val in gwh_asignados],
                textposition="inside",
                textfont=dict(color="white", size=12),
                showlegend=True,
                # Hacer barras interactivas
                hovertemplate="<b>%{x}</b><br>" +
                             "GW Asignados: %{y:.2f}<br>" +
                             "<extra></extra>"
            ),
            secondary_y=False
        )
        
        # Agregar barras de GW No Asignado
        fig.add_trace(
            go.Bar(
                x=anos,
                y=gwh_no_asignados,
                name="GW No Asignado",
                marker_color="#87CEEB",  # Azul claro (igual que la gráfica por horas)
                text=[f"{val:.1f}" for val in gwh_no_asignados],
                textposition="inside",
                textfont=dict(color="black", size=12),
                showlegend=True,
                # Hacer barras interactivas
                hovertemplate="<b>%{x}</b><br>" +
                             "GW No Asignado: %{y:.2f}<br>" +
                             "<extra></extra>"
            ),
            secondary_y=False
        )
        
        # Agregar línea de porcentaje no asignado (CON CURVATURA)
        fig.add_trace(
            go.Scatter(
                x=anos,
                y=porcentajes_no_asignado,
                mode="lines+markers",
                name="% No Asignado",
                line=dict(
                    color="#2ecc71", 
                    width=3,
                    shape='spline',     # Línea curva
                    smoothing=0.8       # Suavizado
                ),
                marker=dict(
                    size=10,  # Marcadores más grandes para años
                    color="#2ecc71",
                    line=dict(color="white", width=2)
                ),
                text=[f"{val:.1f}%" for val in porcentajes_no_asignado],
                textposition="top center",
                textfont=dict(color="#2ecc71", size=12, family="Arial Black"),
                showlegend=True,
                # Hover personalizado
                hovertemplate="<b>%{x}</b><br>" +
                             "% No Asignado: %{y:.2f}%<br>" +
                             "<extra></extra>"
            ),
            secondary_y=True
        )
        
        # Configurar eje Y principal (igual que la gráfica original)
        fig.update_yaxes(
            title_text="GW-TOTAL",
            title_font=dict(size=14, color="#1f4e79"),
            tickfont=dict(size=12),
            showgrid=True,
            gridcolor="lightgray",
            secondary_y=False
        )
        
        # Configurar eje Y secundario (%)
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
        
        # Configurar eje X (AÑOS)
        fig.update_xaxes(
            title_text="AÑOS",
            title_font=dict(size=14),
            tickfont=dict(size=12),
            showgrid=True,
            gridcolor="lightgray",
            # Configuración específica para años
            type='category',  # Tratar como categorías para mejor espaciado
        )
        
        # CORREGIDO: Layout sin filtros interactivos y más espacio inferior
        fig.update_layout(
            title={
                'text': "ENERGÍA ASIGNADA Y NO ASIGNADA POR AÑO",
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
                y=-0.20,  # CORREGIDO: Más espacio para evitar superposición
                xanchor="center",
                x=0.5,
                font=dict(size=12)
            ),
            margin=dict(l=80, r=80, t=100, b=180),  # CORREGIDO: Más margen inferior
            showlegend=True
            # REMOVIDO: Sin filtros interactivos updatemenus
        )
        
        # CORREGIDO: Anotación reubicada fuera del área de la gráfica
        total_anos = len(anos)
        ano_inicio = anos[0] if anos else "N/A"
        ano_fin = anos[-1] if anos else "N/A"
        
        # Información de resumen debajo de la gráfica, fuera del área de ploteo
        fig.add_annotation(
            x=0.5, y=-0.27,  # CORREGIDO: Más abajo para evitar superposición
            xref="paper", yref="paper",
            text=f"📊 Período analizado: {ano_inicio} - {ano_fin} ({total_anos} años) | <b>DA:</b> Demanda Asignada | <b>ENA:</b> Energía No Asignada",
            showarrow=False,
            font=dict(size=11, color="#666666"),
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="#cccccc",
            borderwidth=1,
            align="center"
        )
        
        logger.info(f"Gráfica anual CORREGIDA creada exitosamente para {len(anos)} años: {ano_inicio}-{ano_fin}")
        print(f"📊 Gráfica anual generada (sin filtros): {len(anos)} años ({ano_inicio}-{ano_fin})")
        
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear la gráfica anual: {e}")
        print(f"❌ Error en gráfica anual: {e}")
        return None
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
        
        # Precios ponderados (redondeados a 2 decimales)
        precio_ponderado_indexado = round(costo_total_indexado / energia_total, 2) if energia_total > 0 else 0
        precio_ponderado_no_indexado = round(costo_total_no_indexado / energia_total, 2) if energia_total > 0 else 0
        
        # Precios mínimos y máximos con agentes
        if precios_indexados:
            precio_min_indexado = round(min(precios_indexados), 2)
            precio_max_indexado = round(max(precios_indexados), 2)
            agente_min_indexado = agentes_precios_indexados.get(min(precios_indexados), "N/A")
            agente_max_indexado = agentes_precios_indexados.get(max(precios_indexados), "N/A")
        else:
            precio_min_indexado = precio_max_indexado = 0
            agente_min_indexado = agente_max_indexado = "N/A"
        
        if precios_no_indexados:
            precio_min_no_indexado = round(min(precios_no_indexados), 2)
            precio_max_no_indexado = round(max(precios_no_indexados), 2)
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
                text=[f"${precio_ponderado_indexado:.2f}", f"${precio_ponderado_no_indexado:.2f}"],
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
                        [f"${precio_min_indexado:.2f}", f"${precio_max_indexado:.2f}"],
                        [f"${precio_min_no_indexado:.2f}", f"${precio_max_no_indexado:.2f}"]
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
    
def crear_grafica_torta_adjudicacion(resultados_dict, ofertas_df=None):
    """
    Crea gráfica de torta ADAPTATIVA que funciona con cualquier número de ofertas.
    Se adapta automáticamente sin configuraciones manuales.
    MEJORADO: Incluye ofertas no participantes en energía no adjudicada.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        ofertas_df (DataFrame, opcional): DataFrame con ofertas originales
        
    Returns:
        plotly.graph_objects.Figure: Figura con gráficas de torta optimizada
    """
    logger.info("Creando gráfica de torta ADAPTATIVA")
    
    try:
        from plotly.subplots import make_subplots
        
        # Calcular totales de energía asignada
        total_asignado = 0
        total_no_asignado_pyomo = 0
        total_no_participo_pyomo = 0
        datos_por_agente = {}
        
        # 1. PROCESAR ENERGÍA ASIGNADA
        for clave, df in resultados_dict.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
                
            if "DEMANDA ASIGNADA" in clave and "_COMPRAR" in clave:
                try:
                    agente = clave.split("DEMANDA ASIGNADA ")[1].split(" IT")[0]
                    
                    if agente not in datos_por_agente:
                        datos_por_agente[agente] = {
                            'asignado': 0,
                            'no_asignado_pyomo': 0,
                            'no_participo_pyomo': 0,
                            'total_ofertado': 0
                        }
                    
                    for _, row in df.iterrows():
                        for hora in range(1, 25):
                            if hora in row and pd.notna(row[hora]):
                                energia = float(row[hora])
                                total_asignado += energia
                                datos_por_agente[agente]['asignado'] += energia
                
                except Exception as e:
                    logger.warning(f"Error procesando clave {clave}: {e}")
                    continue
        
        # 2. PROCESAR ENERGÍA NO ASIGNADA
        for clave, df in resultados_dict.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            
            if "DEMANDA ASIGNADA" in clave and "_NO_COMPRADA" in clave:
                try:
                    agente = clave.split("DEMANDA ASIGNADA ")[1].split(" IT")[0]
                    
                    if agente in datos_por_agente:
                        for _, row in df.iterrows():
                            for hora in range(1, 25):
                                if hora in row and pd.notna(row[hora]):
                                    energia = float(row[hora])
                                    total_no_asignado_pyomo += energia
                                    datos_por_agente[agente]['no_asignado_pyomo'] += energia
                
                except Exception as e:
                    logger.warning(f"Error procesando clave {clave}: {e}")
                    continue

        # NUEVO: Agregar ofertas no participantes al total no adjudicado
        if ofertas_df is not None and not ofertas_df.empty:
            # Identificar ofertas participantes
            ofertas_participantes = set()
            for clave in resultados_dict.keys():
                if "_COMPRAR" in clave or "_NO_COMPRADA" in clave:
                    if "DEMANDA ASIGNADA" in clave:
                        partes = clave.split()
                        for i, parte in enumerate(partes):
                            if parte in ["IT1_COMPRAR", "IT2_COMPRAR", "IT1_NO_COMPRADA", "IT2_NO_COMPRADA"]:
                                if i > 2:
                                    nombre_oferta = " ".join(partes[2:i])
                                    ofertas_participantes.add(nombre_oferta)
                                break
            
            todas_las_ofertas = set(ofertas_df['CÓDIGO OFERTA'].unique())
            ofertas_no_participantes = todas_las_ofertas - ofertas_participantes
            
            # Sumar energía de ofertas no participantes
            for _, row in ofertas_df.iterrows():
                oferta = row['CÓDIGO OFERTA']
                if oferta in ofertas_no_participantes:
                    cantidad = row.get('CANTIDAD', 0)
                    if pd.notna(cantidad) and cantidad > 0:
                        total_no_asignado_pyomo += cantidad
        
        # 3. CALCULAR TOTALES
        for agente in datos_por_agente:
            datos_por_agente[agente]['total_ofertado'] = (
                datos_por_agente[agente]['asignado'] + 
                datos_por_agente[agente]['no_asignado_pyomo']
            )
        
        # Convertir a GWh
        total_asignado_gwh = convert_to_gwh(total_asignado)
        total_no_asignado_pyomo_gwh = convert_to_gwh(total_no_asignado_pyomo)
        total_ofertado_gwh = total_asignado_gwh + total_no_asignado_pyomo_gwh
        
        num_ofertas = len(datos_por_agente)
        print(f"📊 Procesando {num_ofertas} ofertas con criterio adaptativo")
        
        # =================================================================
        # LÓGICA ADAPTATIVA: Criterio automático basado en número de ofertas
        # =================================================================
        
        def calcular_criterio_adaptativo(num_ofertas):
            """Calcula criterio de agrupación basado en número de ofertas"""
            if num_ofertas <= 5:
                return 8.0    # Con pocas ofertas, mostrar casi todas
            elif num_ofertas <= 10:
                return 4.0    # Criterio medio
            elif num_ofertas <= 25:
                return 2.0    # Criterio estándar  
            elif num_ofertas <= 50:
                return 1.5    # Más restrictivo
            else:
                return 1.0    # Muy restrictivo para 50+ ofertas
        
        porcentaje_minimo = calcular_criterio_adaptativo(num_ofertas)
        
        # Calcular datos por agente
        agentes_con_datos = []
        for agente, datos in datos_por_agente.items():
            if datos['total_ofertado'] > 0:
                porcentaje_del_total = (datos['asignado'] / total_asignado) * 100 if total_asignado > 0 else 0
                
                agentes_con_datos.append({
                    'agente': agente,
                    'asignado_gwh': convert_to_gwh(datos['asignado']),
                    'porcentaje_del_total': porcentaje_del_total,
                    'datos': datos
                })
        
        # Ordenar por energía asignada
        agentes_con_datos.sort(key=lambda x: x['asignado_gwh'], reverse=True)
        
        # Aplicar criterio adaptativo
        ofertas_individuales = []
        ofertas_agrupadas = []
        
        for agente_info in agentes_con_datos:
            if agente_info['porcentaje_del_total'] >= porcentaje_minimo:
                ofertas_individuales.append(agente_info)
            else:
                ofertas_agrupadas.append(agente_info)
        
        print(f"  📋 Criterio: {porcentaje_minimo}% mínimo -> {len(ofertas_individuales)} individuales + {len(ofertas_agrupadas)} agrupadas")
        
        # Crear subplot
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{"type": "domain"}, {"type": "domain"}]],
            subplot_titles=[
                "DISTRIBUCIÓN GENERAL", 
                f"POR OFERTA ({len(ofertas_individuales)} + Otros)"
            ],
            horizontal_spacing=0.15
        )
        
        # ==========================================
        # GRÁFICA 1: Distribución general
        # ==========================================
        labels_general = ['Adjudicada', 'No Adjudicada']
        values_general = [total_asignado_gwh, total_no_asignado_pyomo_gwh]
        colors_general = ['#1f4e79', '#a8c8ec']
        
        fig.add_trace(
            go.Pie(
                labels=labels_general,
                values=values_general,
                hole=0.3,
                marker_colors=colors_general,
                textinfo='label+percent+value',
                texttemplate='%{label}<br>%{percent}<br>%{value:.1f} GWh',
                textfont=dict(size=12),
                showlegend=False
            ),
            row=1, col=1
        )
        
        # ==========================================
        # GRÁFICA 2: Por oferta (ADAPTATIVA)
        # ==========================================
        
        # ✅ CAMBIO 1: Función de colores dinámicos (del consolidado)
        def generar_colores_ofertas(num_ofertas):
            import colorsys
            
            colores = []
            for i in range(num_ofertas):
                # Distribuir con más separación para evitar similares
                hue = (i * 0.618033988749895) % 1  # Proporción áurea para mejor distribución
                saturation = 0.9 if i % 2 == 0 else 0.7  # Alternar saturación
                value = 0.8 if i % 3 == 0 else 0.6  # Alternar brillo
                
                rgb = colorsys.hsv_to_rgb(hue, saturation, value)
                hex_color = '#{:02x}{:02x}{:02x}'.format(
                    int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255)
                )
                colores.append(hex_color)
            
            return colores
        
        # Generar colores dinámicamente
        total_segmentos = len(ofertas_individuales) + (1 if ofertas_agrupadas else 0)
        colores_dinamicos = generar_colores_ofertas(total_segmentos)
        
        # Preparar datos
        labels_ofertas = []
        values_ofertas = []
        colors_ofertas = []
        
        # Ofertas individuales
        for i, agente_info in enumerate(ofertas_individuales):
            # Etiqueta concisa
            if agente_info['asignado_gwh'] >= 1:
                label = f"{agente_info['agente']}\n{agente_info['asignado_gwh']:.1f} GWh"
            else:
                label = f"{agente_info['agente']}\n{agente_info['asignado_gwh']:.2f} GWh"
            
            labels_ofertas.append(label)
            values_ofertas.append(agente_info['asignado_gwh'])
            colors_ofertas.append(colores_dinamicos[i])  # ✅ Usar colores dinámicos
        
        # Grupo "Otros"
        if ofertas_agrupadas:
            total_otros_gwh = sum(ag['asignado_gwh'] for ag in ofertas_agrupadas)
            
            if total_otros_gwh >= 1:
                label_otros = f"Otros ({len(ofertas_agrupadas)})\n{total_otros_gwh:.1f} GWh"
            else:
                label_otros = f"Otros ({len(ofertas_agrupadas)})\n{total_otros_gwh:.2f} GWh"
            
            labels_ofertas.append(label_otros)
            values_ofertas.append(total_otros_gwh)
            colors_ofertas.append('#bdc3c7')
        
        # Crear segunda torta CON HOVER DETALLADO
        if labels_ofertas:
            # Crear hovertemplate personalizado para cada segmento
            hover_templates = []
            
            # Para ofertas individuales
            for i, agente_info in enumerate(ofertas_individuales):
                hover_templates.append(
                    "<b>%{label}</b><br>" +
                    "Energía: %{value:.2f} GWh<br>" +
                    "% del Total: %{percent}<br>" +
                    "<extra></extra>"
                )
            
            # Para el grupo "Otros" - hover detallado
            if ofertas_agrupadas:
                # Crear lista detallada de ofertas agrupadas
                detalles_otros = []
                ofertas_agrupadas_sorted = sorted(ofertas_agrupadas, key=lambda x: x['asignado_gwh'], reverse=True)
                
                # Mostrar hasta las primeras 8 ofertas en el hover
                max_mostrar = min(8, len(ofertas_agrupadas_sorted))
                for ag in ofertas_agrupadas_sorted[:max_mostrar]:
                    detalles_otros.append(f"• {ag['agente']}: {ag['asignado_gwh']:.2f} GWh ({ag['porcentaje_del_total']:.1f}%)")
                
                # Si hay más de 8, agregar indicador
                if len(ofertas_agrupadas_sorted) > max_mostrar:
                    restantes = len(ofertas_agrupadas_sorted) - max_mostrar
                    detalles_otros.append(f"...y {restantes} más")
                
                # Crear hover template para "Otros"
                total_otros_gwh = sum(ag['asignado_gwh'] for ag in ofertas_agrupadas)
                hover_otros = (
                    f"<b>Otros ({len(ofertas_agrupadas)} ofertas)</b><br>" +
                    f"Total: {total_otros_gwh:.2f} GWh<br>" +
                    f"% del Total: %{{percent}}<br><br>" +
                    "<b>Detalles:</b><br>" +
                    "<br>".join(detalles_otros) +
                    "<extra></extra>"
                )
                hover_templates.append(hover_otros)
            
            fig.add_trace(
                go.Pie(
                    labels=labels_ofertas,
                    values=values_ofertas,
                    hole=0.3,
                    marker_colors=colors_ofertas,
                    textinfo='label+percent+value',  # ✅ CAMBIO 2: Agregar porcentajes
                    texttemplate='%{label}<br>%{percent}<br>%{value:.1f} GWh',  # ✅ Igual que la izquierda
                    textfont=dict(size=10),
                    showlegend=False,
                    hovertemplate=hover_templates  # Usar templates personalizados
                ),
                row=1, col=2
            )
        
        # ==========================================
        # LAYOUT LIMPIO Y COMPACTO
        # ==========================================
        fig.update_layout(
            title={
                'text': f"ADJUDICACIÓN DE ENERGÍA<br><sub>{num_ofertas} ofertas • Criterio adaptativo: {porcentaje_minimo}% mínimo</sub>",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'color': '#1f4e79'}
            },
            width=1200,
            height=600,
            plot_bgcolor='white',
            paper_bgcolor='white',
            showlegend=False,
            margin=dict(l=50, r=50, t=120, b=80)  # Márgenes reducidos
        )
        
        # ==========================================
        # ANOTACIÓN MÍNIMA Y COMPACTA
        # ==========================================
        fig.add_annotation(
            x=0.5, y=-0.08,
            xref="paper", yref="paper",
            text=f"📊 Total: {total_ofertado_gwh:.1f} GWh • ✅ Adjudicado: {total_asignado_gwh:.1f} GWh ({(total_asignado_gwh/total_ofertado_gwh*100) if total_ofertado_gwh > 0 else 0:.1f}%)",
            showarrow=False,
            font=dict(size=12, color="#1f4e79"),
            bgcolor="rgba(240,248,255,0.9)",
            bordercolor="#1f4e79",
            borderwidth=1
        )
        
        logger.info(f"Gráfica adaptativa creada: {len(ofertas_individuales)} individuales + {len(ofertas_agrupadas)} agrupadas")
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear gráfica adaptativa: {e}")
        return None
    