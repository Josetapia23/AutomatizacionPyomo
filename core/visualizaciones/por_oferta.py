"""
Módulo para generar gráficas individuales por oferta.
Incluye visualizaciones específicas que muestran cantidad asignada/no asignada y precios por oferta.
ACTUALIZADO: Incluye gráfica consolidada de todas las ofertas.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.offline as pyo
import logging
from datetime import datetime
from pathlib import Path

from .utils import (
    format_number, convert_to_gwh, ensure_directory_exists
)

logger = logging.getLogger(__name__)

def extraer_datos_para_grafica_oferta(resultados_dict, nombre_oferta):
    """
    Extrae los datos necesarios para la gráfica de una oferta específica.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        nombre_oferta (str): Nombre de la oferta a procesar
        
    Returns:
        dict: Diccionario con los datos extraídos o None si no hay datos
    """
    logger.info(f"Extrayendo datos para gráfica de oferta: {nombre_oferta}")
    
    # Verificar que existe el resumen ejecutivo
    if "RESUMEN EJECUTIVO" not in resultados_dict:
        logger.warning("No se encontró RESUMEN EJECUTIVO en los resultados")
        return None
    
    resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
    
    if resumen_df.empty:
        logger.warning("RESUMEN EJECUTIVO está vacío")
        return None
    
    # Buscar columnas de esta oferta en el resumen ejecutivo
    col_cantidad = f"{nombre_oferta} CANTIDAD (KWh)"
    col_precio = f"{nombre_oferta} PRECIO ($/KWh)"
    col_precio_indexado = f"{nombre_oferta} PRECIO INDEXADO ($/KWh)"
    
    # Verificar que existen las columnas necesarias
    columnas_necesarias = [col_cantidad, col_precio, col_precio_indexado]
    columnas_existentes = [col for col in columnas_necesarias if col in resumen_df.columns]
    
    if len(columnas_existentes) < 3:
        logger.warning(f"No se encontraron todas las columnas necesarias para {nombre_oferta}")
        logger.debug(f"Columnas encontradas: {columnas_existentes}")
        logger.debug(f"Columnas disponibles en resumen: {list(resumen_df.columns)}")
        return None
    
    # Extraer datos del resumen ejecutivo
    datos = {
        'fechas': [],
        'cantidad_asignada': [],
        'cantidad_no_asignada': [],
        'precio_indexado': [],
        'precio_sin_indexar': []
    }
    
    for _, row in resumen_df.iterrows():
        fecha = row['FECHA']
        cantidad = row[col_cantidad] if pd.notna(row[col_cantidad]) else 0
        precio_sin_indexar = row[col_precio] if pd.notna(row[col_precio]) else 0
        precio_indexado = row[col_precio_indexado] if pd.notna(row[col_precio_indexado]) else 0
        
        # Solo incluir filas que tienen datos (cantidad > 0 o precios > 0)
        if cantidad > 0 or precio_sin_indexar > 0 or precio_indexado > 0:
            datos['fechas'].append(fecha)
            datos['cantidad_asignada'].append(cantidad)
            datos['precio_indexado'].append(precio_indexado)
            datos['precio_sin_indexar'].append(precio_sin_indexar)
    
    # Calcular cantidad no asignada usando las hojas _NO_COMPRADA
    for i, fecha in enumerate(datos['fechas']):
        cantidad_no_asignada_total = 0
        
        # Buscar todas las iteraciones de esta oferta
        for clave, df in resultados_dict.items():
            if (f"DEMANDA ASIGNADA {nombre_oferta}" in clave and 
                "_NO_COMPRADA" in clave and 
                isinstance(df, pd.DataFrame) and 
                not df.empty):
                
                # Convertir fecha del resumen (MM/YYYY) a formato comparable
                try:
                    # El resumen tiene formato "MM/YYYY", convertir a fecha para comparar
                    mes, año = fecha.split('/')
                    fecha_objetivo = datetime(int(año), int(mes), 1).date()
                    
                    # Buscar filas del mes correspondiente en el DataFrame
                    for _, row_df in df.iterrows():
                        fecha_df = row_df['FECHA']
                        
                        # Asegurar que la fecha del DataFrame sea tipo date
                        if hasattr(fecha_df, 'year') and hasattr(fecha_df, 'month'):
                            if fecha_df.year == fecha_objetivo.year and fecha_df.month == fecha_objetivo.month:
                                # Sumar todas las horas (columnas 1-24)
                                for hora in range(1, 25):
                                    if hora in row_df and pd.notna(row_df[hora]):
                                        cantidad_no_asignada_total += row_df[hora]
                        elif isinstance(fecha_df, str):
                            # Si viene como string, intentar parsear
                            try:
                                fecha_parsed = pd.to_datetime(fecha_df).date()
                                if fecha_parsed.year == fecha_objetivo.year and fecha_parsed.month == fecha_objetivo.month:
                                    for hora in range(1, 25):
                                        if hora in row_df and pd.notna(row_df[hora]):
                                            cantidad_no_asignada_total += row_df[hora]
                            except:
                                continue
                except Exception as e:
                    logger.warning(f"Error procesando fecha {fecha}: {e}")
                    cantidad_no_asignada_total = 0
        
        datos['cantidad_no_asignada'].append(cantidad_no_asignada_total)
    
    # Verificar que tenemos datos válidos
    if not datos['fechas']:
        logger.warning(f"No se encontraron datos válidos para la oferta {nombre_oferta}")
        return None
    
    logger.info(f"Datos extraídos para {nombre_oferta}: {len(datos['fechas'])} períodos")
    return datos

def crear_grafica_oferta_individual(nombre_oferta, datos_oferta):
    """
    Crea la gráfica combinada (barras + líneas) para una oferta específica.
    MEJORADO: Espaciado perfecto para evitar sobreposición de texto.
    
    Args:
        nombre_oferta (str): Nombre de la oferta
        datos_oferta (dict): Datos extraídos de la oferta
        
    Returns:
        plotly.graph_objects.Figure: Figura de la gráfica
    """
    logger.info(f"Creando gráfica individual para oferta: {nombre_oferta}")
    
    if not datos_oferta or not datos_oferta['fechas']:
        logger.warning(f"No hay datos para crear gráfica de {nombre_oferta}")
        return None
    
    # Convertir cantidades a GWh
    cantidad_asignada_gwh = [convert_to_gwh(x) for x in datos_oferta['cantidad_asignada']]
    cantidad_no_asignada_gwh = [convert_to_gwh(x) for x in datos_oferta['cantidad_no_asignada']]
    
    # Crear figura con eje secundario
    fig = make_subplots(
        specs=[[{"secondary_y": True}]],
        subplot_titles=[f"DEMANDA ASIGNADA Y NO ASIGNADA GWh - {nombre_oferta}"]
    )
    
    # Agregar barras de cantidad asignada (azul claro)
    fig.add_trace(
        go.Bar(
            x=datos_oferta['fechas'],
            y=cantidad_asignada_gwh,
            name=f"{nombre_oferta} CANTIDAD",
            marker_color="#7BA7D7",
            text=[f"{val:.2f}" if val > 0 else "" for val in cantidad_asignada_gwh],
            textposition="inside",
            textfont=dict(color="white", size=10),
            showlegend=True,
            opacity=0.8
        ),
        secondary_y=False
    )
    
    # Agregar barras de cantidad no asignada (naranja)
    fig.add_trace(
        go.Bar(
            x=datos_oferta['fechas'],
            y=cantidad_no_asignada_gwh,
            name=f"{nombre_oferta} CANTIDAD NO ASIGNADA",
            marker_color="#FF7F50",
            text=[f"{val:.2f}" if val > 0 else "" for val in cantidad_no_asignada_gwh],
            textposition="inside",
            textfont=dict(color="white", size=10),
            showlegend=True,
            opacity=0.8
        ),
        secondary_y=False
    )
    
    # Línea suave de precio indexado
    fig.add_trace(
        go.Scatter(
            x=datos_oferta['fechas'],
            y=datos_oferta['precio_indexado'],
            mode="lines+markers",
            name=f"{nombre_oferta} PRECIO PROMEDIO",
            line=dict(
                color="#1f4e79",
                width=3,
                shape='spline',
                smoothing=0.8
            ),
            marker=dict(
                size=8,
                color="#1f4e79",
                line=dict(color="white", width=2)
            ),
            showlegend=True
        ),
        secondary_y=True
    )
    
    # Línea suave de precio sin indexar
    fig.add_trace(
        go.Scatter(
            x=datos_oferta['fechas'],
            y=datos_oferta['precio_sin_indexar'],
            mode="lines+markers",
            name=f"{nombre_oferta} PRECIO PROMEDIO SIN INDEXAR",
            line=dict(
                color="#2ecc71",
                width=3,
                shape='spline',
                smoothing=0.8
            ),
            marker=dict(
                size=8,
                color="#2ecc71",
                line=dict(color="white", width=2)
            ),
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
    
    # Configurar eje Y secundario ($/kWh)
    fig.update_yaxes(
        title_text="$/kWh",
        title_font=dict(size=14, color="#2ecc71"),
        tickfont=dict(size=12),
        tickprefix="$ ",
        showgrid=False,
        secondary_y=True
    )
    
    # PERFECTO: Eje X con espaciado óptimo
    fig.update_xaxes(
        title_text="FECHA",
        title_font=dict(size=14),
        tickfont=dict(size=10),           # Texto más pequeño
        tickangle=45,                     # Ángulo consistente
        showgrid=True,
        gridcolor="lightgray",
        title_standoff=20                # 🎯 Más separación (era 25)
    )
    
    # PERFECTO: Layout con espaciado optimizado
    fig.update_layout(
        title={
            'text': f"DEMANDA ASIGNADA Y NO ASIGNADA GWh - {nombre_oferta}",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16, 'color': '#1f4e79', 'family': 'Arial Black'}
        },
        width=1200,
        height=600,
        barmode='stack',
        plot_bgcolor='white',
        paper_bgcolor='white',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.4,                       # 🎯 Más espacio (era -0.35)
            xanchor="center",
            x=0.5,
            font=dict(size=10)
        ),
        margin=dict(l=80, r=80, t=100, b=200),  # 🎯 Margen inferior aumentado (era 180)
        showlegend=True
    )
    
    logger.info(f"Gráfica optimizada creada para {nombre_oferta}")
    return fig

def crear_grafica_oferta_no_participante(nombre_oferta, datos_originales):
    """
    Crea gráfica para ofertas que NO participaron en optimización Pyomo.
    Muestra capacidad total como "No Asignada" y precios originales.
    
    Args:
        nombre_oferta (str): Nombre de la oferta
        datos_originales (dict): Datos originales de la oferta (no de Pyomo)
        
    Returns:
        plotly.graph_objects.Figure: Figura de la gráfica
    """
    logger.info(f"Creando gráfica para oferta NO PARTICIPANTE: {nombre_oferta}")
    
    if not datos_originales or not datos_originales['fechas']:
        logger.warning(f"No hay datos originales para {nombre_oferta}")
        return None
    
    # Para no participantes: toda la capacidad es "no asignada"
    cantidad_asignada_gwh = [0] * len(datos_originales['fechas'])  # Siempre cero
    cantidad_no_asignada_gwh = [convert_to_gwh(x) for x in datos_originales['capacidad_total']]
    
    # Crear figura con eje secundario
    fig = make_subplots(
        specs=[[{"secondary_y": True}]],
        subplot_titles=[f"ANÁLISIS DE OFERTA NO PARTICIPANTE - {nombre_oferta}"]
    )
    
    # NO agregar barras de "asignada" (porque es cero)
    
    # Agregar barras de capacidad total como "no asignada" (color especial)
    fig.add_trace(
        go.Bar(
            x=datos_originales['fechas'],
            y=cantidad_no_asignada_gwh,
            name=f"{nombre_oferta} CAPACIDAD NO UTILIZADA",
            marker_color="#FF6B6B",  # Rojo más suave para no participantes
            text=[f"{val:.2f}" if val > 0 else "" for val in cantidad_no_asignada_gwh],
            textposition="inside",
            textfont=dict(color="white", size=10),
            showlegend=True,
            opacity=0.7,  # Más transparente para indicar "no activa"
            # Patrón rayado para diferenciar
            marker=dict(
                line=dict(color="#E74C3C", width=2),
                pattern=dict(shape="/", size=8, solidity=0.3)
            )
        ),
        secondary_y=False
    )
    
    # Línea de precio indexado (color diferenciado)
    fig.add_trace(
        go.Scatter(
            x=datos_originales['fechas'],
            y=datos_originales['precio_indexado'],
            mode="lines+markers",
            name=f"{nombre_oferta} PRECIO OFERTADO (INDEXADO)",
            line=dict(
                color="#95A5A6",  # Gris para indicar inactivo
                width=3,
                dash="dash",  # Línea punteada
                shape='spline',
                smoothing=0.8
            ),
            marker=dict(
                size=8,
                color="#95A5A6",
                line=dict(color="white", width=2),
                symbol="diamond"  # Símbolo diferente
            ),
            showlegend=True
        ),
        secondary_y=True
    )
    
    # Línea de precio sin indexar (color diferenciado)
    fig.add_trace(
        go.Scatter(
            x=datos_originales['fechas'],
            y=datos_originales['precio_sin_indexar'],
            mode="lines+markers",
            name=f"{nombre_oferta} PRECIO OFERTADO (SIN INDEXAR)",
            line=dict(
                color="#BDC3C7",  # Gris más claro
                width=3,
                dash="dot",  # Línea punteada diferente
                shape='spline',
                smoothing=0.8
            ),
            marker=dict(
                size=8,
                color="#BDC3C7",
                line=dict(color="white", width=2),
                symbol="square"  # Símbolo diferente
            ),
            showlegend=True
        ),
        secondary_y=True
    )
    
    # Configurar eje Y principal (GWh)
    fig.update_yaxes(
        title_text="GWh (No Utilizada)",
        title_font=dict(size=14, color="#E74C3C"),
        tickfont=dict(size=12),
        showgrid=True,
        gridcolor="lightgray",
        secondary_y=False
    )
    
    # Configurar eje Y secundario ($/kWh)
    fig.update_yaxes(
        title_text="$/kWh (Precio Ofertado)",
        title_font=dict(size=14, color="#95A5A6"),
        tickfont=dict(size=12),
        tickprefix="$ ",
        showgrid=False,
        secondary_y=True
    )
    
    # Eje X
    fig.update_xaxes(
        title_text="FECHA",
        title_font=dict(size=14),
        tickfont=dict(size=10),
        tickangle=45,
        showgrid=True,
        gridcolor="lightgray",
        title_standoff=20
    )
    
    # Layout con tema para "no participante"
    fig.update_layout(
        title={
            'text': f"🚫 OFERTA NO PARTICIPANTE - {nombre_oferta}<br><sub>Capacidad no utilizada en optimización Pyomo</sub>",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16, 'color': '#E74C3C', 'family': 'Arial Black'}
        },
        width=1200,
        height=600,
        plot_bgcolor='#F8F9FA',  # Fondo gris muy claro
        paper_bgcolor='#FFFFFF',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.4,
            xanchor="center",
            x=0.5,
            font=dict(size=10)
        ),
        margin=dict(l=80, r=80, t=120, b=200),
        showlegend=True,
        
        # Añadir patrón de fondo para indicar "inactivo"
        shapes=[
            dict(
                type="rect",
                x0=0, x1=1, y0=0, y1=1,
                xref="paper", yref="paper",
                fillcolor="rgba(231, 76, 60, 0.05)",
                layer="below",
                line=dict(width=0)
            )
        ]
    )
    
    # Anotación explicativa
    fig.add_annotation(
        x=0.02, y=0.98,
        xref="paper", yref="paper",
        text="⚠️ Esta oferta NO participó en la optimización Pyomo<br>Se muestra la capacidad total ofertada como no utilizada",
        showarrow=False,
        font=dict(size=11, color="#E74C3C"),
        bgcolor="rgba(255,255,255,0.9)",
        bordercolor="#E74C3C",
        borderwidth=2,
        align="left",
        xanchor="left",
        yanchor="top"
    )
    
    logger.info(f"Gráfica de no participante creada para {nombre_oferta}")
    return fig

def extraer_datos_oferta_original(nombre_oferta, resultados_dict):
    """
    Extrae datos originales de ofertas no participantes desde RESUMEN EJECUTIVO.
    
    Args:
        nombre_oferta (str): Nombre de la oferta
        resultados_dict (dict): Resultados completos
        
    Returns:
        dict: Datos originales o None
    """
    if "RESUMEN EJECUTIVO" not in resultados_dict:
        return None
    
    resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
    
    # Buscar columnas de esta oferta
    col_cantidad = f"{nombre_oferta} CANTIDAD (KWh)"
    col_precio = f"{nombre_oferta} PRECIO ($/KWh)"
    col_precio_indexado = f"{nombre_oferta} PRECIO INDEXADO ($/KWh)"
    
    if not any(col in resumen_df.columns for col in [col_cantidad, col_precio, col_precio_indexado]):
        return None
    
    datos = {
        'fechas': [],
        'capacidad_total': [],  # Toda la capacidad ofertada
        'precio_indexado': [],
        'precio_sin_indexar': []
    }
    
    for _, row in resumen_df.iterrows():
        fecha = row['FECHA']
        # Para no participantes, usar datos originales (si están disponibles)
        # O asumir capacidad basada en precios ofertados
        capacidad = row[col_cantidad] if col_cantidad in resumen_df.columns and pd.notna(row[col_cantidad]) else 0
        precio_sin_indexar = row[col_precio] if col_precio in resumen_df.columns and pd.notna(row[col_precio]) else 0
        precio_indexado = row[col_precio_indexado] if col_precio_indexado in resumen_df.columns and pd.notna(row[col_precio_indexado]) else 0
        
        # Incluir datos aunque sean cero (para mostrar la no participación)
        datos['fechas'].append(fecha)
        datos['capacidad_total'].append(capacidad)
        datos['precio_indexado'].append(precio_indexado)
        datos['precio_sin_indexar'].append(precio_sin_indexar)
    
    return datos if datos['fechas'] else None

def crear_graficas_por_oferta(resultados_dict, output_dir):
    """
    Crea gráficas individuales para TODAS las ofertas: participantes Y no participantes.
    ACTUALIZADO: Incluye gráficas informativas para ofertas no participantes.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        output_dir (Path): Directorio donde guardar las gráficas
        
    Returns:
        dict: Diccionario con {oferta: ruta_archivo_html} para todas las gráficas creadas
    """
    logger.info("Creando gráficas para TODAS las ofertas (participantes + no participantes)")
    print("🔹 Creando gráficas completas (participantes + no participantes)...")
    
    # Asegurar que el directorio existe
    output_dir = ensure_directory_exists(output_dir)
    
    # Extraer ofertas del resumen ejecutivo
    if "RESUMEN EJECUTIVO" not in resultados_dict:
        logger.warning("No se encontró RESUMEN EJECUTIVO")
        print("  ⚠️ No se encontró RESUMEN EJECUTIVO")
        return {}
    
    resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
    
    # 1. IDENTIFICAR TODAS LAS OFERTAS DEL SISTEMA
    todas_las_ofertas = set()
    for col in resumen_df.columns:
        if "CANTIDAD (KWh)" in col:
            nombre_oferta = col.replace(" CANTIDAD (KWh)", "")
            todas_las_ofertas.add(nombre_oferta)
    
    # 2. SEPARAR PARTICIPANTES VS NO PARTICIPANTES
    ofertas_participantes = set()
    ofertas_no_participantes = set()
    
    for nombre_oferta in todas_las_ofertas:
        col_cantidad = f"{nombre_oferta} CANTIDAD (KWh)"
        if col_cantidad in resumen_df.columns:
            # Verificar si tiene energía asignada (participó)
            total_asignado = resumen_df[col_cantidad].sum()
            if total_asignado > 0:
                ofertas_participantes.add(nombre_oferta)
            else:
                ofertas_no_participantes.add(nombre_oferta)
    
    print(f"  📋 Ofertas encontradas: {len(todas_las_ofertas)} total")
    print(f"    ✅ Participantes: {len(ofertas_participantes)}")
    print(f"    ❌ No participantes: {len(ofertas_no_participantes)}")
    
    graficas_creadas = {}
    
    # 3. CREAR GRÁFICAS PARA OFERTAS PARTICIPANTES
    print(f"\n  🔄 Procesando ofertas PARTICIPANTES...")
    for nombre_oferta in sorted(ofertas_participantes):
        try:
            print(f"    📊 {nombre_oferta} (PARTICIPANTE)")
            
            # Extraer datos de Pyomo
            datos_oferta = extraer_datos_para_grafica_oferta(resultados_dict, nombre_oferta)
            
            if datos_oferta is None:
                print(f"      ⚠️ Sin datos suficientes")
                continue
            
            # Crear gráfica normal
            fig = crear_grafica_oferta_individual(nombre_oferta, datos_oferta)
            
            if fig is None:
                print(f"      ❌ Error al crear gráfica")
                continue
            
            # Guardar
            nombre_archivo_seguro = nombre_oferta.replace(" ", "_").replace("-", "_").replace("/", "_")
            archivo_grafica = output_dir / f"oferta_{nombre_archivo_seguro}.html"
            pyo.plot(fig, filename=str(archivo_grafica), auto_open=False)
            
            graficas_creadas[nombre_oferta] = archivo_grafica
            print(f"      ✅ Guardada: {archivo_grafica.name}")
            
        except Exception as e:
            logger.error(f"Error en participante {nombre_oferta}: {e}")
            print(f"      ❌ Error: {e}")
            continue
    
    # 4. CREAR GRÁFICAS PARA OFERTAS NO PARTICIPANTES
    print(f"\n  🔄 Procesando ofertas NO PARTICIPANTES...")
    for nombre_oferta in sorted(ofertas_no_participantes):
        try:
            print(f"    🚫 {nombre_oferta} (NO PARTICIPANTE)")
            
            datos_originales = extraer_datos_oferta_original(nombre_oferta, resultados_dict)
            
            if datos_originales is None:
                print(f"      ⚠️ Sin datos originales")
                continue
            
            fig = crear_grafica_oferta_no_participante(nombre_oferta, datos_originales)
            
            if fig is None:
                print(f"      ❌ Error al crear gráfica")
                continue
            
            # CORREGIDO: Sin caracteres problemáticos
            nombre_limpio = nombre_oferta.replace(" ", "_").replace("-", "_").replace("/", "_")
            archivo_grafica = output_dir / f"oferta_{nombre_limpio}.html"
            pyo.plot(fig, filename=str(archivo_grafica), auto_open=False)
            
            graficas_creadas[nombre_oferta] = archivo_grafica
            print(f"      ✅ Guardada: {archivo_grafica.name}")
            
        except Exception as e:
            logger.error(f"Error en no participante {nombre_oferta}: {e}")
            print(f"      ❌ Error: {e}")
            continue
    # 5. RESUMEN FINAL
    participantes_creadas = sum(1 for k in graficas_creadas.keys() if k in ofertas_participantes)
    no_participantes_creadas = sum(1 for k in graficas_creadas.keys() if k in ofertas_no_participantes)
    
    print(f"\n  📊 RESUMEN DE GRÁFICAS CREADAS:")
    print(f"    ✅ Participantes: {participantes_creadas}/{len(ofertas_participantes)}")
    print(f"    🚫 No participantes: {no_participantes_creadas}/{len(ofertas_no_participantes)}")
    print(f"    🎯 Total: {len(graficas_creadas)}/{len(todas_las_ofertas)}")
    
    return graficas_creadas

def crear_datos_basicos_oferta(nombre_oferta, resumen_df):
    """
    Crea datos básicos para ofertas que no participaron en Pyomo.
    Intenta extraer información mínima del RESUMEN EJECUTIVO.
    
    Args:
        nombre_oferta (str): Nombre de la oferta
        resumen_df (DataFrame): DataFrame del resumen ejecutivo
        
    Returns:
        dict: Datos básicos o None si no se pueden crear
    """
    try:
        # Buscar columnas de esta oferta
        col_cantidad = f"{nombre_oferta} CANTIDAD (KWh)"
        col_precio = f"{nombre_oferta} PRECIO ($/KWh)"
        col_precio_indexado = f"{nombre_oferta} PRECIO INDEXADO ($/KWh)"
        
        # Verificar si al menos existe alguna columna
        columnas_existentes = [col for col in [col_cantidad, col_precio, col_precio_indexado] 
                              if col in resumen_df.columns]
        
        if not columnas_existentes:
            return None
        
        # Crear datos básicos (probablemente todo en cero para no participantes)
        datos = {
            'fechas': [],
            'cantidad_asignada': [],
            'cantidad_no_asignada': [],
            'precio_indexado': [],
            'precio_sin_indexar': []
        }
        
        # Procesar cada fila del resumen
        for _, row in resumen_df.iterrows():
            fecha = row['FECHA']
            
            # Extraer valores (probablemente ceros)
            cantidad = row[col_cantidad] if col_cantidad in resumen_df.columns and pd.notna(row[col_cantidad]) else 0
            precio_sin_indexar = row[col_precio] if col_precio in resumen_df.columns and pd.notna(row[col_precio]) else 0
            precio_indexado = row[col_precio_indexado] if col_precio_indexado in resumen_df.columns and pd.notna(row[col_precio_indexado]) else 0
            
            # Agregar datos (incluso si son cero)
            datos['fechas'].append(fecha)
            datos['cantidad_asignada'].append(cantidad)
            datos['cantidad_no_asignada'].append(0)  # No participantes no tienen "no asignada"
            datos['precio_indexado'].append(precio_indexado)
            datos['precio_sin_indexar'].append(precio_sin_indexar)
        
        return datos if datos['fechas'] else None
        
    except Exception as e:
        logger.warning(f"Error creando datos básicos para {nombre_oferta}: {e}")
        return None

def generar_reporte_consolidado_ofertas(graficas_creadas, output_dir, resultados_dict=None):
    """
    Genera reporte HTML con ofertas organizadas: participantes vs no participantes.
    ACTUALIZADO: Enlaces funcionales para ambos tipos de ofertas.
    """
    if not graficas_creadas:
        logger.warning("No hay gráficas creadas para consolidar")
        return None
    
    # Extraer TODAS las ofertas del sistema
    todas_las_ofertas = set()
    ofertas_con_graficas = set(graficas_creadas.keys())
    
    # Buscar ofertas en RESUMEN EJECUTIVO
    if resultados_dict and "RESUMEN EJECUTIVO" in resultados_dict:
        resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
        for col in resumen_df.columns:
            if "CANTIDAD (KWh)" in col:
                oferta = col.replace(" CANTIDAD (KWh)", "")
                todas_las_ofertas.add(oferta)
    
    # Buscar ofertas en hojas DEMANDA ASIGNADA
    if resultados_dict:
        for clave in resultados_dict.keys():
            if "DEMANDA ASIGNADA" in clave:
                try:
                    parte_oferta = clave.split("DEMANDA ASIGNADA ")[1].split(" IT")[0]
                    todas_las_ofertas.add(parte_oferta)
                except:
                    continue
    
    if not todas_las_ofertas:
        todas_las_ofertas = ofertas_con_graficas.copy()
    
    # Separar ofertas PARTICIPANTES vs NO PARTICIPANTES
    ofertas_participantes = set()
    ofertas_no_participantes = set()
    
    if resultados_dict and "RESUMEN EJECUTIVO" in resultados_dict:
        resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
        for oferta in todas_las_ofertas:
            col_cantidad = f"{oferta} CANTIDAD (KWh)"
            if col_cantidad in resumen_df.columns:
                total_asignado = resumen_df[col_cantidad].sum()
                if total_asignado > 0:
                    ofertas_participantes.add(oferta)
                else:
                    ofertas_no_participantes.add(oferta)
            else:
                ofertas_no_participantes.add(oferta)
    else:
        ofertas_participantes = ofertas_con_graficas.copy()
    
    total_ofertas = len(todas_las_ofertas)
    num_participantes = len(ofertas_participantes)
    num_no_participantes = len(ofertas_no_participantes)
    
    # HTML escalable
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Reporte de Ofertas - Sistema de Optimización</title>
        <meta charset="utf-8">
        <style>
            body {{
                font-family: 'Segoe UI', Arial, sans-serif;
                margin: 20px;
                background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
                min-height: 100vh;
            }}
            .header {{
                text-align: center;
                background: white;
                padding: 30px;
                border-radius: 12px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);
                margin-bottom: 30px;
            }}
            .stats-container {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
                margin: 20px 0;
                max-width: 800px;
                margin-left: auto;
                margin-right: auto;
            }}
            .stat-box {{
                background: white;
                padding: 25px;
                border-radius: 10px;
                box-shadow: 0 3px 10px rgba(0,0,0,0.1);
                text-align: center;
                transition: transform 0.3s ease;
            }}
            .stat-box:hover {{
                transform: translateY(-5px);
            }}
            .stat-number {{
                font-size: 2.5em;
                font-weight: bold;
                margin-bottom: 5px;
            }}
            .stat-number.total {{ color: #3498db; }}
            .stat-number.participantes {{ color: #2ecc71; }}
            .stat-number.no-participantes {{ color: #e74c3c; }}
            .stat-label {{
                color: #666;
                font-size: 0.95em;
                font-weight: 500;
            }}
            .section-header {{
                background: white;
                margin: 30px 0 20px 0;
                padding: 25px;
                border-radius: 10px;
                box-shadow: 0 3px 10px rgba(0,0,0,0.1);
                border-left: 6px solid #2ecc71;
            }}
            .section-header.no-participantes {{
                border-left-color: #e74c3c;
            }}
            .section-title {{
                margin: 0 0 12px 0;
                font-size: 1.6em;
                color: #2c3e50;
                font-weight: 600;
            }}
            .section-description {{
                margin: 0;
                color: #7f8c8d;
                line-height: 1.5;
            }}
            .ofertas-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
                gap: 20px;
                margin-top: 20px;
            }}
            .oferta-card {{
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 3px 10px rgba(0,0,0,0.1);
                transition: all 0.3s ease;
                border: 2px solid transparent;
            }}
            .oferta-card:hover {{
                transform: translateY(-3px);
                box-shadow: 0 6px 20px rgba(0,0,0,0.15);
                border-color: #3498db;
            }}
            .oferta-card.no-participante {{
                border-left: 4px solid #e74c3c;
                background: #fdf2f2;
            }}
            .oferta-name {{
                font-size: 1.1em;
                font-weight: bold;
                color: #2c3e50;
                margin-bottom: 10px;
            }}
            .oferta-description {{
                color: #7f8c8d;
                margin-bottom: 15px;
                font-size: 0.9em;
                line-height: 1.4;
            }}
            .oferta-link {{
                display: inline-block;
                padding: 10px 18px;
                background: linear-gradient(135deg, #3498db, #2980b9);
                color: white;
                text-decoration: none;
                border-radius: 6px;
                font-weight: 500;
                transition: all 0.3s ease;
                font-size: 0.9em;
            }}
            .oferta-link:hover {{
                background: linear-gradient(135deg, #2ecc71, #27ae60);
                transform: scale(1.05);
            }}
            .oferta-link.no-participante {{
                background: linear-gradient(135deg, #e74c3c, #c0392b);
            }}
            .oferta-link.no-participante:hover {{
                background: linear-gradient(135deg, #f39c12, #e67e22);
            }}
            .timestamp {{
                text-align: center;
                color: #95a5a6;
                font-size: 12px;
                margin-top: 40px;
                padding: 15px;
                background: white;
                border-radius: 8px;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📊 REPORTE COMPLETO DE OFERTAS</h1>
            <h2>Sistema de Optimización Energética Pyomo</h2>
            <p>Análisis completo de participación y adjudicación de ofertas</p>
        </div>
        
        <div class="stats-container">
            <div class="stat-box">
                <div class="stat-number total">{total_ofertas}</div>
                <div class="stat-label">Total en Sistema</div>
            </div>
            <div class="stat-box">
                <div class="stat-number participantes">{num_participantes}</div>
                <div class="stat-label">Participaron en Pyomo</div>
            </div>
            <div class="stat-box">
                <div class="stat-number no-participantes">{num_no_participantes}</div>
                <div class="stat-label">No Participaron</div>
            </div>
        </div>
    """
    
    # SECCIÓN: Ofertas participantes
    if ofertas_participantes:
        porcentaje_participacion = (num_participantes / total_ofertas * 100) if total_ofertas > 0 else 0
        html_content += f"""
        <div class="section-header">
            <h2 class="section-title">✅ Ofertas Participantes ({num_participantes})</h2>
            <p class="section-description">
                Estas ofertas participaron en el proceso de optimización Pyomo y tienen análisis 
                detallado de demanda asignada, no asignada y evolución de precios. 
                <strong>Tasa de participación: {porcentaje_participacion:.1f}%</strong>
            </p>
        </div>
        
        <div class="ofertas-grid">
        """
        
        for nombre_oferta in sorted(ofertas_participantes):
            if nombre_oferta in graficas_creadas:
                archivo_grafica = graficas_creadas[nombre_oferta]
                nombre_archivo = archivo_grafica.name
                html_content += f"""
                    <div class="oferta-card">
                        <div class="oferta-name">{nombre_oferta}</div>
                        <div class="oferta-description">
                            Análisis completo de asignación energética y evolución de precios indexados
                        </div>
                        <a href="{nombre_archivo}" class="oferta-link" target="_blank">
                            📈 Ver Análisis Completo
                        </a>
                    </div>
                """
        
        html_content += "</div>"
    
    # SECCIÓN: Ofertas no participantes CON ENLACES
    if ofertas_no_participantes:
        html_content += f"""
        <div class="section-header no-participantes">
            <h2 class="section-title">❌ Ofertas No Participantes ({num_no_participantes})</h2>
            <p class="section-description">
                Estas ofertas estaban disponibles en el sistema pero no participaron en el 
                proceso de optimización Pyomo. Se muestra su capacidad como "no utilizada".
            </p>
        </div>
        
        <div class="ofertas-grid">
        """
        
        for nombre_oferta in sorted(ofertas_no_participantes):
            if nombre_oferta in graficas_creadas:
                archivo_grafica = graficas_creadas[nombre_oferta]
                nombre_archivo = archivo_grafica.name
                html_content += f"""
                    <div class="oferta-card no-participante">
                        <div class="oferta-name">{nombre_oferta}</div>
                        <div class="oferta-description">
                            Capacidad total ofertada mostrada como "no utilizada" en la optimización
                        </div>
                        <a href="{nombre_archivo}" class="oferta-link no-participante" target="_blank">
                            🚫 Ver Análisis (No Participante)
                        </a>
                    </div>
                """
            else:
                html_content += f"""
                    <div class="oferta-card no-participante">
                        <div class="oferta-name">{nombre_oferta}</div>
                        <div class="oferta-description">
                            Oferta presente en el sistema pero sin datos suficientes para análisis
                        </div>
                        <span style="color: #e74c3c; font-size: 0.9em; font-weight: 500;">
                            ⚠️ Sin gráfica disponible
                        </span>
                    </div>
                """
        
        html_content += "</div>"
    else:
        html_content += f"""
        <div class="section-header no-participantes">
            <h2 class="section-title">❌ Ofertas No Participantes</h2>
            <p class="section-description">Ofertas que no participaron en la optimización</p>
        </div>
        
        <div class="no-data-message">
            <p>🎉 ¡Excelente! Todas las ofertas del sistema participaron en la optimización Pyomo.</p>
        </div>
        """
    
    html_content += f"""
        <div class="timestamp">
            Generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M:%S')}<br>
            Sistema escalable para cualquier número de ofertas
        </div>
    </body>
    </html>
    """
    
    # Guardar archivo
    archivo_consolidado = output_dir / "reporte_ofertas.html"
    with open(archivo_consolidado, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"Reporte completo de ofertas creado: {archivo_consolidado}")
    print(f"  📋 Reporte completo: {archivo_consolidado.name}")
    print(f"    - Participantes: {num_participantes}")
    print(f"    - No participantes: {num_no_participantes}")
    
    return archivo_consolidado

def crear_grafica_consolidada_ofertas_simplificada(resultados_dict):
    """
    Dashboard CONSOLIDADO corregido: sin solapamientos y filtros funcionales.
    """
    logger.info("Creando dashboard consolidado corregido")
    
    if "RESUMEN EJECUTIVO" not in resultados_dict:
        logger.warning("No se encontró RESUMEN EJECUTIVO")
        return None
    
    resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
    
    if resumen_df.empty:
        logger.warning("RESUMEN EJECUTIVO está vacío")
        return None
    
    # 1. EXTRAER DATOS
    ofertas_adjudicadas = []
    datos_energia = {}
    datos_precios = {}
    
    for col in resumen_df.columns:
        if "CANTIDAD (KWh)" in col:
            oferta = col.replace(" CANTIDAD (KWh)", "")
            total_energia = resumen_df[col].sum()
            
            if total_energia > 0:
                ofertas_adjudicadas.append((oferta, total_energia))
                datos_energia[oferta] = [convert_to_gwh(val) if pd.notna(val) else 0 
                                       for val in resumen_df[col]]
                
                col_precio = f"{oferta} PRECIO INDEXADO ($/KWh)"
                if col_precio in resumen_df.columns:
                    datos_precios[oferta] = [val if pd.notna(val) and val > 0 else None 
                                           for val in resumen_df[col_precio]]
    
    ofertas_adjudicadas.sort(key=lambda x: x[1], reverse=True)
    ofertas_adjudicadas = [x[0] for x in ofertas_adjudicadas]
    fechas = resumen_df['FECHA'].tolist()
    
    # 2. COLORES
    colores_energia = [
        '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', 
        '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#aec7e8', '#ffbb78',
        '#98df8a', '#ff9896', '#c5b0d5', '#c49c94', '#f7b6d3', '#c7c7c7'
    ]
    
    colores_precios = [
        '#0d47a1', '#e65100', '#1b5e20', '#b71c1c', '#4a148c', '#3e2723',
        '#880e4f', '#424242', '#827717', '#006064', '#01579b', '#bf360c'
    ]
    
    # 3. CREAR FIGURA
    fig = go.Figure()
    
    # 4. AGREGAR BARRAS DE ENERGÍA
    for i, oferta in enumerate(ofertas_adjudicadas):
        color_energia = colores_energia[i % len(colores_energia)]
        
        fig.add_trace(
            go.Bar(
                x=fechas,
                y=datos_energia[oferta],
                name=f"⚡ {oferta}",
                marker_color=color_energia,
                opacity=0.85,
                text=[f"{val:.1f}" if val > 1 else f"{val:.2f}" if val > 0 else "" 
                      for val in datos_energia[oferta]],
                textposition="inside",
                textfont=dict(color="white", size=10, family="Arial Black"),
                marker=dict(line=dict(color="white", width=1)),
                hovertemplate=(
                    f"<b>⚡ ENERGÍA - {oferta}</b><br>"
                    "📅 %{x}<br>"
                    "🔋 %{y:.2f} GWh<br>"
                    "<extra></extra>"
                ),
                yaxis="y",
                legendgroup="energia",
                legendgrouptitle_text="🔋 ENERGÍA ASIGNADA (GWh)"
            )
        )
    
    # 5. AGREGAR LÍNEAS DE PRECIOS
    for i, oferta in enumerate(ofertas_adjudicadas):
        if oferta in datos_precios:
            color_precio = colores_precios[i % len(colores_precios)]
            
            fig.add_trace(
                go.Scatter(
                    x=fechas,
                    y=datos_precios[oferta],
                    mode="lines+markers",
                    name=f"💰 {oferta}",
                    line=dict(
                        color=color_precio, 
                        width=4,
                        shape='spline',
                        smoothing=1.0
                    ),
                    marker=dict(
                        size=10, 
                        color=color_precio,
                        line=dict(color="white", width=2),
                        symbol="circle"
                    ),
                    connectgaps=False,
                    hovertemplate=(
                        f"<b>💰 PRECIO - {oferta}</b><br>"
                        "📅 %{x}<br>"
                        "💵 $%{y:.2f}/kWh<br>"
                        "<extra></extra>"
                    ),
                    yaxis="y2",
                    legendgroup="precios",
                    legendgrouptitle_text="💰 PRECIOS INDEXADOS ($/kWh)"
                )
            )
    
    # 6. FILTROS QUE AJUSTAN LA GRÁFICA
    años_disponibles = sorted(list(set([
        fecha.split('/')[1] if '/' in fecha else fecha[-4:] 
        for fecha in fechas
    ])))
    
    botones_filtro = []
    
    # Botón "Todos" - datos completos
    x_todos = []
    y_todos = []
    for oferta in ofertas_adjudicadas:
        x_todos.append(fechas)
        y_todos.append(datos_energia[oferta])
    for oferta in ofertas_adjudicadas:
        if oferta in datos_precios:
            x_todos.append(fechas)
            y_todos.append(datos_precios[oferta])
    
    botones_filtro.append(
        dict(
            label="📅 TODOS LOS AÑOS",
            method="restyle",
            args=[{"x": x_todos, "y": y_todos}]
        )
    )
    
    # Botones por año - datos filtrados
    for año in años_disponibles:
        fechas_año = [f for f in fechas if año in f]
        indices_año = [i for i, f in enumerate(fechas) if año in f]
        
        x_año = []
        y_año = []
        
        # Datos de energía filtrados
        for oferta in ofertas_adjudicadas:
            x_año.append(fechas_año)
            y_año.append([datos_energia[oferta][i] for i in indices_año])
        
        # Datos de precios filtrados
        for oferta in ofertas_adjudicadas:
            if oferta in datos_precios:
                x_año.append(fechas_año)
                y_año.append([datos_precios[oferta][i] for i in indices_año])
        
        botones_filtro.append(
            dict(
                label=f"📆 AÑO {año}",
                method="restyle",
                args=[{"x": x_año, "y": y_año}]
            )
        )
    
    # 7. LAYOUT CORREGIDO
    fig.update_layout(
        title={
            'text': f"DASHBOARD ENERGÉTICO CONSOLIDADO<br><sub>📊 {len(ofertas_adjudicadas)} ofertas adjudicadas | Energía y Precios por Período</sub>",
            'x': 0.4,  # CENTRADO EN ÁREA DE GRÁFICA
            'xanchor': 'center',
            'font': {'size': 24, 'color': '#1f4e79', 'family': 'Arial Black'}
        },
        
        width=2000,  # MÁS ANCHO
        height=1000,
        
        plot_bgcolor='white',
        paper_bgcolor='#fafafa',
        barmode='stack',
        
        # LEYENDA MOVIDA MÁS A LA DERECHA
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.98,
            xanchor="left", 
            x=1.15,  # MÁS A LA DERECHA
            font=dict(size=12, family="Arial"),
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor="#1f4e79",
            borderwidth=2,
            itemsizing="constant",
            itemwidth=40,
            tracegroupgap=20
        ),
        
        # FILTROS
        updatemenus=[
            dict(
                type="dropdown",
                direction="down",
                x=0.02,
                y=1.15,
                xanchor="left",
                yanchor="top",
                buttons=botones_filtro,
                font=dict(size=14, family="Arial Black"),
                bgcolor="rgba(255,255,255,0.95)",
                bordercolor="#1f4e79",
                borderwidth=2,
                active=0
            )
        ],
        
        # MÁRGENES AMPLIADOS
        margin=dict(l=100, r=600, t=150, b=150)  # MÁS ESPACIO DERECHO Y ABAJO
    )
    
    # 8. EJES
    fig.update_xaxes(
        title=dict(
            text="📅 FECHAS",
            font=dict(size=16, color="#1f4e79")
        ),
        tickfont=dict(size=13),
        tickangle=45,
        showgrid=True,
        gridcolor="rgba(128,128,128,0.2)",
        linecolor="#1f4e79",
        linewidth=2
    )
    
    # Eje Y principal (Energía)
    max_energia = max([max(datos_energia[oferta]) for oferta in ofertas_adjudicadas if datos_energia[oferta]])
    fig.update_layout(
        yaxis=dict(
            title=dict(
                text="⚡ ENERGÍA ASIGNADA (GWh)",
                font=dict(size=16, color="#1f4e79")
            ),
            tickfont=dict(size=13, color="#1f4e79"),
            showgrid=True,
            gridcolor="rgba(31,78,121,0.1)",
            linecolor="#1f4e79",
            linewidth=2,
            range=[0, max_energia * 1.1],
            side="left"
        )
    )
    
    # Eje Y secundario (Precios)
    precios_validos = []
    for oferta in ofertas_adjudicadas:
        if oferta in datos_precios:
            precios_validos.extend([p for p in datos_precios[oferta] if p is not None])
    
    if precios_validos:
        max_precio = max(precios_validos)
        min_precio = min(precios_validos)
        
        fig.update_layout(
            yaxis2=dict(
                title=dict(
                    text="💰 PRECIO INDEXADO ($/kWh)",
                    font=dict(size=16, color="#e74c3c")
                ),
                tickfont=dict(size=13, color="#e74c3c"),
                tickprefix="$ ",
                overlaying="y",
                side="right",
                showgrid=False,
                linecolor="#e74c3c",
                linewidth=2,
                range=[min_precio * 0.9, max_precio * 1.1]
            )
        )
    
    # 9. GUÍA RÁPIDA AL FONDO
    fig.add_annotation(
        x=0.5, y=-0.12,  # ABAJO EN EL CENTRO
        xref="paper", yref="paper",
        text=(
            "<b>💡 GUÍA RÁPIDA:</b> "
            "🔋 Barras = Energía por oferta | "
            "💰 Líneas = Precios indexados | "
            "📅 Dropdown = Filtrar por año | "
            "🖱️ Hover = Ver detalles | "
            "📊 Escalas: GWh (izq) - $/kWh (der)"
        ),
        showarrow=False,
        font=dict(size=11, color="#2c3e50"),
        bgcolor="rgba(240,248,255,0.95)",
        bordercolor="#3498db",
        borderwidth=1,
        align="center",
        xanchor="center",
        yanchor="top"
    )
    
    logger.info(f"Dashboard consolidado corregido: {len(ofertas_adjudicadas)} ofertas")
    return fig

# FUNCIÓN WRAPPER PARA COMPATIBILIDAD
def crear_grafica_consolidada_ofertas(resultados_dict):
    """Wrapper que llama a la versión simplificada"""
    return crear_grafica_consolidada_ofertas_simplificada(resultados_dict)

def crear_graficas_por_oferta_completo(resultados_dict, output_dir):
    """
    Crea gráficas individuales + consolidada + reporte completo.
    ACTUALIZADO: Pasa resultados_dict al reporte para detectar ofertas no participantes.
    """
    logger.info("Creando gráficas completas por oferta (individuales + consolidada)")
    print("🔹 Creando gráficas completas por oferta...")
    
    resultado = {
        'individuales': {},
        'consolidada': None,
        'reporte_individuales': None
    }
    
    # 1. Crear gráficas individuales
    print("  📋 Generando gráficas individuales...")
    graficas_individuales = crear_graficas_por_oferta(resultados_dict, output_dir)
    resultado['individuales'] = graficas_individuales
    
    # 2. Crear gráfica consolidada
    print("  📊 Generando gráfica consolidada...")
    try:
        fig_consolidada = crear_grafica_consolidada_ofertas(resultados_dict)
        
        if fig_consolidada:
            archivo_consolidada = output_dir / "08_consolidado_ofertas.html"
            pyo.plot(fig_consolidada, filename=str(archivo_consolidada), auto_open=False)
            resultado['consolidada'] = archivo_consolidada
            print(f"    ✅ Gráfica consolidada guardada: {archivo_consolidada.name}")
        else:
            print("    ⚠️ No se pudo crear la gráfica consolidada")
            
    except Exception as e:
        logger.error(f"Error al crear gráfica consolidada: {e}")
        print(f"    ❌ Error en gráfica consolidada: {e}")
    
    # 3. Crear reporte completo (ACTUALIZADO: pasa resultados_dict)
    if graficas_individuales:
        print("  📝 Generando reporte completo de ofertas...")
        try:
            reporte_completo = generar_reporte_consolidado_ofertas(
                graficas_individuales, 
                output_dir, 
                resultados_dict  # 🆕 AGREGADO
            )
            resultado['reporte_individuales'] = reporte_completo
            if reporte_completo:
                print(f"    ✅ Reporte completo: {reporte_completo.name}")
        except Exception as e:
            logger.error(f"Error al crear reporte completo: {e}")
            print(f"    ❌ Error en reporte completo: {e}")
    
    # Resumen
    total_creadas = len(graficas_individuales) + (1 if resultado['consolidada'] else 0)
    print(f"  🎯 Total gráficas creadas: {total_creadas}")
    print(f"    - Individuales: {len(graficas_individuales)}")
    print(f"    - Consolidada: {'✅' if resultado['consolidada'] else '❌'}")
    
    return resultado