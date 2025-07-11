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
    Replica el formato de la imagen proporcionada.
    
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
            marker_color="#7BA7D7",  # Azul claro similar a la imagen
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
            marker_color="#FF7F50",  # Naranja similar a la imagen
            text=[f"{val:.2f}" if val > 0 else "" for val in cantidad_no_asignada_gwh],
            textposition="inside",
            textfont=dict(color="white", size=10),
            showlegend=True,
            opacity=0.8
        ),
        secondary_y=False
    )
    
    # Agregar línea de precio promedio indexado (azul oscuro)
    fig.add_trace(
        go.Scatter(
            x=datos_oferta['fechas'],
            y=datos_oferta['precio_indexado'],
            mode="lines+markers",
            name=f"{nombre_oferta} PRECIO PROMEDIO",
            line=dict(
                color="#1f4e79",  # Azul oscuro
                width=3
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
    
    # Agregar línea de precio promedio sin indexar (verde)
    fig.add_trace(
        go.Scatter(
            x=datos_oferta['fechas'],
            y=datos_oferta['precio_sin_indexar'],
            mode="lines+markers",
            name=f"{nombre_oferta} PRECIO PROMEDIO SIN INDEXAR",
            line=dict(
                color="#2ecc71",  # Verde
                width=3
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
    
    # Configurar eje X
    fig.update_xaxes(
        title_text="FECHA",
        title_font=dict(size=14),
        tickfont=dict(size=12),
        tickangle=45,
        showgrid=True,
        gridcolor="lightgray"
    )
    
    # Configurar layout general
    fig.update_layout(
        title={
            'text': f"DEMANDA ASIGNADA Y NO ASIGNADA GWh - {nombre_oferta}",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16, 'color': '#1f4e79', 'family': 'Arial Black'}
        },
        width=1200,
        height=600,
        barmode='stack',  # Barras apiladas
        plot_bgcolor='white',
        paper_bgcolor='white',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5,
            font=dict(size=10)
        ),
        margin=dict(l=80, r=80, t=100, b=150),
        showlegend=True
    )
    
    logger.info(f"Gráfica creada exitosamente para {nombre_oferta}")
    return fig

def crear_graficas_por_oferta(resultados_dict, output_dir):
    """
    Crea gráficas individuales para cada oferta encontrada en los resultados.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        output_dir (Path): Directorio donde guardar las gráficas
        
    Returns:
        dict: Diccionario con {oferta: ruta_archivo_html} para las gráficas creadas exitosamente
    """
    logger.info("Creando gráficas individuales por oferta")
    print("🔹 Creando gráficas individuales por oferta...")
    
    # Asegurar que el directorio existe
    output_dir = ensure_directory_exists(output_dir)
    
    # Extraer todas las ofertas únicas del resumen ejecutivo
    if "RESUMEN EJECUTIVO" not in resultados_dict:
        logger.warning("No se encontró RESUMEN EJECUTIVO, no se pueden crear gráficas por oferta")
        print("  ⚠️ No se encontró RESUMEN EJECUTIVO")
        return {}
    
    resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
    
    # Identificar ofertas únicas buscando columnas que terminen en "CANTIDAD (KWh)"
    ofertas_encontradas = set()
    for col in resumen_df.columns:
        if "CANTIDAD (KWh)" in col:
            # Extraer nombre de la oferta
            nombre_oferta = col.replace(" CANTIDAD (KWh)", "")
            ofertas_encontradas.add(nombre_oferta)
    
    print(f"  📋 Ofertas encontradas: {len(ofertas_encontradas)}")
    for oferta in sorted(ofertas_encontradas):
        print(f"    - {oferta}")
    
    graficas_creadas = {}
    
    # Crear gráfica para cada oferta
    for nombre_oferta in sorted(ofertas_encontradas):
        try:
            print(f"  🔄 Procesando: {nombre_oferta}")
            
            # Extraer datos específicos de esta oferta
            datos_oferta = extraer_datos_para_grafica_oferta(resultados_dict, nombre_oferta)
            
            if datos_oferta is None:
                print(f"    ⚠️ Sin datos suficientes para {nombre_oferta}")
                continue
            
            # Crear la gráfica
            fig = crear_grafica_oferta_individual(nombre_oferta, datos_oferta)
            
            if fig is None:
                print(f"    ❌ Error al crear gráfica para {nombre_oferta}")
                continue
            
            # Generar nombre de archivo seguro
            nombre_archivo_seguro = nombre_oferta.replace(" ", "_").replace("-", "_").replace("/", "_")
            archivo_grafica = output_dir / f"oferta_{nombre_archivo_seguro}.html"
            
            # Exportar la gráfica
            pyo.plot(fig, filename=str(archivo_grafica), auto_open=False)
            
            graficas_creadas[nombre_oferta] = archivo_grafica
            print(f"    ✅ Gráfica guardada: {archivo_grafica.name}")
            
        except Exception as e:
            logger.error(f"Error al crear gráfica para {nombre_oferta}: {e}")
            print(f"    ❌ Error en {nombre_oferta}: {e}")
            continue
    
    print(f"  📊 Gráficas por oferta completadas: {len(graficas_creadas)}/{len(ofertas_encontradas)}")
    
    return graficas_creadas

def generar_reporte_consolidado_ofertas(graficas_creadas, output_dir):
    """
    Genera un reporte HTML consolidado con enlaces a todas las gráficas por oferta.
    
    Args:
        graficas_creadas (dict): Diccionario con las gráficas creadas
        output_dir (Path): Directorio de salida
        
    Returns:
        Path: Ruta al archivo de reporte consolidado
    """
    if not graficas_creadas:
        logger.warning("No hay gráficas creadas para consolidar")
        return None
    
    # Crear contenido HTML
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Reporte de Gráficas por Oferta</title>
        <meta charset="utf-8">
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            .header {{
                text-align: center;
                color: #1f4e79;
                margin-bottom: 30px;
            }}
            .oferta-container {{
                background-color: white;
                margin: 20px 0;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .oferta-link {{
                display: inline-block;
                padding: 10px 20px;
                background-color: #1f4e79;
                color: white;
                text-decoration: none;
                border-radius: 5px;
                margin: 5px;
            }}
            .oferta-link:hover {{
                background-color: #2ecc71;
            }}
            .grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px;
                margin-top: 20px;
            }}
            .timestamp {{
                text-align: center;
                color: #999;
                font-size: 12px;
                margin-top: 30px;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📊 GRÁFICAS POR OFERTA</h1>
            <h2>Análisis Individual de Demanda Asignada y Precios</h2>
            <p>Total de ofertas procesadas: <strong>{len(graficas_creadas)}</strong></p>
        </div>
        
        <div class="grid">
    """
    
    # Añadir enlace para cada oferta
    for nombre_oferta, archivo_grafica in sorted(graficas_creadas.items()):
        nombre_archivo = archivo_grafica.name
        html_content += f"""
            <div class="oferta-container">
                <h3>{nombre_oferta}</h3>
                <p>Gráfica de demanda asignada, no asignada y evolución de precios</p>
                <a href="{nombre_archivo}" class="oferta-link" target="_blank">
                    Ver Gráfica Completa
                </a>
            </div>
        """
    
    html_content += f"""
        </div>
        
        <div class="timestamp">
            Generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M:%S')}
        </div>
    </body>
    </html>
    """
    
    # Guardar archivo consolidado
    archivo_consolidado = output_dir / "reporte_ofertas.html"
    with open(archivo_consolidado, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"Reporte consolidado de ofertas creado: {archivo_consolidado}")
    print(f"  📋 Reporte consolidado creado: {archivo_consolidado.name}")
    
    return archivo_consolidado

def crear_grafica_consolidada_ofertas(resultados_dict):
    """
    Crea una sola gráfica que muestra todas las ofertas consolidadas.
    MEJORADO: Barras APILADAS por fecha con diferentes ofertas y líneas de precios.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        
    Returns:
        plotly.graph_objects.Figure: Figura de la gráfica consolidada
    """
    logger.info("Creando gráfica consolidada de todas las ofertas (barras apiladas)")
    
    if "RESUMEN EJECUTIVO" not in resultados_dict:
        logger.warning("No se encontró RESUMEN EJECUTIVO para gráfica consolidada")
        return None
    
    resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
    
    if resumen_df.empty:
        logger.warning("RESUMEN EJECUTIVO está vacío")
        return None
    
    # Extraer todas las ofertas
    ofertas_encontradas = set()
    for col in resumen_df.columns:
        if "CANTIDAD (KWh)" in col:
            oferta = col.replace(" CANTIDAD (KWh)", "")
            ofertas_encontradas.add(oferta)
    
    ofertas_ordenadas = sorted(list(ofertas_encontradas))
    
    if not ofertas_ordenadas:
        logger.warning("No se encontraron ofertas en el resumen ejecutivo")
        return None
    
    print(f"📊 Creando gráfica consolidada (BARRAS APILADAS) para {len(ofertas_ordenadas)} ofertas")
    
    # Extraer fechas únicas del resumen
    fechas = resumen_df['FECHA'].tolist()
    
    # Definir colores para cada oferta (paleta más distintiva y moderna)
    colores_ofertas = [
        '#1f4e79',  # Azul oscuro
        '#2ecc71',  # Verde
        '#e74c3c',  # Rojo
        '#f39c12',  # Naranja
        '#9b59b6',  # Morado
        '#3498db',  # Azul claro
        '#e67e22',  # Naranja oscuro
        '#1abc9c',  # Verde azulado
        '#34495e',  # Gris oscuro
        '#c0392b',  # Rojo oscuro
        '#d35400',  # Naranja rojizo
        '#8e44ad'   # Morado oscuro
    ]
    
    # Crear figura con eje secundario
    fig = make_subplots(
        specs=[[{"secondary_y": True}]],
        subplot_titles=["CONSOLIDADO: DEMANDA ASIGNADA Y PRECIOS POR OFERTA (Barras Apiladas)"]
    )
    
    # CAMBIO PRINCIPAL: Procesar cada oferta para BARRAS APILADAS
    for i, oferta in enumerate(ofertas_ordenadas):
        color_oferta = colores_ofertas[i % len(colores_ofertas)]
        
        # Extraer datos de esta oferta
        col_cantidad = f"{oferta} CANTIDAD (KWh)"
        col_precio_indexado = f"{oferta} PRECIO INDEXADO ($/KWh)"
        
        if col_cantidad in resumen_df.columns:
            # Convertir cantidades a GWh
            cantidades_gwh = [convert_to_gwh(val) if pd.notna(val) else 0 
                            for val in resumen_df[col_cantidad]]
            
            # 🆕 CAMBIO: Agregar barras APILADAS en lugar de agrupadas
            fig.add_trace(
                go.Bar(
                    x=fechas,
                    y=cantidades_gwh,
                    name=f"{oferta}",
                    marker_color=color_oferta,
                    text=[f"{val:.1f}" if val > 0.1 else "" for val in cantidades_gwh],  # Solo mostrar texto si es > 0.1
                    textposition="inside",
                    textfont=dict(color="white", size=8, family="Arial Black"),  # Texto más pequeño para barras apiladas
                    opacity=0.85,
                    showlegend=True,
                    # 🔧 Configuración para barras más estilizadas
                    marker=dict(
                        line=dict(color="white", width=0.5)  # Borde blanco sutil
                    )
                ),
                secondary_y=False
            )
            
            # Agregar línea de precio indexado (si existe) - SIN CAMBIOS
            if col_precio_indexado in resumen_df.columns:
                precios_indexados = [val if pd.notna(val) and val > 0 else None 
                                   for val in resumen_df[col_precio_indexado]]
                
                # Solo mostrar línea si hay precios válidos
                if any(p is not None for p in precios_indexados):
                    fig.add_trace(
                        go.Scatter(
                            x=fechas,
                            y=precios_indexados,
                            mode="lines+markers",
                            name=f"{oferta} Precio",
                            line=dict(
                                color=color_oferta,
                                width=2.5,  # Líneas un poco más gruesas
                                dash='dot'
                            ),
                            marker=dict(
                                size=7,
                                color=color_oferta,
                                line=dict(color="white", width=2),
                                symbol="circle"
                            ),
                            showlegend=True,
                            connectgaps=False,
                            # 🆕 Agregar información adicional en hover
                            hovertemplate=f"<b>{oferta} Precio</b><br>" +
                                        "Período: %{x}<br>" +
                                        "Precio: $%{y:.4f}/kWh<br>" +
                                        "<extra></extra>"
                        ),
                        secondary_y=True
                    )
    
    # Configurar eje Y principal (GWh) - MEJORADO
    fig.update_yaxes(
        title_text="GWh Asignados (Apilados)",
        title_font=dict(size=14, color="#1f4e79", family="Arial Black"),
        tickfont=dict(size=12),
        showgrid=True,
        gridcolor="rgba(128,128,128,0.2)",  # Grid más sutil
        secondary_y=False
    )
    
    # Configurar eje Y secundario ($/kWh) - SIN CAMBIOS
    fig.update_yaxes(
        title_text="Precio Indexado ($/kWh)",
        title_font=dict(size=14, color="#e74c3c", family="Arial Black"),
        tickfont=dict(size=12),
        tickprefix="$ ",
        showgrid=False,
        secondary_y=True
    )
    
    # Configurar eje X - MEJORADO
    fig.update_xaxes(
        title_text="PERÍODO",
        title_font=dict(size=14, family="Arial Black"),
        tickfont=dict(size=12),
        tickangle=45,
        showgrid=True,
        gridcolor="rgba(128,128,128,0.2)"  # Grid más sutil
    )
    
    # 🆕 CAMBIO PRINCIPAL: Layout con barmode='stack' para barras APILADAS
    fig.update_layout(
        title={
            'text': f"CONSOLIDADO: DEMANDA ASIGNADA Y PRECIOS - TODAS LAS OFERTAS<br><sub>📊 Comparación apilada de {len(ofertas_ordenadas)} ofertas por período</sub>",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 18, 'color': '#1f4e79', 'family': 'Arial Black'}
        },
        width=1400,
        height=700,
        barmode='stack',  # 🎯 CAMBIO CLAVE: De 'group' a 'stack'
        plot_bgcolor='white',
        paper_bgcolor='#fafafa',  # Fondo ligeramente gris
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02,
            font=dict(size=11, family="Arial"),
            bordercolor="#cccccc",
            borderwidth=1,
            bgcolor="rgba(255,255,255,0.95)",
            # 🆕 Mejorar espaciado en leyenda
            itemsizing="constant",
            itemwidth=30
        ),
        margin=dict(l=80, r=220, t=120, b=100),  # Más margen derecho para leyenda
        showlegend=True,
        # 🆕 Añadir configuraciones adicionales para mejor apariencia
        hovermode='x unified',  # Hover unificado por período
    )
    
    # 🆕 MEJORAR: Anotación explicativa más clara
    fig.add_annotation(
        x=0.02, y=-0.18,
        xref="paper", yref="paper",
        text="<b>📊 Interpretación MEJORADA:</b><br>" +
             "• <b>Barras apiladas</b> = Energía total asignada por período (suma de todas las ofertas)<br>" +
             "• <b>Colores en barras</b> = Contribución de cada oferta al total<br>" +
             "• <b>Líneas punteadas</b> = Evolución de precios indexados por oferta<br>" +
             "• <b>💡 Ventaja:</b> Vista menos saturada, fácil comparación de totales y contribuciones individuales",
        showarrow=False,
        font=dict(size=10, color="#2c3e50", family="Arial"),
        bgcolor="rgba(240,248,255,0.95)",
        bordercolor="#3498db",
        borderwidth=1,
        align="left"
    )
    
    # 🆕 Agregar información adicional en el hover para barras
    fig.update_traces(
        hovertemplate="<b>%{fullData.name}</b><br>" +
                     "Período: %{x}<br>" +
                     "Energía: %{y:.2f} GWh<br>" +
                     "<extra></extra>",
        selector=dict(type="bar")
    )
    
    logger.info(f"Gráfica consolidada con barras APILADAS creada exitosamente para {len(ofertas_ordenadas)} ofertas")
    return fig

def crear_graficas_por_oferta_completo(resultados_dict, output_dir):
    """
    Crea tanto las gráficas individuales como la gráfica consolidada.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        output_dir (Path): Directorio donde guardar las gráficas
        
    Returns:
        dict: Diccionario con información de las gráficas creadas
    """
    logger.info("Creando gráficas completas por oferta (individuales + consolidada)")
    print("🔹 Creando gráficas completas por oferta...")
    
    resultado = {
        'individuales': {},
        'consolidada': None,
        'reporte_individuales': None
    }
    
    # 1. Crear gráficas individuales (función existente)
    print("  📋 Generando gráficas individuales...")
    graficas_individuales = crear_graficas_por_oferta(resultados_dict, output_dir)
    resultado['individuales'] = graficas_individuales
    
    # 2. Crear gráfica consolidada (NUEVA)
    print("  📊 Generando gráfica consolidada...")
    try:
        fig_consolidada = crear_grafica_consolidada_ofertas(resultados_dict)
        
        if fig_consolidada:
            archivo_consolidada = output_dir / "06_consolidado_ofertas.html"
            pyo.plot(fig_consolidada, filename=str(archivo_consolidada), auto_open=False)
            resultado['consolidada'] = archivo_consolidada
            print(f"    ✅ Gráfica consolidada guardada: {archivo_consolidada.name}")
        else:
            print("    ⚠️ No se pudo crear la gráfica consolidada")
            
    except Exception as e:
        logger.error(f"Error al crear gráfica consolidada: {e}")
        print(f"    ❌ Error en gráfica consolidada: {e}")
    
    # 3. Crear reporte de individuales (función existente)
    if graficas_individuales:
        print("  📝 Generando reporte de gráficas individuales...")
        try:
            reporte_individuales = generar_reporte_consolidado_ofertas(graficas_individuales, output_dir)
            resultado['reporte_individuales'] = reporte_individuales
            if reporte_individuales:
                print(f"    ✅ Reporte individuales: {reporte_individuales.name}")
        except Exception as e:
            logger.error(f"Error al crear reporte individuales: {e}")
            print(f"    ❌ Error en reporte individuales: {e}")
    
    # Resumen
    total_creadas = len(graficas_individuales) + (1 if resultado['consolidada'] else 0)
    print(f"  🎯 Total gráficas creadas: {total_creadas}")
    print(f"    - Individuales: {len(graficas_individuales)}")
    print(f"    - Consolidada: {'✅' if resultado['consolidada'] else '❌'}")
    
    return resultado