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


def crear_graficas_por_oferta(resultados_dict, output_dir):
    """
    Crea gráficas individuales SOLO para ofertas participantes.
    REVERTIDO: No genera gráficas para ofertas no participantes.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        output_dir (Path): Directorio donde guardar las gráficas
        
    Returns:
        dict: Diccionario con {oferta: ruta_archivo_html} para las gráficas creadas exitosamente
    """
    logger.info("Creando gráficas individuales por oferta (solo participantes)")
    print("🔹 Creando gráficas individuales por oferta...")
    
    # Asegurar que el directorio existe
    output_dir = ensure_directory_exists(output_dir)
    
    # Extraer ofertas del resumen ejecutivo
    if "RESUMEN EJECUTIVO" not in resultados_dict:
        logger.warning("No se encontró RESUMEN EJECUTIVO, no se pueden crear gráficas por oferta")
        print("  ⚠️ No se encontró RESUMEN EJECUTIVO")
        return {}
    
    resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
    
    # Identificar ofertas con datos (participantes)
    ofertas_encontradas = set()
    for col in resumen_df.columns:
        if "CANTIDAD (KWh)" in col:
            # Extraer nombre de la oferta
            nombre_oferta = col.replace(" CANTIDAD (KWh)", "")
            ofertas_encontradas.add(nombre_oferta)
    
    print(f"  📋 Ofertas participantes encontradas: {len(ofertas_encontradas)}")
    for oferta in sorted(ofertas_encontradas):
        print(f"    - {oferta}")
    
    graficas_creadas = {}
    
    # Crear gráfica solo para ofertas participantes
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

def crear_datos_basicos_oferta(nombre_oferta, resumen_df):
    """
    Crea datos básicos para ofertas que no participaron en PyOMO.
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
    ESCALABLE: Funciona con 5 o 50+ ofertas.
    
    Args:
        graficas_creadas (dict): Gráficas generadas exitosamente
        output_dir (Path): Directorio de salida
        resultados_dict (dict): Todos los resultados para detectar ofertas no participantes
        
    Returns:
        Path: Ruta al archivo de reporte consolidado
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
                    # Extraer nombre: "DEMANDA ASIGNADA OFERTA-001 IT1_COMPRAR"
                    parte_oferta = clave.split("DEMANDA ASIGNADA ")[1].split(" IT")[0]
                    todas_las_ofertas.add(parte_oferta)
                except:
                    continue
    
    # Si no se encontraron ofertas del sistema, usar solo las con gráficas
    if not todas_las_ofertas:
        todas_las_ofertas = ofertas_con_graficas.copy()
    
    # Separar ofertas
    ofertas_participantes = ofertas_con_graficas
    ofertas_no_participantes = todas_las_ofertas - ofertas_con_graficas
    
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
                opacity: 0.7;
                border-left: 4px solid #e74c3c;
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
            .no-data-message {{
                text-align: center;
                padding: 40px;
                color: #95a5a6;
                font-style: italic;
                background: white;
                border-radius: 10px;
                margin: 20px 0;
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
            <h2>Sistema de Optimización Energética PyOMO</h2>
            <p>Análisis completo de participación y adjudicación de ofertas</p>
        </div>
        
        <div class="stats-container">
            <div class="stat-box">
                <div class="stat-number total">{total_ofertas}</div>
                <div class="stat-label">Total en Sistema</div>
            </div>
            <div class="stat-box">
                <div class="stat-number participantes">{num_participantes}</div>
                <div class="stat-label">Participaron en PyOMO</div>
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
                Estas ofertas participaron en el proceso de optimización PyOMO y tienen análisis 
                detallado de demanda asignada, no asignada y evolución de precios. 
                <strong>Tasa de participación: {porcentaje_participacion:.1f}%</strong>
            </p>
        </div>
        
        <div class="ofertas-grid">
        """
        
        for nombre_oferta in sorted(ofertas_participantes):
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
    
    # SECCIÓN: Ofertas no participantes
    if ofertas_no_participantes:
        html_content += f"""
        <div class="section-header no-participantes">
            <h2 class="section-title">❌ Ofertas No Participantes ({num_no_participantes})</h2>
            <p class="section-description">
                Estas ofertas estaban disponibles en el sistema pero no participaron en el 
                proceso de optimización PyOMO durante el período analizado.
            </p>
        </div>
        
        <div class="ofertas-grid">
        """
        
        for nombre_oferta in sorted(ofertas_no_participantes):
            html_content += f"""
                <div class="oferta-card no-participante">
                    <div class="oferta-name">{nombre_oferta}</div>
                    <div class="oferta-description">
                        Oferta presente en el sistema pero sin participación en la optimización
                    </div>
                    <span style="color: #e74c3c; font-size: 0.9em; font-weight: 500;">
                        ⚠️ Sin análisis disponible
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
            <p>🎉 ¡Excelente! Todas las ofertas del sistema participaron en la optimización PyOMO.</p>
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
    Dashboard SIMPLIFICADO: Solo 2 gráficas con filtros funcionales.
    ✅ Energía (arriba) + Precios (abajo)
    ✅ Filtros por año que funcionan
    ✅ Colores contrastantes
    ✅ Legibilidad mejorada
    """
    logger.info("Creando dashboard SIMPLIFICADO con filtros funcionales")
    
    if "RESUMEN EJECUTIVO" not in resultados_dict:
        logger.warning("No se encontró RESUMEN EJECUTIVO")
        return None
    
    resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
    
    if resumen_df.empty:
        logger.warning("RESUMEN EJECUTIVO está vacío")
        return None
    
    # 1. EXTRAER DATOS Y SEPARAR OFERTAS
    ofertas_adjudicadas = []
    datos_energia = {}
    datos_precios = {}
    
    for col in resumen_df.columns:
        if "CANTIDAD (KWh)" in col:
            oferta = col.replace(" CANTIDAD (KWh)", "")
            total_energia = resumen_df[col].sum()
            
            # Solo procesar ofertas con energía > 0
            if total_energia > 0:
                ofertas_adjudicadas.append((oferta, total_energia))
                
                # Datos de energía
                datos_energia[oferta] = [convert_to_gwh(val) if pd.notna(val) else 0 
                                       for val in resumen_df[col]]
                
                # Datos de precios
                col_precio = f"{oferta} PRECIO INDEXADO ($/KWh)"
                if col_precio in resumen_df.columns:
                    datos_precios[oferta] = [val if pd.notna(val) and val > 0 else None 
                                           for val in resumen_df[col_precio]]
    
    # Ordenar por energía total (descendente)
    ofertas_adjudicadas.sort(key=lambda x: x[1], reverse=True)
    ofertas_adjudicadas = [x[0] for x in ofertas_adjudicadas]  # Solo nombres
    
    fechas = resumen_df['FECHA'].tolist()
    
    # 2. COLORES CONTRASTANTES (máximo 10 ofertas para claridad)
    import plotly.express as px
    colores_distinctivos = [
        '#1f77b4',  # Azul
        '#ff7f0e',  # Naranja
        '#2ca02c',  # Verde
        '#d62728',  # Rojo
        '#9467bd',  # Morado
        '#8c564b',  # Marrón
        '#e377c2',  # Rosa
        '#7f7f7f',  # Gris
        '#bcbd22',  # Oliva
        '#17becf'   # Cian
    ]
    
    # Limitar a top 10 ofertas para claridad
    ofertas_top = ofertas_adjudicadas[:10]
    
    # 3. CREAR FIGURA CON 2 SUBGRÁFICAS
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=[
            "⚡ ENERGÍA ASIGNADA POR PERÍODO (GWh)",
            "💰 PRECIOS INDEXADOS POR PERÍODO ($/kWh)"
        ],
        row_heights=[0.6, 0.4],
        vertical_spacing=0.15
    )
    
    # 4. GRÁFICA 1: ENERGÍA (barras apiladas)
    for i, oferta in enumerate(ofertas_top):
        color = colores_distinctivos[i % len(colores_distinctivos)]
        
        fig.add_trace(
            go.Bar(
                x=fechas,
                y=datos_energia[oferta],
                name=oferta,
                marker_color=color,
                opacity=0.85,
                
                # Texto mejorado
                text=[f"{val:.1f}" if val > 1 else "" for val in datos_energia[oferta]],
                textposition="inside",
                textfont=dict(color="white", size=10, family="Arial Black"),
                
                # Bordes para definición
                marker=dict(line=dict(color="white", width=1)),
                
                showlegend=True,
                legendgroup="energia",
                
                # Hover detallado
                hovertemplate=(
                    f"<b>{oferta}</b><br>"
                    "📅 %{x}<br>"
                    "⚡ %{y:.2f} GWh<br>"
                    "<extra></extra>"
                ),
                
                # ID para filtros
                visible=True,
                meta=dict(tipo="energia", oferta=oferta)
            ),
            row=1, col=1
        )
    
    # 5. GRÁFICA 2: PRECIOS (líneas claras)
    for i, oferta in enumerate(ofertas_top):
        if oferta in datos_precios:
            color = colores_distinctivos[i % len(colores_distinctivos)]
            
            fig.add_trace(
                go.Scatter(
                    x=fechas,
                    y=datos_precios[oferta],
                    mode="lines+markers",
                    name=oferta,
                    line=dict(color=color, width=4),
                    marker=dict(
                        size=8, 
                        color=color,
                        line=dict(color="white", width=2)
                    ),
                    showlegend=False,  # No duplicar leyenda
                    connectgaps=False,
                    
                    # Hover específico
                    hovertemplate=(
                        f"<b>{oferta}</b><br>"
                        "📅 %{x}<br>"
                        "💵 $%{y:.3f}/kWh<br>"
                        "<extra></extra>"
                    ),
                    
                    # ID para filtros
                    visible=True,
                    meta=dict(tipo="precio", oferta=oferta)
                ),
                row=2, col=1
            )
    
    # 6. CONFIGURAR EJES
    fig.update_yaxes(
        title_text="GWh",
        title_font=dict(size=14, color="#1f4e79"),
        showgrid=True,
        gridcolor="rgba(128,128,128,0.2)",
        row=1, col=1
    )
    
    fig.update_yaxes(
        title_text="$/kWh",
        title_font=dict(size=14, color="#e74c3c"),
        tickprefix="$ ",
        showgrid=True,
        gridcolor="rgba(128,128,128,0.2)",
        row=2, col=1
    )
    
    fig.update_xaxes(
        title_text="PERÍODO",
        title_font=dict(size=14),
        tickangle=45,
        tickfont=dict(size=11),
        row=2, col=1
    )
    
    # 7. FILTROS FUNCIONALES POR AÑO (CORREGIDOS)
    años_disponibles = sorted(list(set([
        fecha.split('/')[1] if '/' in fecha else fecha[-4:] 
        for fecha in fechas
    ])))
    
    # Crear botones de filtro que funcionan
    botones_filtro = []
    
    # Botón "Todos" - mostrar todas las trazas
    botones_filtro.append(
        dict(
            label="📅 TODOS LOS AÑOS",
            method="restyle",
            args=[{"visible": True}, list(range(len(ofertas_top) * 2))]  # Todas las trazas
        )
    )
    
    # Botones por año con filtrado correcto
    for año in años_disponibles:
        # Para cada año, crear máscara de visibilidad
        visible_energia = []
        visible_precios = []
        
        for i, oferta in enumerate(ofertas_top):
            # Verificar si esta oferta tiene datos para el año
            fechas_oferta = [f for f in fechas if año in f]
            tiene_datos = len(fechas_oferta) > 0
            
            visible_energia.append(tiene_datos)
            visible_precios.append(tiene_datos)
        
        # Combinar visibilidad para energía y precios
        visible_total = visible_energia + visible_precios
        
        botones_filtro.append(
            dict(
                label=f"📆 {año}",
                method="restyle",
                args=[{"visible": visible_total}, list(range(len(ofertas_top) * 2))]
            )
        )
    
    # 8. LAYOUT OPTIMIZADO CON LEYENDA A LA DERECHA
    fig.update_layout(
        title={
            'text': f"DASHBOARD CONSOLIDADO - OFERTAS ENERGÉTICAS<br><sub>📊 {len(ofertas_top)} ofertas principales | Vista simplificada con filtros</sub>",
            'x': 0.4,  # Centrado en el área de gráficas (no incluyendo leyenda)
            'xanchor': 'center',
            'font': {'size': 18, 'color': '#1f4e79', 'family': 'Arial Black'}
        },
        
        # Dimensiones ajustadas para leyenda derecha
        width=1600,  # Más ancho para leyenda
        height=900,
        
        # Colores
        plot_bgcolor='white',
        paper_bgcolor='#fafafa',
        
        # Barras apiladas
        barmode='stack',
        
        # LEYENDA MOVIDA A LA DERECHA (en el espacio en blanco)
        legend=dict(
            orientation="v",  # Vertical
            yanchor="top",
            y=0.95,
            xanchor="left", 
            x=1.02,  # A la derecha de las gráficas
            font=dict(size=11, family="Arial"),
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor="#1f4e79",
            borderwidth=2,
            title=dict(
                text="<b>📊 OFERTAS PRINCIPALES</b>",
                font=dict(size=13, color="#1f4e79", family="Arial Black")
            ),
            itemsizing="constant",
            itemwidth=30
        ),
        
        # FILTROS FUNCIONALES
        updatemenus=[
            dict(
                type="dropdown",
                direction="down",
                x=0.02,
                y=1.02,
                xanchor="left",
                yanchor="bottom",
                buttons=botones_filtro,
                font=dict(size=12, family="Arial Black"),
                bgcolor="rgba(255,255,255,0.95)",
                bordercolor="#1f4e79",
                borderwidth=2,
                active=0  # "Todos" seleccionado por defecto
            )
        ],
        
        # Márgenes ajustados (más espacio a la derecha)
        margin=dict(l=80, r=300, t=120, b=100)  # r=300 para leyenda e instrucciones
    )
    
    # 9. INSTRUCCIONES EN LA PARTE DERECHA (debajo de la leyenda)
    fig.add_annotation(
        x=1.02, y=0.45,  # Posición derecha, centro vertical
        xref="paper", yref="paper",
        text=(
            "<b>💡 INSTRUCCIONES:</b><br><br>"
            "🔽 <b>Filtrar por año:</b><br>"
            "Usar dropdown arriba a la izquierda<br><br>"
            "🖱️ <b>Ver detalles:</b><br>"
            "Hover sobre barras/líneas<br><br>"
            "📊 <b>Nota:</b><br>"
            "Solo se muestran las 10<br>"
            "ofertas principales<br><br>"
            "⚡ <b>Energía:</b> Barras apiladas<br>"
            "💰 <b>Precios:</b> Líneas con marcadores"
        ),
        showarrow=False,
        font=dict(size=11, color="#2c3e50", family="Arial"),
        bgcolor="rgba(240,248,255,0.95)",
        bordercolor="#3498db",
        borderwidth=2,
        align="left",
        xanchor="left",
        yanchor="middle",
        width=280  # Ancho fijo para las instrucciones
    )
    
    logger.info(f"Dashboard SIMPLIFICADO creado: {len(ofertas_top)} ofertas con filtros funcionales")
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