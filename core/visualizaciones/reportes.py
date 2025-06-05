"""
Funciones para la generación de informes y reportes con las visualizaciones.
VERSIÓN SIMPLIFICADA: Solo la gráfica principal por ahora.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf_backend
import matplotlib.ticker as ticker
import plotly.io as pio
from pathlib import Path
import logging
from datetime import datetime
import os

from .utils import (
    format_number, convert_to_gwh, ensure_directory_exists,
    extract_dates_from_results, extract_years_months_from_dates
)
from .basicas import calcular_totales_energia

# Configurar logging
logger = logging.getLogger(__name__)

def generar_estadisticas_anuales(resultados_dict, ofertas_df=None):
    """
    FUNCIÓN AUXILIAR: Genera estadísticas agregadas por año.
    
    Analiza el resumen ejecutivo y calcula para cada año:
    - Energía total asignada
    - Precios mínimo, máximo y promedio ponderado
    - Ofertas utilizadas
    - Demanda no asignada
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        ofertas_df (DataFrame, opcional): DataFrame con información de ofertas originales
        
    Returns:
        dict: Diccionario con estadísticas anuales
    """
    logger.info("Generando estadísticas anuales")
    
    estadisticas_anuales = {}
    
    # Obtener el resumen ejecutivo si existe
    if "RESUMEN EJECUTIVO" not in resultados_dict:
        logger.warning("No se encontró resumen ejecutivo en los resultados")
        return estadisticas_anuales
    
    resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
    
    # Extraer años de las fechas (formato MM/YYYY)
    años = []
    for fecha in resumen_df['FECHA']:
        if isinstance(fecha, str) and '/' in fecha:
            año = fecha.split('/')[1]
            if año not in años:
                años.append(año)
    
    # Si no se extrajeron años, retornar vacío
    if not años:
        logger.warning("No se pudieron extraer años de las fechas")
        return estadisticas_anuales
    
    # Para cada año, generar estadísticas
    for año in años:
        # Filtrar filas para este año
        filas_año = []
        for idx, row in resumen_df.iterrows():
            if isinstance(row['FECHA'], str) and row['FECHA'].endswith(f'/{año}'):
                filas_año.append(row)
        
        if not filas_año:
            continue
            
        df_año = pd.DataFrame(filas_año)
        
        estadisticas_año = {
            'año': año,
            'energia_total_asignada': 0,
            'precio_ponderado_promedio': 0,
            'precio_minimo': float('inf'),
            'precio_maximo': 0,
            'costo_total': 0,
            'ofertas_utilizadas': [],
            'distribucion_por_oferta': {},
            'demanda_no_asignada_total': 0
        }
        
        # Analizar cada oferta en el año
        for col in df_año.columns:
            if 'CANTIDAD (KWh)' in col:
                oferta = col.split(' CANTIDAD (KWh)')[0]
                cantidad_total = df_año[col].sum()
                
                if cantidad_total > 0:
                    estadisticas_año['ofertas_utilizadas'].append(oferta)
                    estadisticas_año['distribucion_por_oferta'][oferta] = cantidad_total
                    estadisticas_año['energia_total_asignada'] += cantidad_total
                    
                    # Buscar precio correspondiente
                    precio_col = f"{oferta} PRECIO INDEXADO ($/KWh)"
                    if precio_col in df_año.columns:
                        precios = df_año[precio_col][df_año[precio_col] > 0]
                        if not precios.empty:
                            precio_promedio = precios.mean()
                            estadisticas_año['precio_minimo'] = min(estadisticas_año['precio_minimo'], precios.min())
                            estadisticas_año['precio_maximo'] = max(estadisticas_año['precio_maximo'], precios.max())
                            estadisticas_año['costo_total'] += cantidad_total * precio_promedio
        
        # Calcular precio ponderado promedio
        if estadisticas_año['energia_total_asignada'] > 0:
            estadisticas_año['precio_ponderado_promedio'] = (
                estadisticas_año['costo_total'] / estadisticas_año['energia_total_asignada']
            )
        
        # Demanda no asignada
        if 'DEMANDA NO ASIGNADA (KWh)' in df_año.columns:
            estadisticas_año['demanda_no_asignada_total'] = df_año['DEMANDA NO ASIGNADA (KWh)'].sum()
        
        # Si el precio mínimo sigue siendo infinito, establecerlo en 0
        if estadisticas_año['precio_minimo'] == float('inf'):
            estadisticas_año['precio_minimo'] = 0
            
        estadisticas_anuales[año] = estadisticas_año
    
    return estadisticas_anuales

def generar_informe_interactivo(resultados_dict, ofertas_df, directorio_salida):
    """
    FUNCIÓN PRINCIPAL: Genera un informe HTML interactivo con la gráfica solicitada.
    
    Crea un archivo HTML con:
    - La gráfica de barras por hora (exacta como solicita el cliente)
    - Diseño profesional y responsivo
    - Interactividad con Plotly
    
    Args:
        resultados_dict (dict): Resultados de la optimización
        ofertas_df (DataFrame): DataFrame con ofertas originales
        directorio_salida (str): Directorio donde guardar el informe
        
    Returns:
        str: Ruta al archivo HTML generado
    """
    print("🚀 Generando informe HTML interactivo...")
    
    # Importar la función principal
    from .basicas import crear_grafica_barras_por_hora
    
    # Configurar ruta de salida
    directorio = ensure_directory_exists(directorio_salida)
    ruta_html = directorio / "informe_optimizacion_ofertas.html"
    
    # Generar la gráfica principal
    figuras = []
    titulos = []
    
    try:
        print("   📊 Creando gráfica de barras por hora...")
        fig_principal = crear_grafica_barras_por_hora(resultados_dict)
        
        if fig_principal is not None:
            figuras.append(fig_principal)
            titulos.append("Energía Asignada y No Asignada por Hora")
            print("   ✅ Gráfica principal creada exitosamente")
        else:
            print("   ❌ Error: No se pudo crear la gráfica principal")
            
    except Exception as e:
        print(f"   ❌ Error creando gráfica: {e}")
        import traceback
        traceback.print_exc()
    
    # Verificar que tenemos gráficas para mostrar
    if not figuras:
        print("❌ No se pudo generar ninguna gráfica")
        return None

    # Generar archivo HTML
    print("   📄 Generando archivo HTML...")
    
    with open(ruta_html, 'w', encoding='utf-8') as f:
        f.write(f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>📊 Informe de Optimización Energética</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        
        .header {{
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(15px);
            border-radius: 20px;
            padding: 30px;
            text-align: center;
            margin-bottom: 30px;
            border: 1px solid rgba(255, 255, 255, 0.2);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        }}
        
        .header h1 {{
            color: white;
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.3);
        }}
        
        .header p {{
            color: rgba(255, 255, 255, 0.9);
            font-size: 1.2em;
            margin-bottom: 15px;
        }}
        
        .badge {{
            background: rgba(255, 255, 255, 0.2);
            color: white;
            padding: 8px 20px;
            border-radius: 25px;
            font-weight: bold;
            display: inline-block;
            border: 1px solid rgba(255, 255, 255, 0.3);
        }}
        
        .plot-container {{
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            padding: 30px;
            margin: 20px 0;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }}
        
        .section-title {{
            color: #1f4e79;
            font-size: 1.8em;
            margin-bottom: 15px;
            border-bottom: 3px solid #1f4e79;
            padding-bottom: 10px;
        }}
        
        .description {{
            color: #666;
            font-size: 1.1em;
            line-height: 1.6;
            margin-bottom: 25px;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        
        .stat-card {{
            background: rgba(31, 78, 121, 0.1);
            padding: 15px;
            border-radius: 10px;
            text-align: center;
            border: 1px solid rgba(31, 78, 121, 0.2);
        }}
        
        .stat-number {{
            font-size: 1.5em;
            font-weight: bold;
            color: #1f4e79;
            margin-bottom: 5px;
        }}
        
        .stat-label {{
            color: #666;
            font-size: 0.9em;
        }}
        
        .footer {{
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(15px);
            border-radius: 20px;
            padding: 25px;
            text-align: center;
            margin-top: 30px;
            border: 1px solid rgba(255, 255, 255, 0.2);
            color: white;
        }}
        
        .success-message {{
            background: rgba(76, 175, 80, 0.2);
            color: #4CAF50;
            padding: 15px;
            border-radius: 10px;
            margin: 15px 0;
            border: 1px solid rgba(76, 175, 80, 0.3);
            font-weight: bold;
        }}
        
        @media (max-width: 768px) {{
            .header h1 {{
                font-size: 1.8em;
            }}
            
            .plot-container {{
                padding: 20px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- ENCABEZADO -->
        <div class="header">
            <h1>🔋 Informe de Optimización Energética</h1>
            <p>Resultados del modelo de optimización para la selección óptima de ofertas</p>
            <div class="badge">✅ GRÁFICA PRINCIPAL GENERADA</div>
        </div>

        <!-- CONTENIDO PRINCIPAL -->
        <div class="plot-container">
            <h2 class="section-title">📊 {titulos[0]}</h2>
            <div class="description">
                <strong>Esta gráfica replica exactamente lo solicitado por el cliente:</strong><br>
                • <span style="color: #1f4e79;"><strong>Barras azules:</strong></span> GWh asignados por hora (energía comprada)<br>
                • <span style="color: #d9d9d9;"><strong>Barras grises:</strong></span> GWh no asignados por hora (energía faltante)<br>
                • <span style="color: #70ad47;"><strong>Línea verde:</strong></span> Porcentaje de energía no asignada por hora<br>
                <br>
                <em>La gráfica es interactiva: puedes hacer hover sobre los elementos para ver detalles.</em>
            </div>
            
            <!-- Aquí va la gráfica -->
            <div id="plot_principal"></div>
            
            <!-- Estadísticas rápidas -->
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-number" id="total-asignado">0</div>
                    <div class="stat-label">Total GWh Asignados</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number" id="total-no-asignado">0</div>
                    <div class="stat-label">Total GWh No Asignados</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number" id="porcentaje-promedio">0%</div>
                    <div class="stat-label">% Promedio No Asignado</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">24</div>
                    <div class="stat-label">Horas Analizadas</div>
                </div>
            </div>
            
            <div class="success-message">
                ✅ <strong>Gráfica generada exitosamente</strong> - Datos procesados de la optimización energética
            </div>
        </div>

        <!-- PIE DE PÁGINA -->
        <div class="footer">
            <h3>🎉 Informe Generado Correctamente</h3>
            <p><strong>Sistema de Optimización Energética</strong></p>
            <p>📅 Fecha de generación: {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}</p>
            <p>🔄 Próximo paso: Agregar más gráficas paso a paso</p>
        </div>
    </div>

    <!-- JAVASCRIPT PARA LA GRÁFICA -->
    <script>
        // Datos de la gráfica principal
        var plotData = {pio.to_json(figuras[0])};
        
        // Renderizar la gráfica
        Plotly.newPlot('plot_principal', plotData.data, plotData.layout, {{
            responsive: true,
            displayModeBar: true,
            modeBarButtonsToRemove: ['lasso2d', 'select2d']
        }});
        
        // Calcular y mostrar estadísticas
        var totalAsignado = 0;
        var totalNoAsignado = 0;
        
        // Buscar los datos en las trazas de la gráfica
        plotData.data.forEach(function(trace) {{
            if (trace.name === 'GWh Asignados') {{
                totalAsignado = trace.y.reduce((a, b) => a + b, 0);
            }}
            if (trace.name === 'GWh No Asignado') {{
                totalNoAsignado = trace.y.reduce((a, b) => a + b, 0);
            }}
        }});
        
        // Actualizar estadísticas en el HTML
        document.getElementById('total-asignado').textContent = totalAsignado.toFixed(2);
        document.getElementById('total-no-asignado').textContent = totalNoAsignado.toFixed(2);
        
        var porcentajePromedio = totalNoAsignado > 0 ? 
            ((totalNoAsignado / (totalAsignado + totalNoAsignado)) * 100).toFixed(1) : 0;
        document.getElementById('porcentaje-promedio').textContent = porcentajePromedio + '%';
        
        console.log('✅ Gráfica cargada exitosamente');
        console.log('📊 Total asignado:', totalAsignado.toFixed(2), 'GWh');
        console.log('📊 Total no asignado:', totalNoAsignado.toFixed(2), 'GWh');
    </script>
</body>
</html>""")
    
    logger.info(f"Informe interactivo generado: {ruta_html}")
    print(f"✅ Informe HTML generado exitosamente: {ruta_html}")
    print(f"🌐 Para ver la gráfica, abre el archivo en tu navegador web")
    
    return str(ruta_html)

def generar_reporte_completo_mejorado(resultados_dict, ofertas_df, archivo_salida):
    """
    FUNCIÓN PRINCIPAL DEL MÓDULO: Genera el reporte completo.
    
    Por ahora solo genera el HTML interactivo (sin PDF para evitar errores).
    Una vez que funcione bien, agregaremos más gráficas paso a paso.
    
    Args:
        resultados_dict (dict): Resultados de la optimización
        ofertas_df (DataFrame): DataFrame con ofertas originales
        archivo_salida (str): Archivo base para generar el reporte
        
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    logger.info("Generando reporte completo (VERSIÓN SIMPLIFICADA)")
    print("🔧 Generando reporte simplificado...")
    
    try:
        # Solo generar HTML por ahora
        directorio_html = Path(archivo_salida).parent
        archivo_html = generar_informe_interactivo(resultados_dict, ofertas_df, directorio_html)
        
        if archivo_html and Path(archivo_html).exists():
            print(f"✅ Reporte generado exitosamente")
            print(f"📄 Archivo HTML: {archivo_html}")
            print(f"🌐 Abre el archivo en tu navegador para ver la gráfica")
            return True
        else:
            print("❌ Error: No se pudo generar el archivo HTML")
            return False
        
    except Exception as e:
        logger.exception(f"Error al generar reporte: {e}")
        print(f"❌ Error al generar reporte: {e}")
        return False

def crear_resumen_estadisticas_excel(estadisticas_anuales, archivo_salida):
    """
    FUNCIÓN AUXILIAR: Crea un resumen de estadísticas en formato Excel.
    
    Args:
        estadisticas_anuales (dict): Estadísticas calculadas por año
        archivo_salida (str): Archivo base para generar el Excel
    """
    # Crear DataFrame con resumen
    filas_resumen = []
    
    for año, stats in estadisticas_anuales.items():
        filas_resumen.append({
            'AÑO': año,
            'ENERGÍA TOTAL ASIGNADA (GWh)': convert_to_gwh(stats['energia_total_asignada']),
            'PRECIO PONDERADO PROMEDIO ($/KWh)': stats['precio_ponderado_promedio'],
            'PRECIO MÍNIMO ($/KWh)': stats['precio_minimo'] if stats['precio_minimo'] != float('inf') else 0,
            'PRECIO MÁXIMO ($/KWh)': stats['precio_maximo'],
            'COSTO TOTAL ($ millones)': convert_to_gwh(stats['costo_total']),
            'DEMANDA NO ASIGNADA (GWh)': convert_to_gwh(stats['demanda_no_asignada_total']),
            'OFERTAS UTILIZADAS': len(stats['ofertas_utilizadas']),
            'COBERTURA (%)': (stats['energia_total_asignada'] / (stats['energia_total_asignada'] + stats['demanda_no_asignada_total']) * 100) if (stats['energia_total_asignada'] + stats['demanda_no_asignada_total']) > 0 else 0
        })
    
    df_resumen = pd.DataFrame(filas_resumen)
    
    # Guardar en Excel
    archivo_estadisticas = Path(archivo_salida).parent / "estadisticas_anuales.xlsx"
    df_resumen.to_excel(archivo_estadisticas, index=False)
    
    logger.info(f"Estadísticas anuales guardadas en: {archivo_estadisticas}")
    print(f"✅ Estadísticas anuales guardadas en: {archivo_estadisticas}")
    
    return archivo_estadisticas