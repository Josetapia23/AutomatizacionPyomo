"""
Funciones para la generación de informes y reportes con las visualizaciones.
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
from .avanzadas import crear_grafica_distribucion_horaria

# Configurar logging
logger = logging.getLogger(__name__)

def generar_estadisticas_anuales(resultados_dict, ofertas_df=None):
    """
    Genera estadísticas agregadas por año basadas en los resultados de optimización.
    
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

def crear_grafica_resumen_energia_total(fig, resultados_dict):
    """
    Crea una gráfica clara y simple del total de energía asignada vs no asignada.
    
    Args:
        fig: Figure de matplotlib
        resultados_dict (dict): Diccionario con los resultados
    """
    # Calcular totales
    total_asignada, total_no_asignada, _ = calcular_totales_energia(resultados_dict)
    total_energia = total_asignada + total_no_asignada
    
    # Calcular porcentajes
    pct_asignada = (total_asignada / total_energia * 100) if total_energia > 0 else 0
    pct_no_asignada = (total_no_asignada / total_energia * 100) if total_energia > 0 else 0
    
    # 1. GRÁFICA DE BARRAS (lado izquierdo)
    ax1 = plt.subplot(1, 3, 1)
    
    categorias = ['Energía\nAsignada', 'Energía\nNo Asignada', 'Total\nDemanda']
    valores = [total_asignada, total_no_asignada, total_energia]
    colores = ['#2E86AB', '#E63946', '#42B883']
    
    bars = ax1.bar(categorias, valores, color=colores, alpha=0.8, edgecolor='black', linewidth=1.5)
    
    # Añadir valores en las barras
    for bar, valor in zip(bars, valores):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{valor:,.0f} GWh', ha='center', va='bottom', 
                fontweight='bold', fontsize=11)
    
    ax1.set_title('Resumen Total de Energía', fontsize=14, fontweight='bold')
    ax1.set_ylabel('GWh', fontsize=12)
    ax1.grid(axis='y', alpha=0.3, linestyle='--')
    ax1.set_ylim(0, max(valores) * 1.15)
    
    # 2. GRÁFICA DE PASTEL (centro)
    ax2 = plt.subplot(1, 3, 2)
    
    sizes = [total_asignada, total_no_asignada]
    labels = [f'Asignada\n{pct_asignada:.1f}%', f'No Asignada\n{pct_no_asignada:.1f}%']
    colors_pie = ['#2E86AB', '#E63946']
    explode = (0.05, 0.05)
    
    wedges, texts, autotexts = ax2.pie(sizes, labels=labels, colors=colors_pie, 
                                        autopct=lambda pct: f'{pct:.1f}%' if pct > 0 else '',
                                        startangle=90, explode=explode,
                                        shadow=True, textprops={'fontsize': 11})
    
    # Mejorar el texto
    for text in texts:
        text.set_fontweight('bold')
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')
        autotext.set_fontsize(12)
    
    ax2.set_title('Distribución Porcentual', fontsize=14, fontweight='bold')
    
    # 3. TABLA DE RESUMEN (lado derecho)
    ax3 = plt.subplot(1, 3, 3)
    ax3.axis('tight')
    ax3.axis('off')
    
    # Datos para la tabla
    table_data = [
        ['Concepto', 'Valor', 'Porcentaje'],
        ['Energía Asignada', f'{total_asignada:,.0f} GWh', f'{pct_asignada:.1f}%'],
        ['Energía No Asignada', f'{total_no_asignada:,.0f} GWh', f'{pct_no_asignada:.1f}%'],
        ['Total Demanda', f'{total_energia:,.0f} GWh', '100.0%'],
        ['', '', ''],
        ['Eficiencia de Asignación', '', f'{pct_asignada:.1f}%'],
        ['Déficit', '', f'{pct_no_asignada:.1f}%']
    ]
    
    table = ax3.table(cellText=table_data, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 2)
    
    # Estilo de la tabla
    for i in range(len(table_data)):
        for j in range(3):
            cell = table[(i, j)]
            if i == 0:  # Encabezados
                cell.set_facecolor('#34495e')
                cell.set_text_props(weight='bold', color='white')
            elif i == 3:  # Total
                cell.set_facecolor('#ecf0f1')
                cell.set_text_props(weight='bold')
            elif i >= 5:  # Métricas finales
                cell.set_facecolor('#e8f4f8')
                if j == 2:  # Columna de porcentajes
                    cell.set_text_props(weight='bold')
    
    ax3.set_title('Tabla de Resumen', fontsize=14, fontweight='bold', pad=20)

def generar_informe_interactivo(resultados_dict, ofertas_df, directorio_salida):
    """
    Genera un informe interactivo HTML con todas las gráficas solicitadas por el cliente.
    
    Args:
        resultados_dict (dict): Resultados de la optimización
        ofertas_df (DataFrame): DataFrame con ofertas originales
        directorio_salida (str): Directorio donde guardar el informe
        
    Returns:
        str: Ruta al archivo HTML generado
    """
    from .basicas import crear_grafica_resumen_adjudicacion, crear_grafica_distribucion_porcentual
    from .avanzadas import (
        crear_grafica_distribucion_horaria, crear_mapa_calor_mensual,
        crear_grafica_distribucion_por_agente, crear_grafica_exposicion_bolsa
    )
    
    # Ruta del archivo de salida HTML
    directorio = ensure_directory_exists(directorio_salida)
    ruta_html = directorio / "informe_optimizacion_ofertas.html"
    
    # Crear lista para almacenar todas las figuras
    figuras = []
    titulos = []
    
    # 1. Gráfica de resumen de adjudicación
    fig_resumen = crear_grafica_resumen_adjudicacion(resultados_dict)
    if fig_resumen is not None:
        figuras.append(fig_resumen)
        titulos.append("Resumen General de Adjudicación")
    
    # 2. Gráfica de distribución porcentual
    fig_porcentual = crear_grafica_distribucion_porcentual(resultados_dict)
    if fig_porcentual is not None:
        figuras.append(fig_porcentual)
        titulos.append("Distribución Porcentual de Energía")
    
    # 3. Gráfica de distribución horaria
    fig_horaria = crear_grafica_distribucion_horaria(resultados_dict)
    if fig_horaria is not None:
        figuras.append(fig_horaria)
        titulos.append("Distribución Horaria de Energía")
    
    # 4. Mapa de calor mensual
    fig_mapa = crear_mapa_calor_mensual(resultados_dict)
    if fig_mapa is not None:
        figuras.append(fig_mapa)
        titulos.append("Mapa de Calor Mensual")
    
    # 5. Gráfica de distribución por agente (DA/ENA)
    fig_agentes = crear_grafica_distribucion_por_agente(resultados_dict)
    if fig_agentes is not None:
        figuras.append(fig_agentes)
        titulos.append("Distribución por Agente")
    
    # 6. Gráfica de exposición óptima en bolsa
    fig_bolsa = crear_grafica_exposicion_bolsa(resultados_dict)
    if fig_bolsa is not None:
        figuras.append(fig_bolsa)
        titulos.append("Exposición Óptima en Bolsa")
    
    # Guardar todas las figuras en un solo archivo HTML
    with open(ruta_html, 'w', encoding='utf-8') as f:
        f.write("""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Informe de Optimización Energética</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f9f9f9;
        }
        .header {
            background-color: #14213D;
            color: white;
            padding: 20px;
            text-align: center;
            margin-bottom: 30px;
            border-radius: 5px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .plot-container {
            background-color: white;
            margin: 30px 0;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .section-title {
            color: #14213D;
            border-bottom: 2px solid #14213D;
            padding-bottom: 10px;
            margin-top: 0;
        }
        .description {
            color: #666;
            font-size: 14px;
            margin-top: 5px;
            margin-bottom: 20px;
        }
        .footer {
            text-align: center;
            margin-top: 30px;
            padding: 20px;
            color: #666;
            font-size: 14px;
        }
        /* Estilos para explicación de siglas */
        .siglas {
            background-color: #f0f8ff;
            padding: 15px;
            border-left: 4px solid #14213D;
            margin: 20px 0;
            border-radius: 0 5px 5px 0;
        }
        .siglas h3 {
            margin-top: 0;
            color: #14213D;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>Informe de Optimización de Ofertas Energéticas</h1>
        <p>Resultados del modelo de optimización para la selección óptima de ofertas</p>
    </div>
    
    <div class="container">
        <!-- Explicación de siglas -->
        <div class="siglas">
            <h3>Definición de Términos</h3>
            <p><strong>DA:</strong> Demanda Asignada - Energía a comprar al vendedor según el modelo de optimización.</p>
            <p><strong>ENA:</strong> Energía No Asignada - Energía disponible que no fue seleccionada para compra.</p>
            <p><strong>GWh:</strong> Gigavatios-hora - Unidad de medida de energía.</p>
        </div>
""")
        
        # Añadir cada gráfica en su propia sección
        for i, (fig, titulo) in enumerate(zip(figuras, titulos)):
            div_id = f"plot_{i}"
            
            # Añadir título de sección y contenedor
            f.write(f"""
        <div class="plot-container">
            <h2 class="section-title">{titulo}</h2>
""")
            
            # Añadir descripción específica según el tipo de gráfica
            if "Resumen General" in titulo:
                f.write("""
            <p class="description">Esta gráfica muestra un resumen de la energía adjudicada, el precio ponderado de adjudicación, y los precios máximo y mínimo adjudicados.</p>
""")
            elif "Distribución Porcentual" in titulo:
                f.write("""
            <p class="description">Muestra la distribución porcentual de la energía asignada versus la no asignada respecto al total ofertado.</p>
""")
            elif "Distribución Horaria" in titulo:
                f.write("""
            <p class="description">Presenta la distribución de energía asignada y no asignada por hora del día, junto con el porcentaje de no asignación.</p>
""")
            elif "Mapa de Calor" in titulo:
                f.write("""
            <p class="description">Visualización en formato de mapa de calor de los valores mensuales a lo largo de los años del horizonte de optimización.</p>
""")
            elif "Distribución por Agente" in titulo:
                f.write("""
            <p class="description">Muestra la distribución de GWh por tipo de asignación (DA/ENA) para cada agente a lo largo del tiempo.</p>
""")
            elif "Exposición Óptima" in titulo:
                f.write("""
            <p class="description">Especifica qué parte de la demanda resulta más conveniente cubrir mediante compras en bolsa, según el modelo de optimización.</p>
""")
            
            # Añadir el div para la gráfica y el script
            f.write(f"""
            <div id="{div_id}"></div>
        </div>
""")
            
            # Convertir la figura a JavaScript y añadirla
            plot_js = pio.to_json(fig)
            f.write(f"""
    <script>
        var plotData{i} = {plot_js};
        Plotly.newPlot('{div_id}', plotData{i}.data, plotData{i}.layout);
    </script>
""")
        
        # Cerrar el HTML
        f.write("""
        <div class="footer">
            <p>Informe generado automáticamente por el Sistema de Optimización Energética</p>
            <p>Fecha de generación: """ + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + """</p>
        </div>
    </div>
</body>
</html>
""")
    
    logger.info(f"Informe interactivo generado: {ruta_html}")
    print(f"✅ Informe interactivo generado: {ruta_html}")
    
    return str(ruta_html)

def generar_reporte_completo_mejorado(resultados_dict, ofertas_df, archivo_salida):
    """
    Genera el reporte completo en PDF con todas las gráficas solicitadas por el cliente.
    Versión mejorada que incorpora las nuevas visualizaciones.
    
    Args:
        resultados_dict (dict): Resultados de la optimización
        ofertas_df (DataFrame): DataFrame con ofertas originales
        archivo_salida (str): Archivo base para generar el reporte
        
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    logger.info("Generando reporte PDF completo mejorado")
    
    try:
        # 1. Generar estadísticas anuales
        estadisticas = generar_estadisticas_anuales(resultados_dict, ofertas_df)
        
        if not estadisticas:
            logger.error("No se pudieron generar estadísticas")
            return False
        
        # 2. Crear PDF
        archivo_pdf = Path(archivo_salida).parent / "informe_optimizacion_ofertas.pdf"
        
        with pdf_backend.PdfPages(archivo_pdf) as pdf:
            # Primera página: Resumen general de todo el ejercicio
            fig = plt.figure(figsize=(16, 10))
            fig.suptitle('ANÁLISIS DE ENERGÍA ASIGNADA Y NO ASIGNADA - EJERCICIO COMPLETO', 
                        fontsize=16, fontweight='bold', y=0.98)
            
            # Crear la gráfica de resumen total
            crear_grafica_resumen_energia_total(fig, resultados_dict)
            
            # Ajustar espaciado
            plt.tight_layout(rect=[0, 0.03, 1, 0.95])
            
            # Guardar página en PDF
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            # Generar HTML con Plotly
            directorio_html = Path(archivo_salida).parent
            generar_informe_interactivo(resultados_dict, ofertas_df, directorio_html)
            
            # Añadir páginas con gráficas de Plotly convertidas a matplotlib
            # Esto es para incluir las nuevas gráficas en el PDF
            
            # Distribución horaria
            try:
                plt.figure(figsize=(11, 8.5))
                plt.suptitle('Distribución Horaria de Energía', fontsize=16, fontweight='bold')
                
                # Extraer datos por hora
                horas = list(range(1, 25))
                gwh_asignados = [0] * 24
                gwh_no_asignados = [0] * 24
                
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
                        gwh_asignados[hora] = energia_asignada_por_hora[hora] / 1000000  # kWh a GWh
                        gwh_no_asignados[hora] = demanda_faltante_por_hora[hora] / 1000000  # kWh a GWh
                
                # Crear gráfica
                ax1 = plt.subplot(111)
                
                # Crear barras apiladas
                ax1.bar(horas, gwh_asignados, color='#14213D', label='GWh Asignados')
                ax1.bar(horas, gwh_no_asignados, bottom=gwh_asignados, color='#E5E5E5', label='GWh No Asignados')
                
                # Crear segundo eje para porcentaje de no asignación
                ax2 = ax1.twinx()
                
                # Calcular porcentajes de no asignación
                porcentaje_no_asignado = []
                for i in range(24):
                    total = gwh_asignados[i] + gwh_no_asignados[i]
                    if total > 0:
                        porcentaje_no_asignado.append((gwh_no_asignados[i] / total) * 100)
                    else:
                        porcentaje_no_asignado.append(0)
                
                # Añadir línea para porcentaje
                ax2.plot(horas, porcentaje_no_asignado, color='#48CAE4', marker='o', linestyle='-', linewidth=2, label='% No Asignado')
                
                # Configurar ejes
                ax1.set_xlabel('Horas', fontsize=12, fontweight='bold')
                ax1.set_ylabel('GWh', fontsize=12, fontweight='bold')
                ax2.set_ylabel('% No Asignado', fontsize=12, fontweight='bold')
                
                # Añadir leyendas
                ax1.legend(loc='upper left')
                ax2.legend(loc='upper right')
                
                # Ajustar
                plt.tight_layout(rect=[0, 0.03, 1, 0.95])
                
                # Guardar página en PDF
                pdf.savefig(bbox_inches='tight')
                plt.close()
            except Exception as e:
                logger.warning(f"Error al crear gráfica de distribución horaria: {e}")
            
            # Mapa de calor mensual
            try:
                plt.figure(figsize=(11, 8.5))
                plt.suptitle('Mapa de Calor Mensual', fontsize=16, fontweight='bold')
                
                # Extraer datos del resumen ejecutivo si existe
                if "RESUMEN EJECUTIVO" in resultados_dict:
                    df_resumen = resultados_dict["RESUMEN EJECUTIVO"]
                    
                    # Simplificar para matplotlib: mostrar un heatmap de los precios promedios por mes
                    meses = []
                    años = []
                    valores = []
                    
                    for _, row in df_resumen.iterrows():
                        if "FECHA" in row and isinstance(row["FECHA"], str) and "/" in row["FECHA"]:
                            partes = row["FECHA"].split("/")
                            if len(partes) == 2:
                                mes = int(partes[0])
                                año = partes[1]
                                
                                # Buscar precio ponderado
                                for col in row.index:
                                    if "PRECIO INDEXADO" in col:
                                        valor = row[col]
                                        meses.append(mes)
                                        años.append(año)
                                        valores.append(valor)
                                        break
                    
                    # Crear DataFrame para el heatmap
                    import pandas as pd
                    df_heatmap = pd.DataFrame({
                        'Mes': meses,
                        'Año': años,
                        'Valor': valores
                    })
                    
                    if not df_heatmap.empty:
                        # Pivotar para crear matriz
                        pivot = df_heatmap.pivot_table(index='Mes', columns='Año', values='Valor')
                        
                        # Crear heatmap
                        ax = plt.subplot(111)
                        im = ax.imshow(pivot.values, cmap='viridis')
                        
                        # Configurar ejes
                        ax.set_xticks(range(len(pivot.columns)))
                        ax.set_yticks(range(len(pivot.index)))
                        ax.set_xticklabels(pivot.columns)
                        ax.set_yticklabels(pivot.index)
                        
                        # Añadir valores en celdas
                        for i in range(len(pivot.index)):
                            for j in range(len(pivot.columns)):
                                value = pivot.values[i, j]
                                if not pd.isna(value):
                                    ax.text(j, i, f"{value:.2f}", ha="center", va="center", color="w")
                        
                        # Añadir colorbar
                        plt.colorbar(im, ax=ax, label='Precio Indexado ($/KWh)')
                        
                        # Títulos
                        ax.set_xlabel('Año', fontsize=12, fontweight='bold')
                        ax.set_ylabel('Mes', fontsize=12, fontweight='bold')
                    else:
                        plt.text(0.5, 0.5, "No hay suficientes datos para generar el mapa de calor", 
                                ha='center', va='center', transform=plt.gca().transAxes, fontsize=14)
                else:
                    plt.text(0.5, 0.5, "No se encontró la hoja de resumen ejecutivo", 
                            ha='center', va='center', transform=plt.gca().transAxes, fontsize=14)
                
                plt.tight_layout(rect=[0, 0.03, 1, 0.95])
                pdf.savefig(bbox_inches='tight')
                plt.close()
            except Exception as e:
                logger.warning(f"Error al crear mapa de calor mensual: {e}")
            
            # Páginas adicionales por año (resumen anual)
            for año, stats in estadisticas.items():
                try:
                    fig = plt.figure(figsize=(11, 8.5))
                    fig.suptitle(f'Análisis Anual - {año}', fontsize=16, fontweight='bold')
                    
                    # Crear tabla con estadísticas del año
                    ax = plt.subplot(111)
                    ax.axis('off')
                    
                    # Datos para la tabla
                    table_data = [
                        ['Métrica', 'Valor'],
                        ['Energía Total Asignada', f"{convert_to_gwh(stats['energia_total_asignada']):.2f} GWh"],
                        ['Precio Ponderado Promedio', f"${stats['precio_ponderado_promedio']:.2f}/KWh"],
                        ['Precio Mínimo', f"${stats['precio_minimo']:.2f}/KWh"],
                        ['Precio Máximo', f"${stats['precio_maximo']:.2f}/KWh"],
                        ['Costo Total', f"${convert_to_gwh(stats['costo_total']):.2f} millones"],
                        ['Demanda No Asignada', f"{convert_to_gwh(stats['demanda_no_asignada_total']):.2f} GWh"],
                        ['Ofertas Utilizadas', f"{len(stats['ofertas_utilizadas'])}"]
                    ]
                    
                    # Calcular eficiencia
                    total_demanda = stats['energia_total_asignada'] + stats['demanda_no_asignada_total']
                    if total_demanda > 0:
                        eficiencia = (stats['energia_total_asignada'] / total_demanda) * 100
                        table_data.append(['Eficiencia de Asignación', f"{eficiencia:.2f}%"])
                    
                    # Crear tabla
                    table = ax.table(cellText=table_data, loc='center', cellLoc='left')
                    table.auto_set_font_size(False)
                    table.set_fontsize(12)
                    table.scale(1.2, 2)
                    
                    # Estilo de la tabla
                    for i in range(len(table_data)):
                        for j in range(2):
                            cell = table[(i, j)]
                            if i == 0:  # Encabezados
                                cell.set_facecolor('#34495e')
                                cell.set_text_props(weight='bold', color='white')
                            elif i % 2 == 1:  # Filas impares
                                cell.set_facecolor('#ecf0f1')
                    
                    # Guardar página en PDF
                    pdf.savefig(fig, bbox_inches='tight')
                    plt.close()
                except Exception as e:
                    logger.warning(f"Error al crear página para año {año}: {e}")
            
            # Añadir metadatos al PDF
            d = pdf.infodict()
            d['Title'] = 'Informe de Optimización de Ofertas Energéticas'
            d['Author'] = 'Sistema de Optimización GECELCA'
            d['Subject'] = 'Análisis de asignación óptima de ofertas'
            d['Keywords'] = 'Energía, Optimización, Ofertas, GECELCA'
            d['CreationDate'] = datetime.now()
        
        logger.info(f"Reporte PDF generado exitosamente: {archivo_pdf}")
        print(f"✅ Reporte PDF generado: {archivo_pdf}")
        
        # También generar el archivo de estadísticas en Excel
        crear_resumen_estadisticas_excel(estadisticas, archivo_salida)
        
        return True
        
    except Exception as e:
        logger.exception(f"Error al generar reporte PDF: {e}")
        return False

def crear_resumen_estadisticas_excel(estadisticas_anuales, archivo_salida):
    """
    Crea un resumen de estadísticas en formato Excel.
    
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