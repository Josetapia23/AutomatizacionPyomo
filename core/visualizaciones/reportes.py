"""
Módulo para generar reportes completos con visualizaciones.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.offline as pyo
import logging
from pathlib import Path
from datetime import datetime

from .basicas import (
    crear_grafica_principal_energia_asignada,
    crear_grafica_resumen_general,
    crear_grafica_torta_adjudicacion
)
from .avanzadas import (
    crear_mapa_calor_mensual,
    crear_distribucion_por_agente,
    crear_tabla_energia_faltante_horaria  
)
from .utils import ensure_directory_exists, format_number

logger = logging.getLogger(__name__)

def generar_reporte_completo_mejorado(resultados_dict, ofertas_df, archivo_salida):
    """
    Genera un reporte completo con visualizaciones mejoradas según especificaciones del cliente.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        ofertas_df (DataFrame): DataFrame con ofertas originales
        archivo_salida (str): Archivo base para generar el reporte
        
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    logger.info("Generando reporte completo mejorado")
    print("📊 Iniciando generación de visualizaciones...")
    
    try:
        # Crear directorio de salida para las visualizaciones
        archivo_base = Path(archivo_salida)
        output_dir = archivo_base.parent / "visualizaciones"
        ensure_directory_exists(output_dir)
        
        # Contadores de éxito
        graficas_exitosas = 0
        graficas_totales = 0
        
        # 1. Gráfica Principal: Energía Asignada y No Asignada
        print("🔹 Creando gráfica principal de energía asignada...")
        graficas_totales += 1
        try:
            fig_principal = crear_grafica_principal_energia_asignada(resultados_dict)
            if fig_principal:
                archivo_principal = output_dir / "01_energia_asignada_principal.html"
                pyo.plot(fig_principal, filename=str(archivo_principal), auto_open=False)
                print(f"  ✅ Gráfica principal guardada: {archivo_principal}")
                graficas_exitosas += 1
            else:
                print("  ❌ Error: No se pudo crear la gráfica principal")
        except Exception as e:
            print(f"  ❌ Error en gráfica principal: {e}")
            logger.error(f"Error en gráfica principal: {e}")
        
        # 2. Gráfica de Resumen General
        print("🔹 Creando gráfica de resumen general...")
        graficas_totales += 1
        try:
            fig_resumen = crear_grafica_resumen_general(resultados_dict)
            if fig_resumen:
                archivo_resumen = output_dir / "02_resumen_general.html"
                pyo.plot(fig_resumen, filename=str(archivo_resumen), auto_open=False)
                print(f"  ✅ Gráfica de resumen guardada: {archivo_resumen}")
                graficas_exitosas += 1
            else:
                print("  ⚠️ No se pudo crear la gráfica de resumen (posiblemente faltan datos)")
        except Exception as e:
            print(f"  ❌ Error en gráfica de resumen: {e}")
            logger.error(f"Error en gráfica de resumen: {e}")
        
        # 3. Gráfica de Torta
        print("🔹 Creando gráfica de torta de adjudicación...")
        graficas_totales += 1
        try:
            fig_torta = crear_grafica_torta_adjudicacion(resultados_dict)
            if fig_torta:
                archivo_torta = output_dir / "03_torta_adjudicacion.html"
                pyo.plot(fig_torta, filename=str(archivo_torta), auto_open=False)
                print(f"  ✅ Gráfica de torta guardada: {archivo_torta}")
                graficas_exitosas += 1
            else:
                print("  ⚠️ No se pudo crear la gráfica de torta")
        except Exception as e:
            print(f"  ❌ Error en gráfica de torta: {e}")
            logger.error(f"Error en gráfica de torta: {e}")
        
        # 4. Mapa de Calor Mensual
        print("🔹 Creando mapa de calor mensual...")
        graficas_totales += 1
        try:
            fig_mapa_calor = crear_mapa_calor_mensual(resultados_dict)
            if fig_mapa_calor:
                archivo_mapa = output_dir / "04_mapa_calor_mensual.html"
                pyo.plot(fig_mapa_calor, filename=str(archivo_mapa), auto_open=False)
                print(f"  ✅ Mapa de calor mensual guardado: {archivo_mapa}")
                graficas_exitosas += 1
            else:
                print("  ⚠️ No se pudo crear el mapa de calor mensual")
        except Exception as e:
            print(f"  ❌ Error en mapa de calor mensual: {e}")
            logger.error(f"Error en mapa de calor mensual: {e}")

        # 5. Distribución por Agente
        print("🔹 Creando distribución por agente...")
        graficas_totales += 1
        try:
            fig_agentes = crear_distribucion_por_agente(resultados_dict)
            if fig_agentes:
                archivo_agentes = output_dir / "05_distribucion_agentes.html"
                pyo.plot(fig_agentes, filename=str(archivo_agentes), auto_open=False)
                print(f"  ✅ Distribución por agente guardada: {archivo_agentes}")
                graficas_exitosas += 1
            else:
                print("  ⚠️ No se pudo crear la distribución por agente")
        except Exception as e:
            print(f"  ❌ Error en distribución por agente: {e}")
            logger.error(f"Error en distribución por agente: {e}")
        
        # 6. NUEVA VISUALIZACIÓN: Tabla de Energía Faltante Horaria
        print("🔹 Creando tabla de energía faltante horaria...")
        graficas_totales += 1
        try:
            fig_energia_horaria = crear_tabla_energia_faltante_horaria(resultados_dict)
            if fig_energia_horaria:
                archivo_energia_horaria = output_dir / "06_energia_faltante_horaria.html"
                pyo.plot(fig_energia_horaria, filename=str(archivo_energia_horaria), auto_open=False)
                print(f"  ✅ Tabla de energía faltante horaria guardada: {archivo_energia_horaria}")
                graficas_exitosas += 1
            else:
                print("  ⚠️ No se pudo crear la tabla de energía faltante horaria")
        except Exception as e:
            print(f"  ❌ Error en tabla de energía faltante horaria: {e}")
            logger.error(f"Error en tabla de energía faltante horaria: {e}")
        
        print("🔹 Creando reporte HTML consolidado...")
        try:
            crear_reporte_html_consolidado(resultados_dict, output_dir)
            print(f"  ✅ Reporte HTML consolidado creado")
            graficas_exitosas += 1
        except Exception as e:
            print(f"  ❌ Error en reporte HTML: {e}")
            logger.error(f"Error en reporte HTML: {e}")
        
        # Resumen final
        print(f"\n📈 Resumen de visualizaciones:")
        print(f"   ✅ Exitosas: {graficas_exitosas}")
        print(f"   ❌ Fallidas: {graficas_totales - graficas_exitosas}")
        print(f"   📁 Ubicación: {output_dir}")
        
        if graficas_exitosas > 0:
            print(f"\n🎉 ¡Se generaron {graficas_exitosas} visualizaciones exitosamente!")
            return True
        else:
            print(f"\n⚠️ No se pudieron generar visualizaciones")
            return False
            
    except Exception as e:
        logger.exception(f"Error general en generación de reporte: {e}")
        print(f"❌ Error general: {e}")
        return False

def crear_reporte_html_consolidado(resultados_dict, output_dir):
    """
    Crea un reporte HTML consolidado con todas las gráficas.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        output_dir (Path): Directorio de salida
    """
    try:
        # Crear las gráficas
        fig_principal = crear_grafica_principal_energia_asignada(resultados_dict)
        fig_resumen = crear_grafica_resumen_general(resultados_dict)
        fig_torta = crear_grafica_torta_adjudicacion(resultados_dict)
        fig_mapa_calor = crear_mapa_calor_mensual(resultados_dict)
        fig_agentes = crear_distribucion_por_agente(resultados_dict)
        fig_energia_horaria = crear_tabla_energia_faltante_horaria(resultados_dict)  # NUEVA
        
        # Generar HTML consolidado
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Reporte de Optimización Energética</title>
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
                .grafica-container {{
                    background-color: white;
                    margin: 20px 0;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .descripcion {{
                    color: #666;
                    font-size: 14px;
                    margin-bottom: 15px;
                }}
                .timestamp {{
                    text-align: center;
                    color: #999;
                    font-size: 12px;
                    margin-top: 30px;
                }}
            </style>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        </head>
        <body>
            <div class="header">
                <h1>📊 REPORTE DE OPTIMIZACIÓN ENERGÉTICA</h1>
                <h2>Análisis de Asignación de Ofertas</h2>
            </div>
        """
        
        # Agregar gráfica principal
        if fig_principal:
            html_content += """
            <div class="grafica-container">
                <h3>🔹 Energía Asignada y No Asignada por Hora</h3>
                <div class="descripcion">
                    Esta gráfica muestra la distribución horaria de energía asignada (DA) y no asignada (ENA), 
                    junto con el porcentaje de energía no asignada por hora.
                </div>
                <div id="grafica-principal"></div>
            </div>
            """
        
        # Agregar gráfica de resumen
        if fig_resumen:
            html_content += """
            <div class="grafica-container">
                <h3>🔹 Resumen General de Adjudicación</h3>
                <div class="descripcion">
                    Métricas principales: energía total adjudicada, precio ponderado, 
                    y rangos de precios máximo y mínimo.
                </div>
                <div id="grafica-resumen"></div>
            </div>
            """
        
        # Agregar gráfica de torta
        if fig_torta:
            html_content += """
            <div class="grafica-container">
                <h3>🔹 Distribución de Energía Adjudicada</h3>
                <div class="descripcion">
                    Proporción de energía adjudicada versus no adjudicada del total ofertado.
                </div>
                <div id="grafica-torta"></div>
            </div>
            """
        
        # Agregar mapa de calor
        if fig_mapa_calor:
            html_content += """
            <div class="grafica-container">
                <h3>🔹 Mapa de Calor: Demanda Faltante Mensual</h3>
                <div class="descripcion">
                    Visualización de la demanda faltante agregada por mes y año.
                </div>
                <div id="grafica-mapa-calor"></div>
            </div>
            """
        
        # NUEVA SECCIÓN: Agregar tabla de energía faltante horaria
        if fig_energia_horaria:
            html_content += """
            <div class="grafica-container">
                <h3>🔹 Energía Faltante Horaria Mensual por Año</h3>
                <div class="descripcion">
                    Tabla detallada que muestra la energía faltante (GWh) desglosada por año, mes y hora.
                    Permite identificar patrones temporales de déficit energético con granularidad horaria.
                </div>
                <div id="grafica-energia-horaria"></div>
            </div>
            """
        
        # Cerrar HTML y agregar scripts de Plotly
        html_content += f"""
            <div class="timestamp">
                Generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M:%S')}
            </div>
            
            <script>
        """
        
        # Agregar scripts de las gráficas
        if fig_principal:
            config_json = fig_principal.to_json()
            html_content += f"""
                var grafica_principal = {config_json};
                Plotly.newPlot('grafica-principal', grafica_principal.data, grafica_principal.layout, {{responsive: true}});
            """
        
        if fig_resumen:
            config_json = fig_resumen.to_json()
            html_content += f"""
                var grafica_resumen = {config_json};
                Plotly.newPlot('grafica-resumen', grafica_resumen.data, grafica_resumen.layout, {{responsive: true}});
            """
        
        if fig_torta:
            config_json = fig_torta.to_json()
            html_content += f"""
                var grafica_torta = {config_json};
                Plotly.newPlot('grafica-torta', grafica_torta.data, grafica_torta.layout, {{responsive: true}});
            """
        
        if fig_mapa_calor:
            config_json = fig_mapa_calor.to_json()
            html_content += f"""
                var grafica_mapa_calor = {config_json};
                Plotly.newPlot('grafica-mapa-calor', grafica_mapa_calor.data, grafica_mapa_calor.layout, {{responsive: true}});
            """
        
        # NUEVO: Script para tabla de energía faltante horaria
        if fig_energia_horaria:
            config_json = fig_energia_horaria.to_json()
            html_content += f"""
                var grafica_energia_horaria = {config_json};
                Plotly.newPlot('grafica-energia-horaria', grafica_energia_horaria.data, grafica_energia_horaria.layout, {{responsive: true}});
            """
        
        html_content += """
            </script>
        </body>
        </html>
        """
        
        # Guardar archivo HTML
        archivo_consolidado = output_dir / "reporte_consolidado.html"
        with open(archivo_consolidado, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Reporte HTML consolidado creado: {archivo_consolidado}")
        
    except Exception as e:
        logger.error(f"Error al crear reporte HTML consolidado: {e}")
        raise