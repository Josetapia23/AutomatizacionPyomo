"""
Módulo para generar reportes completos con visualizaciones.
FUSIONADO: Incluye todas las gráficas existentes + nuevas gráficas por oferta + navegación mejorada.
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
    crear_tabla_energia_faltante_horaria  # 🔄 MANTENER del sistema actual
)
from .por_oferta import (  # 🆕 NUEVO MÓDULO AGREGADO
    crear_graficas_por_oferta_completo,
    generar_reporte_consolidado_ofertas
)
from .utils import ensure_directory_exists, format_number

logger = logging.getLogger(__name__)

def generar_reporte_completo_mejorado(resultados_dict, ofertas_df, archivo_salida):
    """
    Genera un reporte completo con visualizaciones mejoradas según especificaciones del cliente.
    FUSIONADO: Incluye TODAS las gráficas + navegación mejorada del sistema anterior.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        ofertas_df (DataFrame): DataFrame con ofertas originales
        archivo_salida (str): Archivo base para generar el reporte
        
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    logger.info("Generando reporte completo FUSIONADO con todas las visualizaciones")
    print("📊 Iniciando generación de visualizaciones completas...")
    
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
        
        # 🔄 6. MANTENER: Tabla de Energía Faltante Horaria (del sistema actual)
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

        # 🆕 7. NUEVO: Gráficas por Oferta (Individuales + Consolidada)
        print("🔹 Creando gráficas por oferta (individuales + consolidada)...")
        graficas_totales += 1
        resultado_ofertas = {}
        try:
            # Usar la nueva función completa del módulo por_oferta
            resultado_ofertas = crear_graficas_por_oferta_completo(resultados_dict, output_dir)
            
            graficas_individuales = resultado_ofertas.get('individuales', {})
            grafica_consolidada = resultado_ofertas.get('consolidada')
            reporte_individuales = resultado_ofertas.get('reporte_individuales')
            
            if graficas_individuales or grafica_consolidada:
                print(f"  ✅ Gráficas por oferta completadas:")
                print(f"    - Individuales: {len(graficas_individuales)} ofertas")
                print(f"    - Consolidada: {'✅ Creada' if grafica_consolidada else '❌ Error'}")
                if reporte_individuales:
                    print(f"    - Reporte individuales: {reporte_individuales.name}")
                graficas_exitosas += 1
            else:
                print("  ⚠️ No se pudieron crear gráficas por oferta")
        except Exception as e:
            print(f"  ❌ Error en gráficas por oferta: {e}")
            logger.error(f"Error en gráficas por oferta: {e}")

        # 8. Reporte HTML Consolidado MEJORADO (con navegación del sistema anterior)
        print("🔹 Creando reporte HTML consolidado con navegación mejorada...")
        try:
            crear_reporte_html_consolidado_fusionado(resultados_dict, output_dir, resultado_ofertas)
            print(f"  ✅ Reporte HTML consolidado con navegación mejorada creado")
            graficas_exitosas += 1
        except Exception as e:
            print(f"  ❌ Error en reporte HTML: {e}")
            logger.error(f"Error en reporte HTML: {e}")
        
        # Resumen final MEJORADO
        print(f"\n📈 Resumen de visualizaciones COMPLETAS:")
        print(f"   ✅ Exitosas: {graficas_exitosas}")
        print(f"   ❌ Fallidas: {graficas_totales - graficas_exitosas}")
        print(f"   📁 Ubicación: {output_dir}")
        print(f"   📊 Tipos generados:")
        print(f"     - Gráfica principal de energía")
        print(f"     - Resumen general de adjudicación")
        print(f"     - Gráfica de torta de distribución")
        print(f"     - Mapa de calor mensual")
        print(f"     - Distribución por agente")
        print(f"     - Tabla de energía faltante horaria")
        
        if resultado_ofertas:
            graficas_individuales = resultado_ofertas.get('individuales', {})
            grafica_consolidada = resultado_ofertas.get('consolidada')
            print(f"     - Gráficas por oferta: {len(graficas_individuales)} individuales")
            print(f"     - Gráfica consolidada: {'✅ Creada' if grafica_consolidada else '❌ Error'}")
        
        if graficas_exitosas > 0:
            print(f"\n🎉 ¡Se generaron {graficas_exitosas} tipos de visualizaciones exitosamente!")
            print(f"🌐 Archivo principal: {output_dir / 'reporte_consolidado.html'}")
            print(f"💡 Abre el archivo HTML en tu navegador para ver todas las gráficas")
            return True
        else:
            print(f"\n⚠️ No se pudieron generar visualizaciones")
            return False
            
    except Exception as e:
        logger.exception(f"Error general en generación de reporte fusionado: {e}")
        print(f"❌ Error general: {e}")
        return False

def crear_reporte_html_consolidado_fusionado(resultados_dict, output_dir, resultado_ofertas=None):
    """
    Crea un reporte HTML consolidado con TODAS las gráficas y navegación mejorada.
    FUSIONADO: Combina el estilo del sistema anterior con todas las funcionalidades actuales.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        output_dir (Path): Directorio de salida
        resultado_ofertas (dict, opcional): Resultado completo de gráficas por oferta
    """
    try:
        # Crear las gráficas generales
        fig_principal = crear_grafica_principal_energia_asignada(resultados_dict)
        fig_resumen = crear_grafica_resumen_general(resultados_dict)
        fig_torta = crear_grafica_torta_adjudicacion(resultados_dict)
        fig_mapa_calor = crear_mapa_calor_mensual(resultados_dict)
        fig_agentes = crear_distribucion_por_agente(resultados_dict)
        fig_energia_horaria = crear_tabla_energia_faltante_horaria(resultados_dict)  # 🔄 AGREGADA
        
        # Extraer datos de ofertas
        graficas_individuales = resultado_ofertas.get('individuales', {}) if resultado_ofertas else {}
        grafica_consolidada = resultado_ofertas.get('consolidada') if resultado_ofertas else None
        
        # Generar HTML consolidado MEJORADO (con navegación del sistema anterior)
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Reporte de Optimización Energética - Completo</title>
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
                .ofertas-section {{
                    background-color: #e8f4f8;
                    padding: 20px;
                    border-radius: 8px;
                    margin: 20px 0;
                }}
                .consolidada-section {{
                    background-color: #f0f8e8;
                    padding: 20px;
                    border-radius: 8px;
                    margin: 20px 0;
                    border-left: 5px solid #2ecc71;
                }}
                .oferta-link, .nav-button {{
                    display: inline-block;
                    padding: 8px 15px;
                    background-color: #1f4e79;
                    color: white;
                    text-decoration: none;
                    border-radius: 5px;
                    margin: 5px;
                    font-size: 12px;
                }}
                .oferta-link:hover, .nav-button:hover {{
                    background-color: #2ecc71;
                }}
                .consolidada-button {{
                    background-color: #2ecc71;
                    padding: 12px 25px;
                    font-size: 14px;
                    font-weight: bold;
                }}
                .consolidada-button:hover {{
                    background-color: #27ae60;
                }}
                .timestamp {{
                    text-align: center;
                    color: #999;
                    font-size: 12px;
                    margin-top: 30px;
                }}
                .navigation {{
                    text-align: center;
                    margin: 20px 0;
                    background-color: white;
                    padding: 15px;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
            </style>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        </head>
        <body>
            <div class="header">
                <h1>📊 REPORTE DE OPTIMIZACIÓN ENERGÉTICA</h1>
                <h2>Análisis Completo de Asignación de Ofertas</h2>
                <p><strong>Reporte consolidado con todas las visualizaciones disponibles</strong></p>
            </div>
            
            <div class="navigation">
                <h3>🧭 Navegación del Reporte</h3>
                <a href="#generales" class="nav-button">📈 Gráficas Generales</a>
                <a href="#avanzadas" class="nav-button">🔬 Análisis Avanzados</a>
                <a href="#consolidada" class="nav-button">🎯 Consolidado Ofertas</a>
                <a href="#ofertas" class="nav-button">📋 Ofertas Individuales</a>
                <a href="reporte_ofertas.html" class="nav-button" target="_blank">📄 Reporte Detallado</a>
            </div>
        """
        
        # Sección de gráficas generales
        html_content += """
            <div id="generales">
                <h2 style="color: #1f4e79; border-bottom: 2px solid #1f4e79; padding-bottom: 10px;">
                    📈 GRÁFICAS GENERALES DEL SISTEMA
                </h2>
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
        
        html_content += "</div>"  # Cerrar sección generales
        
        # 🔄 NUEVA SECCIÓN: Análisis Avanzados (incluye mapa de calor, distribución por agente, energía faltante horaria)
        html_content += """
            <div id="avanzadas">
                <h2 style="color: #e74c3c; border-bottom: 2px solid #e74c3c; padding-bottom: 10px;">
                    🔬 ANÁLISIS AVANZADOS
                </h2>
        """
        
        # Mapa de calor
        if fig_mapa_calor:
            html_content += f"""
            <div class="grafica-container">
                <h3>🔹 Mapa de Calor: Demanda Faltante Mensual</h3>
                <div class="descripcion">
                    Visualización de la demanda faltante agregada por mes y año.
                </div>
                <div style="text-align: center; margin: 10px 0;">
                    <a href="04_mapa_calor_mensual.html" class="nav-button" target="_blank">
                        🌡️ Ver Mapa de Calor Completo
                    </a>
                </div>
            </div>
            """
        
        # Distribución por agente
        if fig_agentes:
            html_content += f"""
            <div class="grafica-container">
                <h3>🔹 Distribución por Agente</h3>
                <div class="descripcion">
                    Análisis de distribución de energía asignada y no asignada por cada agente participante.
                </div>
                <div style="text-align: center; margin: 10px 0;">
                    <a href="05_distribucion_agentes.html" class="nav-button" target="_blank">
                        🏢 Ver Distribución Completa
                    </a>
                </div>
            </div>
            """
        
        # 🔄 NUEVA: Tabla de energía faltante horaria
        if fig_energia_horaria:
            html_content += f"""
            <div class="grafica-container">
                <h3>🔹 Energía Faltante Horaria Mensual por Año</h3>
                <div class="descripcion">
                    Tabla detallada que muestra la energía faltante (GWh) desglosada por año, mes y hora.
                    Permite identificar patrones temporales de déficit energético con granularidad horaria.
                </div>
                <div style="text-align: center; margin: 10px 0;">
                    <a href="06_energia_faltante_horaria.html" class="nav-button" target="_blank">
                        📊 Ver Tabla Horaria Completa
                    </a>
                </div>
            </div>
            """
        
        html_content += "</div>"  # Cerrar sección avanzadas
        
        # 🆕 SECCIÓN: Gráfica Consolidada de Ofertas
        if grafica_consolidada:
            html_content += f"""
            <div id="consolidada">
                <h2 style="color: #2ecc71; border-bottom: 2px solid #2ecc71; padding-bottom: 10px;">
                    🎯 CONSOLIDADO DE TODAS LAS OFERTAS
                </h2>
                
                <div class="consolidada-section">
                    <h3>📊 Vista Unificada de Todas las Ofertas</h3>
                    <p><strong>Esta gráfica muestra todas las ofertas en una sola vista consolidada</strong></p>
                    <p>Compare fácilmente volúmenes asignados y precios entre todas las ofertas por período.</p>
                    
                    <div style="text-align: center; margin: 20px 0;">
                        <a href="{grafica_consolidada.name}" class="oferta-link consolidada-button" target="_blank">
                            🎯 VER GRÁFICA CONSOLIDADA COMPLETA
                        </a>
                    </div>
                    
                    <div style="background-color: white; padding: 15px; border-radius: 5px; margin-top: 15px;">
                        <h4 style="color: #2ecc71; margin-top: 0;">💡 ¿Qué incluye la gráfica consolidada?</h4>
                        <ul style="color: #666; font-size: 13px;">
                            <li><strong>Barras apiladas:</strong> Energía total asignada por período (suma de todas las ofertas)</li>
                            <li><strong>Colores en barras:</strong> Contribución de cada oferta al total</li>
                            <li><strong>Líneas punteadas:</strong> Evolución de precios indexados por oferta</li>
                            <li><strong>Comparación directa:</strong> Fácil identificación de ofertas más/menos utilizadas</li>
                            <li><strong>Análisis temporal:</strong> Tendencias de asignación y precios en el tiempo</li>
                        </ul>
                    </div>
                </div>
            </div>
            """
        
        # Sección de ofertas individuales
        if graficas_individuales:
            html_content += f"""
            <div id="ofertas">
                <h2 style="color: #1f4e79; border-bottom: 2px solid #1f4e79; padding-bottom: 10px;">
                    📋 GRÁFICAS INDIVIDUALES POR OFERTA ({len(graficas_individuales)} ofertas)
                </h2>
                
                <div class="ofertas-section">
                    <h3>🔍 Análisis Detallado por Oferta Individual</h3>
                    <p>Cada oferta tiene su propia gráfica detallada con información específica.</p>
                    <p><strong>Haga clic en cualquier oferta para ver su análisis individual:</strong></p>
            """
            
            # Agregar enlaces a cada gráfica individual
            for nombre_oferta, archivo_grafica in sorted(graficas_individuales.items()):
                nombre_archivo = archivo_grafica.name
                html_content += f"""
                    <a href="{nombre_archivo}" class="oferta-link" target="_blank">
                        {nombre_oferta}
                    </a>
                """
            
            html_content += """
                </div>
            </div>
            """
        
        # Cerrar HTML y agregar scripts
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
        
        html_content += """
            </script>
        </body>
        </html>
        """
        
        # Guardar archivo HTML
        archivo_consolidado = output_dir / "reporte_consolidado.html"
        with open(archivo_consolidado, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Reporte HTML consolidado fusionado creado: {archivo_consolidado}")
        
    except Exception as e:
        logger.error(f"Error al crear reporte HTML consolidado fusionado: {e}")
        raise