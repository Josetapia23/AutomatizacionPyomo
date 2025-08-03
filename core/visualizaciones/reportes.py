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
    crear_grafica_torta_adjudicacion,
    crear_grafica_energia_por_anos
)
from .avanzadas import (
    crear_mapa_calor_mensual,
    crear_distribucion_por_agente,
    crear_tabla_energia_faltante_horaria,
    crear_tabla_energia_faltante_mw_promedio
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
    SIN DISTRIBUCIÓN POR AGENTE - Solo gráficas esenciales.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        ofertas_df (DataFrame): DataFrame con ofertas originales
        archivo_salida (str): Archivo base para generar el reporte
        
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    logger.info("Generando reporte completo SIN distribución por agente")
    print("📊 Iniciando generación de visualizaciones esenciales...")
    
    try:
        # Crear directorio de salida para las visualizaciones
        archivo_base = Path(archivo_salida)
        output_dir = archivo_base.parent / "visualizaciones"
        ensure_directory_exists(output_dir)
        
        # Contadores de éxito y tracking detallado
        graficas_exitosas = 0
        graficas_totales = 0
        archivos_generados = []
        errores_encontrados = []
        
        # 1. Gráfica Principal: Energía Asignada y No Asignada
        print("🔹 [1/8] Creando gráfica principal de energía asignada...")
        graficas_totales += 1
        try:
            fig_principal = crear_grafica_principal_energia_asignada(resultados_dict)
            if fig_principal:
                archivo_principal = output_dir / "01_energia_asignada_principal.html"
                pyo.plot(fig_principal, filename=str(archivo_principal), auto_open=False)
                print(f"  ✅ ÉXITO: {archivo_principal.name}")
                archivos_generados.append("01_energia_asignada_principal.html")
                graficas_exitosas += 1
            else:
                print("  ❌ FALLO: No se pudo crear la gráfica principal")
                errores_encontrados.append("Gráfica principal: Sin datos válidos")
        except Exception as e:
            print(f"  ❌ FALLO: {str(e)[:60]}...")
            errores_encontrados.append(f"Gráfica principal: {e}")
            logger.error(f"Error en gráfica principal: {e}")
        
        # 2. Gráfica de Resumen General
        print("🔹 [2/8] Creando gráfica de resumen general...")
        graficas_totales += 1
        try:
            fig_resumen = crear_grafica_resumen_general(resultados_dict)
            if fig_resumen:
                archivo_resumen = output_dir / "02_resumen_general.html"
                pyo.plot(fig_resumen, filename=str(archivo_resumen), auto_open=False)
                print(f"  ✅ ÉXITO: {archivo_resumen.name}")
                archivos_generados.append("02_resumen_general.html")
                graficas_exitosas += 1
            else:
                print("  ❌ FALLO: No se encontró RESUMEN EJECUTIVO")
                errores_encontrados.append("Resumen general: Falta RESUMEN EJECUTIVO")
        except Exception as e:
            print(f"  ❌ FALLO: {str(e)[:60]}...")
            errores_encontrados.append(f"Resumen general: {e}")
            logger.error(f"Error en gráfica de resumen: {e}")
        
        # 3. Gráfica de Torta
        print("🔹 [3/8] Creando gráfica de torta de adjudicación...")
        graficas_totales += 1
        try:
            fig_torta = crear_grafica_torta_adjudicacion(resultados_dict)
            if fig_torta:
                archivo_torta = output_dir / "03_torta_adjudicacion.html"
                pyo.plot(fig_torta, filename=str(archivo_torta), auto_open=False)
                print(f"  ✅ ÉXITO: {archivo_torta.name}")
                archivos_generados.append("03_torta_adjudicacion.html")
                graficas_exitosas += 1
            else:
                print("  ❌ FALLO: No se pudieron procesar datos de ofertas")
                errores_encontrados.append("Torta: Sin datos de ofertas procesables")
        except Exception as e:
            print(f"  ❌ FALLO: {str(e)[:60]}...")
            errores_encontrados.append(f"Torta: {e}")
            logger.error(f"Error en gráfica de torta: {e}")
        
        # 4. Mapa de Calor Mensual
        print("🔹 [4/8] Creando mapa de calor mensual...")
        graficas_totales += 1
        try:
            fig_mapa_calor = crear_mapa_calor_mensual(resultados_dict)
            if fig_mapa_calor:
                archivo_mapa = output_dir / "04_mapa_calor_mensual.html"
                pyo.plot(fig_mapa_calor, filename=str(archivo_mapa), auto_open=False)
                print(f"  ✅ ÉXITO: {archivo_mapa.name}")
                archivos_generados.append("04_mapa_calor_mensual.html")
                graficas_exitosas += 1
            else:
                print("  ❌ FALLO: No se encontró DEMANDA_FALTANTE")
                errores_encontrados.append("Mapa calor: Falta DEMANDA_FALTANTE")
        except Exception as e:
            print(f"  ❌ FALLO: {str(e)[:60]}...")
            errores_encontrados.append(f"Mapa calor: {e}")
            logger.error(f"Error en mapa de calor mensual: {e}")

        # 5. Tabla de Energía Faltante Horaria (RENUMERADA)
        print("🔹 [5/8] Creando tabla de energía faltante horaria...")
        graficas_totales += 1
        try:
            fig_energia_horaria = crear_tabla_energia_faltante_horaria(resultados_dict)
            if fig_energia_horaria:
                archivo_energia_horaria = output_dir / "05_energia_faltante_horaria.html"  # RENUMERADA
                pyo.plot(fig_energia_horaria, filename=str(archivo_energia_horaria), auto_open=False)
                print(f"  ✅ ÉXITO: {archivo_energia_horaria.name}")
                archivos_generados.append("05_energia_faltante_horaria.html")
                graficas_exitosas += 1
            else:
                print("  ❌ FALLO: No se encontró DEMANDA_FALTANTE")
                errores_encontrados.append("Tabla horaria: Falta DEMANDA_FALTANTE")
        except Exception as e:
            print(f"  ❌ FALLO: {str(e)[:60]}...")
            errores_encontrados.append(f"Tabla horaria: {e}")
            logger.error(f"Error en tabla de energía faltante horaria: {e}")

        # 6. Tabla de Energía Faltante MW Promedio (RENUMERADA)
        print("🔹 [6/8] Creando tabla de energía faltante MW promedio...")
        graficas_totales += 1
        try:
            fig_energia_mw = crear_tabla_energia_faltante_mw_promedio(resultados_dict)
            if fig_energia_mw:
                archivo_energia_mw = output_dir / "06_energia_faltante_mw_promedio.html"  # RENUMERADA
                pyo.plot(fig_energia_mw, filename=str(archivo_energia_mw), auto_open=False)
                print(f"  ✅ ÉXITO: {archivo_energia_mw.name}")
                archivos_generados.append("06_energia_faltante_mw_promedio.html")
                graficas_exitosas += 1
            else:
                print("  ❌ FALLO: No se encontró DEMANDA_FALTANTE")
                errores_encontrados.append("Tabla MW: Falta DEMANDA_FALTANTE")
        except Exception as e:
            print(f"  ❌ FALLO: {str(e)[:60]}...")
            errores_encontrados.append(f"Tabla MW: {e}")
            logger.error(f"Error en tabla MW promedio: {e}")

        # 7. Gráfica de Energía por Años (RENUMERADA)
        print("🔹 [7/8] Creando gráfica de energía por años...")
        graficas_totales += 1
        try:
            fig_energia_anos = crear_grafica_energia_por_anos(resultados_dict)
            if fig_energia_anos:
                archivo_energia_anos = output_dir / "07_energia_por_anos.html"  # RENUMERADA
                pyo.plot(fig_energia_anos, filename=str(archivo_energia_anos), auto_open=False)
                print(f"  ✅ ÉXITO: {archivo_energia_anos.name}")
                archivos_generados.append("07_energia_por_anos.html")
                graficas_exitosas += 1
            else:
                print("  ❌ FALLO: No se encontraron datos por años")
                errores_encontrados.append("Gráfica anual: Sin datos por años")
        except Exception as e:
            print(f"  ❌ FALLO: {str(e)[:60]}...")
            errores_encontrados.append(f"Gráfica anual: {e}")
            logger.error(f"Error en gráfica anual: {e}")
        
        # 8. Gráficas por Oferta (Individuales + Consolidada) (RENUMERADA)
        print("🔹 [8/8] Creando gráficas por oferta (individuales + consolidada)...")
        graficas_totales += 1
        resultado_ofertas = {}
        try:
            # Usar la nueva función completa del módulo por_oferta
            resultado_ofertas = crear_graficas_por_oferta_completo(resultados_dict, output_dir)
            
            graficas_individuales = resultado_ofertas.get('individuales', {})
            grafica_consolidada = resultado_ofertas.get('consolidada')
            reporte_individuales = resultado_ofertas.get('reporte_individuales')
            
            if graficas_individuales or grafica_consolidada:
                print(f"  ✅ ÉXITO: Gráficas por oferta completadas")
                print(f"    - Individuales: {len(graficas_individuales)} ofertas")
                print(f"    - Consolidada: {'✅' if grafica_consolidada else '❌'}")
                if reporte_individuales:
                    print(f"    - Reporte: {reporte_individuales.name}")
                    archivos_generados.append("reporte_ofertas.html")
                if grafica_consolidada:
                    archivos_generados.append("08_consolidado_ofertas.html")  # RENUMERADA
                archivos_generados.extend([f"oferta_{nome}.html" for nome in graficas_individuales.keys()])
                graficas_exitosas += 1
            else:
                print("  ❌ FALLO: No se pudieron crear gráficas por oferta")
                errores_encontrados.append("Ofertas: Sin gráficas generadas")
        except Exception as e:
            print(f"  ❌ FALLO: {str(e)[:60]}...")
            errores_encontrados.append(f"Ofertas: {e}")
            logger.error(f"Error en gráficas por oferta: {e}")

        # 9. Reporte HTML Consolidado
        print("🔹 [+] Creando reporte HTML consolidado...")
        try:
            crear_reporte_html_consolidado_fusionado(resultados_dict, output_dir, resultado_ofertas)
            print(f"  ✅ ÉXITO: reporte_consolidado.html")
            archivos_generados.append("reporte_consolidado.html")
        except Exception as e:
            print(f"  ❌ FALLO: {str(e)[:60]}...")
            errores_encontrados.append(f"Reporte HTML: {e}")
            logger.error(f"Error en reporte HTML: {e}")
        
        # RESUMEN FINAL DETALLADO
        print(f"\n" + "="*60)
        print(f"📈 RESUMEN FINAL DE VISUALIZACIONES (SIN AGENTES)")
        print(f"="*60)
        print(f"✅ EXITOSAS: {graficas_exitosas}/{graficas_totales}")
        print(f"❌ FALLIDAS: {len(errores_encontrados)}")
        print(f"📁 UBICACIÓN: {output_dir}")
        print(f"")
        
        if archivos_generados:
            print(f"📊 ARCHIVOS GENERADOS ({len(archivos_generados)}):")
            for i, archivo in enumerate(sorted(archivos_generados), 1):
                print(f"   {i:2d}. {archivo}")
        
        if errores_encontrados:
            print(f"\n❌ ERRORES ENCONTRADOS ({len(errores_encontrados)}):")
            for i, error in enumerate(errores_encontrados, 1):
                print(f"   {i:2d}. {error}")
        
        # Información de las gráficas por oferta
        if resultado_ofertas:
            graficas_individuales = resultado_ofertas.get('individuales', {})
            grafica_consolidada = resultado_ofertas.get('consolidada')
            print(f"\n🏢 GRÁFICAS POR OFERTA:")
            print(f"   - Individuales: {len(graficas_individuales)} ofertas")
            print(f"   - Consolidada: {'✅ Generada' if grafica_consolidada else '❌ Error'}")
        
        if graficas_exitosas > 0:
            print(f"\n🎉 ¡PROCESO COMPLETADO!")
            print(f"🌐 Archivo principal: {output_dir / 'reporte_consolidado.html'}")
            print(f"💡 Abre el archivo HTML en tu navegador para ver todas las gráficas")
            print(f"="*60)
            return True
        else:
            print(f"\n⚠️ NO SE GENERARON VISUALIZACIONES")
            print(f"🔧 Revisa los errores arriba para diagnóstico")
            print(f"="*60)
            return False
            
    except Exception as e:
        logger.exception(f"Error general en generación de reporte sin agentes: {e}")
        print(f"❌ ERROR CRÍTICO: {e}")
        return False

def crear_reporte_html_consolidado_fusionado(resultados_dict, output_dir, resultado_ofertas=None):
    """
    Crea un reporte HTML consolidado SIN distribución por agente.
    Solo incluye las gráficas esenciales y consolidado de ofertas.
    
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
        fig_energia_horaria = crear_tabla_energia_faltante_horaria(resultados_dict)
        
        # Extraer datos de ofertas
        graficas_individuales = resultado_ofertas.get('individuales', {}) if resultado_ofertas else {}
        grafica_consolidada = resultado_ofertas.get('consolidada') if resultado_ofertas else None
        
        # Generar HTML consolidado SIN DISTRIBUCIÓN POR AGENTE
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
                <p><strong>Reporte consolidado con visualizaciones esenciales</strong></p>
            </div>
            
            <div class="navigation">
                <h3>🧭 Navegación del Reporte</h3>
                <a href="#generales" class="nav-button">📈 Gráficas Generales</a>
                <a href="#avanzadas" class="nav-button">🔬 Análisis Avanzados</a>
                <a href="#consolidada" class="nav-button">🎯 Consolidado Ofertas</a>
                <a href="reporte_ofertas.html" class="nav-button" target="_blank">📋 Análisis por Ofertas</a>
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
        
        # SECCIÓN: Análisis Avanzados (SIN DISTRIBUCIÓN POR AGENTE)
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
        
        # Tabla de energía faltante horaria (RENUMERADA)
        if fig_energia_horaria:
            html_content += f"""
            <div class="grafica-container">
                <h3>🔹 Energía Faltante Horaria Mensual por Año</h3>
                <div class="descripcion">
                    Tabla detallada que muestra la energía faltante (GW-total) desglosada por año, mes y hora.
                </div>
                <div style="text-align: center; margin: 10px 0;">
                    <a href="05_energia_faltante_horaria.html" class="nav-button" target="_blank">
                        📊 Ver Tabla Horaria Completa
                    </a>
                </div>
            </div>
            """
        
        # Tabla MW promedio (RENUMERADA)
        html_content += f"""
        <div class="grafica-container">
            <h3>🔹 Energía Faltante MW (promedio diario)</h3>
            <div class="descripcion">
                Promedio diario por mes: (GW-total ÷ días del mes) × 1000 = MW promedio.
            </div>
            <div style="text-align: center; margin: 10px 0;">
                <a href="06_energia_faltante_mw_promedio.html" class="nav-button" target="_blank">
                    ⚡ Ver Tabla MW Promedio
                </a>
            </div>
        </div>
        """
        
        # Gráfica de energía por años (RENUMERADA)
        html_content += f"""
            <div class="grafica-container">
                <h3>🔹 Energía Asignada y No Asignada por Año</h3>
                <div class="descripcion">
                    Visualización anual de la distribución de energía con análisis de tendencias.
                </div>
                <div style="text-align: center; margin: 10px 0;">
                    <a href="07_energia_por_anos.html" class="nav-button" target="_blank">
                        📅 Ver Análisis Anual Completo
                    </a>
                </div>
            </div>
            """
        
        html_content += "</div>"  # Cerrar sección avanzadas
        
        # SECCIÓN: Gráfica Consolidada de Ofertas (RENUMERADA)
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
                        <a href="08_consolidado_ofertas.html" class="oferta-link consolidada-button" target="_blank">
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
        
        # SECCIÓN: Análisis Individual por Ofertas
        if graficas_individuales:
            html_content += f"""
            <div id="ofertas">
                <h2 style="color: #1f4e79; border-bottom: 2px solid #1f4e79; padding-bottom: 10px;">
                    📋 ANÁLISIS INDIVIDUAL POR OFERTAS
                </h2>
                
                <div class="ofertas-section">
                    <h3>🔍 Análisis Detallado por Oferta Individual</h3>
                    <p>Se procesaron <strong>{len(graficas_individuales)} ofertas individuales</strong> con gráficas detalladas.</p>
                    <p><strong>Haga clic en el botón para ver el reporte completo de ofertas:</strong></p>
                    
                    <div style="text-align: center; margin: 20px 0;">
                        <a href="reporte_ofertas.html" class="oferta-link consolidada-button" target="_blank">
                            📊 VER REPORTE COMPLETO DE OFERTAS ({len(graficas_individuales)} ofertas)
                        </a>
                    </div>
                    
                    <div style="background-color: white; padding: 15px; border-radius: 5px; margin-top: 15px;">
                        <h4 style="color: #1f4e79; margin-top: 0;">💡 El reporte de ofertas incluye:</h4>
                        <ul style="color: #666; font-size: 13px;">
                            <li><strong>Gráficas individuales:</strong> Análisis específico de cada oferta</li>
                            <li><strong>Navegación fácil:</strong> Botones para alternar entre ofertas</li>
                            <li><strong>Datos detallados:</strong> Energía asignada, precios y evolución temporal</li>
                            <li><strong>Comparación visual:</strong> Barras y líneas para análisis completo</li>
                        </ul>
                    </div>
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
        
        logger.info(f"Reporte HTML consolidado SIN agentes creado: {archivo_consolidado}")
        
    except Exception as e:
        logger.error(f"Error al crear reporte HTML consolidado sin agentes: {e}")
        raise
    