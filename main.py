"""
Punto de entrada principal para el sistema de optimización energética con Pyomo.
Este módulo coordina el flujo de trabajo completo del sistema.
"""

import os
import sys
import pandas as pd
import logging
import argparse
import pyomo.environ as pyo
from pathlib import Path

# Configurar el path para encontrar el paquete
sys.path.insert(0, str(Path(__file__).parent))

# Importar módulos del paquete
from config import (
    DATOS_INICIALES, OFERTAS_DIR, RESULTADO_OFERTAS, ESTADISTICAS_OFERTAS
)
from core.utils import (
    verificar_archivo_existe, solicitar_input_seguro, leer_excel_seguro
)
from core.indexadores import crear_proyeccion_indexadores, crear_proyeccion_precio_sicep
from core.ofertas import procesar_ofertas, procesar_precio_sicep
from core.evaluacion import (
    evaluar_ofertas_para_optimizacion, calcular_estadisticas_ofertas,
    exportar_asignaciones_por_oferta, crear_hoja_demanda_faltante,
    leer_ofertas_evaluadas, exportar_resultados_por_oferta, cargar_resultados_desde_excel
)
from optimizacion.modelo import construir_modelo, extraer_resultados
from optimizacion.solver import resolver_modelo

try:
    from core.ofertas_optimizado import procesar_ofertas_optimizado_corregido
    OPTIMIZACION_DISPONIBLE = True
    print("🚀 Versión optimizada CORREGIDA de procesamiento cargada")
except ImportError:
    OPTIMIZACION_DISPONIBLE = False
    print("⚠️ Versión optimizada no disponible, usando versión estándar")

# Importar visualizaciones (con manejo de errores en caso de que no exista aún)
try:
    from core.visualizaciones import generar_reporte_completo
    VISUALIZACIONES_DISPONIBLES = True
except ImportError:
    print("ADVERTENCIA: Módulo de visualizaciones no disponible. Las gráficas no se generarán.")
    VISUALIZACIONES_DISPONIBLES = False

# Configurar logger
logger = logging.getLogger(__name__)

def leer_demanda(archivo=DATOS_INICIALES, hoja="DEMANDA"):
    """
    Lee los datos de demanda desde el archivo Excel.
    
    Args:
        archivo (Path): Ruta al archivo Excel
        hoja (str): Nombre de la hoja con datos de demanda
        
    Returns:
        DataFrame: DataFrame con los datos de demanda, o None en caso de error
    """
    logger.info(f"Leyendo datos de demanda desde {archivo} (hoja: {hoja})")
    
    if not verificar_archivo_existe(archivo):
        return None
    
    # Leer datos
    demanda_raw = leer_excel_seguro(archivo, hoja)
    if demanda_raw.empty:
        return None
    
    # Convertir formato de columnas de horas a filas
    try:
        # Obtener solo las columnas numéricas (horas)
        hour_cols = [col for col in demanda_raw.columns if col != "FECHA"]
        
        # Hacer melt para convertir de formato ancho a largo
        demanda_melted = demanda_raw.melt(
            id_vars="FECHA", 
            value_vars=hour_cols,
            var_name="HORA", 
            value_name="DEMANDA"
        )
        
        # Convertir tipos de datos
        demanda_melted['FECHA'] = pd.to_datetime(demanda_melted['FECHA'], dayfirst=True).dt.date
        demanda_melted['HORA'] = demanda_melted['HORA'].astype(int)
        demanda_melted['DEMANDA'] = demanda_melted['DEMANDA'].astype(float)
        
        logger.info(f"Datos de demanda leídos correctamente: {len(demanda_melted)} registros")
        return demanda_melted
    
    except Exception as e:
        logger.error(f"Error al procesar datos de demanda: {e}")
        return None

def calcular_estadisticas_correctas(resultados_dict):
    """
    Calcula estadísticas corregidas usando las claves correctas del diccionario de resultados.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        
    Returns:
        list: Lista de estadísticas calculadas
    """
    filas_stats = []
    
    # Buscar la clave correcta para el resumen (puede ser "RESUMEN EJECUTIVO" o "RESUMEN")
    resumen_key = None
    if "RESUMEN EJECUTIVO" in resultados_dict:
        resumen_key = "RESUMEN EJECUTIVO"
    elif "RESUMEN" in resultados_dict:
        resumen_key = "RESUMEN"
    
    if resumen_key and not resultados_dict[resumen_key].empty:
        resumen_df = resultados_dict[resumen_key]
        
        # Agregar información por oferta
        for col in resumen_df.columns:
            if "CANTIDAD (KWh)" in col:
                nombre_oferta = col.replace(" CANTIDAD (KWh)", "")
                cantidad = resumen_df[col].sum()
                
                # Buscar precio correspondiente (puede tener diferentes formatos)
                precio_promedio = 0
                posibles_precios = [
                    f"{nombre_oferta} PRECIO INDEXADO ($/KWh)",
                    f"{nombre_oferta} PRECIO ($/KWh)",
                    f"{nombre_oferta} PRECIO PROMEDIO"
                ]
                
                for precio_col in posibles_precios:
                    if precio_col in resumen_df.columns:
                        precio_promedio = resumen_df[precio_col].mean()
                        break
                
                if cantidad > 0:  # Solo agregar ofertas con asignación
                    filas_stats.append({
                        "TIPO": "OFERTA",
                        "IDENTIFICADOR": nombre_oferta,
                        "TOTAL ASIGNADO (kWh)": cantidad,
                        "PRECIO PROMEDIO": precio_promedio,
                        "COSTO TOTAL": cantidad * precio_promedio
                    })
        
        # Estadísticas generales si hay datos
        if filas_stats:
            total_general = sum(s["TOTAL ASIGNADO (kWh)"] for s in filas_stats)
            costo_general = sum(s["COSTO TOTAL"] for s in filas_stats)
            precio_promedio_general = costo_general / total_general if total_general > 0 else 0
            
            filas_stats.append({
                "TIPO": "TOTAL",
                "IDENTIFICADOR": "TODAS LAS OFERTAS",
                "TOTAL ASIGNADO (kWh)": total_general,
                "PRECIO PROMEDIO": precio_promedio_general,
                "COSTO TOTAL": costo_general
            })
    
    return filas_stats

def ejecutar_flujo_completo():
    """
    Ejecuta el flujo completo del sistema:
    1. Crear/actualizar proyección de indexadores
    2. Crear/actualizar proyección de precio SICEP
    3. Procesar ofertas
    4. Leer demanda
    5. Construir y resolver modelo de optimización
    6. Exportar resultados
    7. Generar visualizaciones
    
    Returns:
        bool: True si el proceso fue exitoso, False en caso contrario
    """
    try:
        # Paso 1: Verificar archivo de datos iniciales
        if not verificar_archivo_existe(DATOS_INICIALES):
            logger.error(f"No se encontró el archivo de datos iniciales: {DATOS_INICIALES}")
            print(f"ERROR: No se encontró el archivo de datos iniciales: {DATOS_INICIALES}")
            return False
        
        # Paso 2: Crear/actualizar proyección de indexadores
        print("\n=== PASO 1: CREAR PROYECCIÓN DE INDEXADORES ===")
        if not crear_proyeccion_indexadores(DATOS_INICIALES, OFERTAS_DIR):
            print("ERROR: No se pudo crear la proyección de indexadores")
            return False
        
        # Paso 3: Crear/actualizar proyección de precio SICEP
        print("\n=== PASO 2: CREAR PROYECCIÓN DE PRECIO SICEP ===")
        if not crear_proyeccion_precio_sicep(DATOS_INICIALES):
            print("ERROR: No se pudo crear la proyección de precio SICEP")
            return False
        
        # Paso 4: Procesar precio SICEP
        print("\n=== PASO 3: PROCESAR PRECIO SICEP ===")
        if procesar_precio_sicep(DATOS_INICIALES) is None:
            print("ERROR: No se pudo procesar el precio SICEP")
            return False
        
        # Paso 5: Procesar ofertas
        print("\n=== PASO 4: PROCESAR OFERTAS ===")
        if not procesar_ofertas(OFERTAS_DIR, DATOS_INICIALES, RESULTADO_OFERTAS):
            print("ERROR: No se pudieron procesar las ofertas")
            return False
        
        # Paso 6: Leer demanda
        print("\n=== PASO 5: LEER DEMANDA ===")
        demanda_df = leer_demanda(DATOS_INICIALES)
        if demanda_df is None:
            print("ERROR: No se pudo leer la demanda")
            return False
        
        # Paso 7: Leer ofertas para optimización
        print("\n=== PASO 6: LEER OFERTAS PARA OPTIMIZACIÓN ===")
        ofertas_df = leer_ofertas_evaluadas(RESULTADO_OFERTAS, solo_validas=True)
        
        # Leer TODAS las ofertas para resumen completo
        ofertas_df_completas = leer_ofertas_evaluadas(RESULTADO_OFERTAS, solo_validas=False)
        
        if ofertas_df.empty:
            print("ERROR: No hay ofertas válidas para optimización")
            return False
        
        # Paso 8: Construir modelo de optimización
        print("\n=== PASO 7: CONSTRUIR MODELO DE OPTIMIZACIÓN ===")
        model = construir_modelo(demanda_df, ofertas_df)
        
        # Paso 9: Resolver modelo de optimización
        print("\n=== PASO 8: RESOLVER MODELO DE OPTIMIZACIÓN ===")
        result = resolver_modelo(model)
        
        if result.solver.termination_condition != 'optimal':
            print(f"ADVERTENCIA: El solver terminó con condición: {result.solver.termination_condition}")
            print("Es posible que no se haya encontrado una solución óptima.")
        
        # Paso 10: Extraer resultados
        print("\n=== PASO 9: EXTRAER RESULTADOS ===")
        resultados_dict = extraer_resultados(model, ofertas_df_completas)
        
        # Calcular déficit total
        if "DEMANDA_FALTANTE" in resultados_dict:
            deficit_total = 0
            demanda_faltante_df = resultados_dict["DEMANDA_FALTANTE"]
            for _, row in demanda_faltante_df.iterrows():
                for hora in range(1, 25):
                    if hora in row:
                        deficit_total += row[hora]
            
            # Calcular demanda total directamente del modelo
            demanda_total = sum(pyo.value(model.D[a, h]) for a in model.A for h in model.H) # type: ignore
            
            if deficit_total > 0:
                porcentaje_deficit = (deficit_total / demanda_total) * 100 if demanda_total > 0 else 0
                print(f"ADVERTENCIA: Déficit total: {deficit_total:.2f} kWh ({porcentaje_deficit:.2f}% de la demanda)")
                print("Esto significa que las ofertas disponibles no son suficientes para cubrir toda la demanda.")
            else:
                print("ÉXITO: Toda la demanda fue cubierta con las ofertas")
        else:
            print("No se encontró información de déficit en los resultados.")
        
        # Paso 11: Exportar asignaciones
        print("\n=== PASO 10: EXPORTAR RESULTADOS ===")
        if not exportar_resultados_por_oferta(resultados_dict, RESULTADO_OFERTAS):
            print("ERROR: No se pudieron exportar los resultados")
            return False
        
        # Paso 12: Generar visualizaciones
        print("\n=== PASO 11: GENERAR VISUALIZACIONES ===")
        if VISUALIZACIONES_DISPONIBLES:
            try:
                if generar_reporte_completo(resultados_dict, ofertas_df, RESULTADO_OFERTAS):
                    print("✓ Visualizaciones generadas exitosamente")
                    print("📊 Se han generado:")
                    print("   - Gráficas de resumen anual")
                    print("   - Mapa de calor de asignaciones")
                    print("   - Estadísticas anuales en Excel")
                    print("   - Gráficas de distribución por oferta")
                else:
                    print("⚠ No se pudieron generar todas las visualizaciones")
            except Exception as e:
                print(f"⚠ Error al generar visualizaciones: {e}")
                logger.warning(f"Error en visualizaciones: {e}")
        else:
            print("⚠ Módulo de visualizaciones no disponible")
        
        # Paso 13: Calcular estadísticas corregidas
        print("\n=== PASO 12: CALCULAR ESTADÍSTICAS ===")
        try:
            filas_stats = calcular_estadisticas_correctas(resultados_dict)
            
            if filas_stats:
                stats_df = pd.DataFrame(filas_stats)
                stats_df.to_excel(ESTADISTICAS_OFERTAS, index=False)
                print(f"✓ Estadísticas guardadas en {ESTADISTICAS_OFERTAS}")
                
                # Mostrar resumen en consola
                total_energia = sum(s["TOTAL ASIGNADO (kWh)"] for s in filas_stats if s["TIPO"] == "OFERTA")
                total_costo = sum(s["COSTO TOTAL"] for s in filas_stats if s["TIPO"] == "OFERTA")
                precio_promedio = total_costo / total_energia if total_energia > 0 else 0
                
                print(f"📈 Resumen de estadísticas:")
                print(f"   - Energía total asignada: {total_energia:,.2f} kWh")
                print(f"   - Costo total: ${total_costo:,.2f}")
                print(f"   - Precio promedio ponderado: ${precio_promedio:.4f}/kWh")
                print(f"   - Ofertas utilizadas: {len([s for s in filas_stats if s['TIPO'] == 'OFERTA'])}")
            else:
                print("⚠ No hay suficientes datos para generar estadísticas")
        
        except Exception as e:
            logger.warning(f"No se pudieron calcular estadísticas: {e}")
            print(f"⚠ ADVERTENCIA: No se pudieron calcular estadísticas completas: {e}")
        
        print("\n🎉 === PROCESO COMPLETADO CON ÉXITO ===")
        return True
    
    except Exception as e:
        logger.exception(f"Error en el flujo de ejecución: {e}")
        print(f"❌ ERROR INESPERADO: {e}")
        return False

def mostrar_menu():
    """Muestra el menú principal de la aplicación."""
    print("\n===== SISTEMA DE OPTIMIZACIÓN ENERGÉTICA =====")
    print("1. Ejecutar flujo completo")
    print("2. Crear/actualizar proyección de indexadores")
    print("3. Crear/actualizar proyección de precio SICEP")
    print("4. Procesar ofertas (solo tabla maestra y precios)")
    print("5. Optimizar asignación de ofertas con Pyomo")
    print("6. Generar solo visualizaciones")  # Nueva opción
    print("7. Ver configuración actual")
    print("0. Salir")
    print("=============================================")
    return solicitar_input_seguro(
        "Seleccione una opción: ",
        tipo=int,
        validacion=lambda x: 0 <= x <= 7,
        mensaje_error="Opción inválida. Ingrese un número entre 0 y 7."
    )

def procesar_ofertas_solo_tabla():
    """
    Ejecuta solo la parte de procesamiento de ofertas para generar la tabla maestra
    y la hoja de cantidades y precios, sin pasar a la optimización.
    AHORA USA LA VERSIÓN OPTIMIZADA.
    
    Returns:
        bool: True si el proceso fue exitoso, False en caso contrario
    """
    try:
        # Verificar archivo de datos iniciales
        if not verificar_archivo_existe(DATOS_INICIALES):
            print(f"ERROR: No se encontró el archivo de datos iniciales: {DATOS_INICIALES}")
            return False
            
        # Procesar PRECIO SICEP
        print("\n=== PROCESANDO PRECIO SICEP ===")
        from core.ofertas_optimizado import procesar_precio_sicep_rapido
        sicep_dict = procesar_precio_sicep_rapido(DATOS_INICIALES)
        if sicep_dict is None:
            print("ERROR: No se pudo procesar el precio SICEP")
            return False
            
       # Procesar ofertas CON LA VERSIÓN OPTIMIZADA CORREGIDA
        print("\n=== PROCESANDO OFERTAS (OPTIMIZADO CORREGIDO) ===")
        from core.ofertas_optimizado import procesar_ofertas_optimizado_corregido
        
        if not procesar_ofertas_optimizado_corregido(OFERTAS_DIR, DATOS_INICIALES, RESULTADO_OFERTAS):
            print("ERROR: No se pudieron procesar las ofertas")
            return False
            
        print(f"\n✅ === PROCESO OPTIMIZADO COMPLETADO CON ÉXITO ===")
        print(f"Resultados guardados en: {RESULTADO_OFERTAS}")
        return True
        
    except Exception as e:
        logger.exception(f"Error en el procesamiento optimizado de ofertas: {e}")
        print(f"❌ ERROR INESPERADO: {e}")
        return False


def optimizar_con_pyomo():
    """
    Ejecuta el proceso de optimización con Pyomo y exporta los resultados
    en el formato específico requerido, incluyendo visualizaciones.
    
    Returns:
        bool: True si el proceso fue exitoso, False en caso contrario
    """
    try:
        # Leer demanda
        print("\n=== LEYENDO DATOS DE DEMANDA ===")
        demanda_df = leer_demanda(DATOS_INICIALES)
        if demanda_df is None:
            print("ERROR: No se pudo leer la demanda")
            return False
        
        # Leer ofertas para optimización
        print("\n=== LEYENDO OFERTAS EVALUADAS ===")
        ofertas_df = leer_ofertas_evaluadas(RESULTADO_OFERTAS, solo_validas=True)
        
        # Leer TODAS las ofertas para resumen completo
        ofertas_df_completas = leer_ofertas_evaluadas(RESULTADO_OFERTAS, solo_validas=False)
        
        if ofertas_df.empty:
            print("ERROR: No hay ofertas válidas para optimización")
            return False
        
        # Construir modelo de optimización
        print("\n=== CONSTRUYENDO MODELO DE OPTIMIZACIÓN ===")
        model = construir_modelo(demanda_df, ofertas_df)
        
        # Resolver modelo
        print("\n=== RESOLVIENDO MODELO DE OPTIMIZACIÓN ===")
        result = resolver_modelo(model)
        
        if result.solver.termination_condition != 'optimal':
            print(f"ADVERTENCIA: El solver terminó con condición: {result.solver.termination_condition}")
            print("Es posible que no se haya encontrado una solución óptima.")
        
        # Extraer resultados en formato mejorado
        print("\n=== EXTRAYENDO RESULTADOS ===")
        resultados_dict = extraer_resultados(model, ofertas_df_completas)
        
        # Exportar resultados en el formato específico
        print("\n=== EXPORTANDO RESULTADOS ===")
        if not exportar_resultados_por_oferta(resultados_dict, RESULTADO_OFERTAS):
            print("ERROR: No se pudieron exportar los resultados")
            return False
        
        # Generar visualizaciones
        print("\n=== GENERANDO VISUALIZACIONES ===")
        if VISUALIZACIONES_DISPONIBLES:
            try:
                if generar_reporte_completo(resultados_dict, ofertas_df, RESULTADO_OFERTAS):
                    print("✓ Visualizaciones generadas exitosamente")
                else:
                    print("⚠ No se pudieron generar todas las visualizaciones")
            except Exception as e:
                print(f"⚠ Error al generar visualizaciones: {e}")
        else:
            print("⚠ Módulo de visualizaciones no disponible")
        
        # Calcular estadísticas corregidas
        print("\n=== CALCULANDO ESTADÍSTICAS ===")
        try:
            filas_stats = calcular_estadisticas_correctas(resultados_dict)
            
            if filas_stats:
                stats_df = pd.DataFrame(filas_stats)
                stats_df.to_excel(ESTADISTICAS_OFERTAS, index=False)
                print(f"✓ Estadísticas guardadas en {ESTADISTICAS_OFERTAS}")
            else:
                print("⚠ No hay suficientes datos para generar estadísticas")
        
        except Exception as e:
            logger.warning(f"No se pudieron calcular estadísticas: {e}")
            print(f"⚠ ADVERTENCIA: No se pudieron calcular estadísticas completas: {e}")
        
        print("\n🎉 === PROCESO COMPLETADO CON ÉXITO ===")
        return True
        
    except Exception as e:
        logger.exception(f"Error en el proceso de optimización: {e}")
        print(f"❌ ERROR INESPERADO: {e}")
        return False

def generar_solo_visualizaciones():
    """
    Genera solo las visualizaciones basadas en resultados existentes.
    Lee los resultados desde el archivo Excel y genera todas las gráficas.
    """
    try:
        print("\n=== GENERANDO VISUALIZACIONES DESDE RESULTADOS EXISTENTES ===")
        
        if not VISUALIZACIONES_DISPONIBLES:
            print("❌ ERROR: Módulo de visualizaciones no disponible")
            return False
        
        # Verificar que existe el archivo de resultados
        if not verificar_archivo_existe(RESULTADO_OFERTAS):
            print(f"❌ ERROR: No se encontró el archivo de resultados: {RESULTADO_OFERTAS}")
            print("💡 Sugerencia: Ejecute primero la opción 5 (Optimizar asignación)")
            return False
        
        print(f"📁 Cargando desde: {RESULTADO_OFERTAS}")
        
        # Importar la función que acabamos de crear
        from core.evaluacion import cargar_resultados_desde_excel
        
        # Cargar los resultados desde Excel
        print("🔄 Cargando resultados desde archivo Excel...")
        resultados_dict = cargar_resultados_desde_excel(RESULTADO_OFERTAS)
        
        if not resultados_dict:
            print("❌ ERROR: No se pudieron cargar los resultados desde el archivo Excel")
            print("💡 Posibles causas:")
            print("   - El archivo no tiene el formato esperado")
            print("   - Faltan hojas necesarias (DA-*, ENA-*, etc.)")
            print("   - El archivo está corrupto o abierto en otra aplicación")
            return False
        
        print(f"✅ Resultados cargados exitosamente: {len(resultados_dict)} hojas de datos")
        
        # Leer ofertas evaluadas para compatibilidad con visualizaciones
        print("📋 Cargando datos de ofertas originales...")
        ofertas_df = leer_ofertas_evaluadas(RESULTADO_OFERTAS, solo_validas=False)
        
        if ofertas_df.empty:
            print("⚠️ ADVERTENCIA: No se encontraron ofertas evaluadas, continuando sin ellas")
            ofertas_df = pd.DataFrame()  # DataFrame vacío para compatibilidad
        else:
            print(f"✅ Ofertas cargadas: {len(ofertas_df)} registros")
        
        # Generar las visualizaciones usando el sistema existente
        print("\n🎨 Generando visualizaciones...")
        
        try:
            if generar_reporte_completo(resultados_dict, ofertas_df, RESULTADO_OFERTAS):
                print("\n🎉 ¡Visualizaciones generadas exitosamente!")
                print("📊 Se han generado:")
                print("   ✅ Gráfica principal de energía asignada")
                print("   ✅ Gráfica de resumen general")
                print("   ✅ Gráfica de torta de adjudicación")
                print("   ✅ Mapa de calor mensual")
                print("   ✅ Distribución por agente")
                print("   ✅ Reporte HTML consolidado")
                
                # Mostrar ubicación de los archivos
                from pathlib import Path
                output_dir = Path(RESULTADO_OFERTAS).parent / "visualizaciones"
                print(f"\n📁 Ubicación de las gráficas: {output_dir}")
                print("💡 Abre el archivo 'reporte_consolidado.html' para ver todas las gráficas")
                
                return True
            else:
                print("❌ ERROR: No se pudieron generar todas las visualizaciones")
                print("💡 Revisar logs para más detalles")
                return False
                
        except Exception as e:
            print(f"❌ ERROR al generar visualizaciones: {e}")
            logger.exception(f"Error en generación de visualizaciones: {e}")
            return False
        
    except Exception as e:
        logger.exception(f"Error general al generar visualizaciones: {e}")
        print(f"❌ ERROR INESPERADO: {e}")
        print("💡 Sugerencias:")
        print("   - Verificar que el archivo Excel no esté abierto en otra aplicación")
        print("   - Revisar que las hojas del archivo tengan el formato correcto")
        print("   - Intentar ejecutar la opción 5 nuevamente para generar resultados frescos")
        return False

def main():
    """Función principal que ejecuta la aplicación."""
    # Configurar parser de argumentos
    parser = argparse.ArgumentParser(description="Sistema de optimización energética con Pyomo")
    parser.add_argument(
        "--automatico", 
        action="store_true", 
        help="Ejecutar el flujo completo automáticamente sin mostrar menú"
    )
    parser.add_argument(
        "--solo-ofertas",
        action="store_true",
        help="Ejecutar solo el procesamiento de ofertas sin optimización"
    )
    parser.add_argument(
        "--solo-optimizacion",
        action="store_true",
        help="Ejecutar solo la optimización con Pyomo"
    )
    args = parser.parse_args()
    
    # Modo automático completo
    if args.automatico:
        return ejecutar_flujo_completo()
        
    # Modo solo ofertas
    if args.solo_ofertas:
        return procesar_ofertas_solo_tabla()
        
    # Modo solo optimización
    if args.solo_optimizacion:
        return optimizar_con_pyomo()
    
    # Modo interactivo
    while True:
        opcion = mostrar_menu()
        
        if opcion == 0:
            print("👋 Saliendo del sistema...")
            break
        elif opcion == 1:
            ejecutar_flujo_completo()
        elif opcion == 2:
            print("\n=== CREAR PROYECCIÓN DE INDEXADORES ===")
            crear_proyeccion_indexadores(DATOS_INICIALES, OFERTAS_DIR)
        elif opcion == 3:
            print("\n=== CREAR PROYECCIÓN DE PRECIO SICEP ===")
            crear_proyeccion_precio_sicep(DATOS_INICIALES)
        elif opcion == 4:
            print("\n=== PROCESAR OFERTAS (SOLO TABLA) ===")
            procesar_ofertas_solo_tabla()
        elif opcion == 5:
            print("\n=== OPTIMIZAR ASIGNACIÓN DE OFERTAS CON PYOMO ===")
            optimizar_con_pyomo()
        elif opcion == 6:
            print("\n=== GENERAR SOLO VISUALIZACIONES ===")
            generar_solo_visualizaciones()
        elif opcion == 7:
            print("\n=== CONFIGURACIÓN ACTUAL ===")
            print(f"📁 Archivo de datos iniciales: {DATOS_INICIALES}")
            print(f"📁 Carpeta de ofertas: {OFERTAS_DIR}")
            print(f"📁 Archivo de resultados: {RESULTADO_OFERTAS}")
            print(f"📁 Archivo de estadísticas: {ESTADISTICAS_OFERTAS}")
            print(f"📊 Visualizaciones disponibles: {'✅' if VISUALIZACIONES_DISPONIBLES else '❌'}")

    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)