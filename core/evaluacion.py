"""
Módulo para evaluación de ofertas y preparación para optimización.
Incluye funciones para evaluar ofertas y preparar datos para el modelo de optimización.
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def evaluar_ofertas_para_optimizacion(ofertas_df):
    """
    Filtra y procesa las ofertas para prepararlas para el proceso de optimización.
    
    Args:
        ofertas_df (DataFrame): DataFrame con las ofertas evaluadas
        
    Returns:
        DataFrame: DataFrame con las ofertas aptas para optimización
    """
    if ofertas_df.empty:
        logger.warning("No hay ofertas para evaluar")
        return pd.DataFrame()
    
    # Verificar que las columnas necesarias existen
    columnas_requeridas = ['CÓDIGO OFERTA', 'FECHA', 'Atributo', 'CANTIDAD', 
                         'PRECIO INDEXADO', 'EVALUACIÓN']
    
    if not all(col in ofertas_df.columns for col in columnas_requeridas):
        logger.error("El DataFrame de ofertas no contiene todas las columnas requeridas")
        return pd.DataFrame()
    
    # Filtrar ofertas que cumplen los criterios
    # Suponiendo que EVALUACIÓN es 1 para ofertas que cumplen
    ofertas_filtradas = ofertas_df[ofertas_df['EVALUACIÓN'] == 1].copy()
    
    # Verificar que haya ofertas después del filtrado
    if ofertas_filtradas.empty:
        logger.warning("No hay ofertas que cumplan los criterios de evaluación")
        return pd.DataFrame()
    
    # Filtrar solo las columnas necesarias para la optimización
    cols_optimizacion = ['CÓDIGO OFERTA', 'FECHA', 'Atributo', 'CANTIDAD', 'PRECIO INDEXADO']
    ofertas_optimizacion = ofertas_filtradas[cols_optimizacion].copy()
    
    # Verificar si hay NaN o valores nulos
    if ofertas_optimizacion.isnull().any().any():
        logger.warning("Hay valores nulos en las ofertas filtradas para optimización")
        # Puedes decidir si completar los nulos o eliminar esas filas
    
    logger.info(f"Se han preparado {len(ofertas_optimizacion)} ofertas para optimización")
    return ofertas_optimizacion

def calcular_estadisticas_ofertas(ofertas_df):
    """
    Calcula estadísticas de las ofertas procesadas.
    
    Args:
        ofertas_df (DataFrame): DataFrame con las ofertas evaluadas
        
    Returns:
        DataFrame: DataFrame con estadísticas calculadas
    """
    if ofertas_df.empty:
        logger.warning("No hay ofertas para calcular estadísticas")
        return pd.DataFrame()
    
    stats = []
    
    # Estadísticas por oferta
    for oferta in ofertas_df["CÓDIGO OFERTA"].unique():
        df_of = ofertas_df[ofertas_df["CÓDIGO OFERTA"] == oferta]
        total_asignado = df_of["CANTIDAD"].sum()
        precio_promedio = 0
        
        # Calcular precio promedio ponderado si hay precios indexados
        if "PRECIO INDEXADO" in df_of.columns and not df_of["PRECIO INDEXADO"].isnull().all():
            precio_promedio = (df_of["CANTIDAD"] * df_of["PRECIO INDEXADO"]).sum() / total_asignado if total_asignado > 0 else 0
        
        stats.append({
            "TIPO": "OFERTA",
            "IDENTIFICADOR": oferta,
            "TOTAL ASIGNADO (kWh)": total_asignado,
            "PRECIO PROMEDIO": precio_promedio,
            "COSTO TOTAL": total_asignado * precio_promedio
        })
    
    # Estadísticas por fecha si existe la columna FECHA
    if "FECHA" in ofertas_df.columns:
        for fecha in ofertas_df["FECHA"].unique():
            df_fecha = ofertas_df[ofertas_df["FECHA"] == fecha]
            total_cantidad = df_fecha["CANTIDAD"].sum()
            
            stats.append({
                "TIPO": "FECHA",
                "IDENTIFICADOR": fecha,
                "TOTAL (kWh)": total_cantidad,
            })
    
    logger.info("Estadísticas calculadas correctamente")
    return pd.DataFrame(stats)

def exportar_asignaciones_por_oferta(asignaciones_df, output_file):
    """
    Exporta las asignaciones por oferta en formato Excel.
    
    Args:
        asignaciones_df (DataFrame): DataFrame con las asignaciones a exportar
        output_file (str o Path): Ruta del archivo de salida
        
    Returns:
        bool: True si la exportación fue exitosa, False en caso contrario
    """
    if asignaciones_df.empty:
        logger.warning("No hay asignaciones para exportar")
        return False
    
    try:
        # Crear una copia del dataframe para no modificar el original
        df = asignaciones_df.copy()
        
        # Filtrar solo las filas con asignaciones (eliminar filas sin asignación)
        if "CÓDIGO OFERTA" in df.columns:
            df = df[df["CÓDIGO OFERTA"] != "SIN ASIGNACIÓN"]
        
        if df.empty:
            logger.warning("No hay asignaciones válidas para exportar")
            return False
        
        # Usar ExcelWriter para crear/modificar el archivo
        with pd.ExcelWriter(output_file, engine="openpyxl", mode="a", 
                          if_sheet_exists="replace") as writer:
            # Para cada oferta, crear una hoja
            for oferta in df["CÓDIGO OFERTA"].unique():
                for it in range(1, 3):  # Iteraciones (IT1, IT2)
                    df_oferta = df[df["CÓDIGO OFERTA"] == oferta].copy()
                    
                    # Pivotar los datos para tener fechas en filas y horas en columnas
                    pivot_df = df_oferta.pivot_table(
                        index="FECHA", 
                        columns="Atributo", 
                        values="CANTIDAD",
                        fill_value=0
                    )
                    
                    # Asegurar que tenemos todas las columnas de 1 a 24
                    for hora in range(1, 25):
                        if hora not in pivot_df.columns:
                            pivot_df[hora] = 0
                    
                    # Ordenar las columnas
                    pivot_df = pivot_df.reindex(columns=range(1, 25))
                    
                    # Ordenar por fecha
                    pivot_df = pivot_df.sort_index()
                    
                    # Crear el nombre de la hoja
                    sheet_name = f"DEMANDA ASIGNADA {oferta} IT{it}"
                    if len(sheet_name) > 31:  # Excel limita nombres de hojas a 31 caracteres
                        sheet_name = sheet_name[:31]
                    
                    # Exportar a Excel
                    pivot_df.to_excel(writer, sheet_name=sheet_name)
                    logger.info(f"Hoja '{sheet_name}' creada en el archivo '{output_file}'")
        
        logger.info(f"Asignaciones exportadas correctamente a {output_file}")
        return True
    except Exception as e:
        logger.error(f"Error al exportar asignaciones: {e}")
        return False

def crear_hoja_demanda_faltante(asignaciones_df, output_file):
    """
    Crea una hoja en el archivo de salida para registrar la demanda faltante.
    
    Args:
        asignaciones_df (DataFrame): DataFrame con las asignaciones
        output_file (str o Path): Ruta del archivo de salida
        
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    if asignaciones_df.empty:
        logger.warning("No hay asignaciones para procesar")
        return False
    
    try:
        # Filtrar solo las filas con déficit
        if "DÉFICIT" in asignaciones_df.columns:
            df_faltante = asignaciones_df[asignaciones_df["DÉFICIT"] > 0]
        else:
            logger.warning("No hay columna DÉFICIT en las asignaciones")
            return False
        
        if df_faltante.empty:
            logger.info("No hay demanda faltante para reportar")
            return True
        
        # Guardar en Excel
        with pd.ExcelWriter(output_file, engine="openpyxl", mode="a", 
                          if_sheet_exists="replace") as writer:
            df_faltante.to_excel(writer, sheet_name="DEMANDA FALTANTE", index=False)
            logger.info(f"Hoja 'DEMANDA FALTANTE' creada en el archivo '{output_file}'")
        
        return True
    except Exception as e:
        logger.error(f"Error al crear hoja de demanda faltante: {e}")
        return False

def leer_ofertas_evaluadas(archivo_ofertas, sheet_name="CANTIDADES Y PRECIOS"):
    """
    Lee las ofertas evaluadas desde un archivo Excel.
    
    Args:
        archivo_ofertas (str o Path): Ruta al archivo Excel con ofertas
        sheet_name (str): Nombre de la hoja a leer
        
    Returns:
        DataFrame: DataFrame con las ofertas evaluadas
    """
    try:
        # Leer el archivo Excel
        xls = pd.ExcelFile(archivo_ofertas)
        if sheet_name not in xls.sheet_names:
            logger.error(f"No se encontró la hoja {sheet_name} en {archivo_ofertas}")
            return pd.DataFrame()
        
        df = pd.read_excel(xls, sheet_name=sheet_name)
        
        # Verificar que tengamos datos
        if df.empty:
            logger.warning(f"No hay datos en la hoja {sheet_name} de {archivo_ofertas}")
            return pd.DataFrame()
        
        # Convertir tipos de datos
        if "FECHA" in df.columns:
            df['FECHA'] = pd.to_datetime(df['FECHA'], format="%d/%m/%Y").dt.date
        
        if "Atributo" in df.columns:
            df['Atributo'] = df['Atributo'].astype(int)
        
        if "CANTIDAD" in df.columns:
            df['CANTIDAD'] = pd.to_numeric(df['CANTIDAD'], errors='coerce')
        
        if "PRECIO INDEXADO" in df.columns:
            df['PRECIO INDEXADO'] = pd.to_numeric(df['PRECIO INDEXADO'], errors='coerce')
        
        # Filtrar ofertas válidas
        if "PRECIO INDEXADO" in df.columns and "CANTIDAD" in df.columns:
            df = df.dropna(subset=['PRECIO INDEXADO'])
            df = df[df['CANTIDAD'] > 0]
        
        # Filtrar ofertas que cumplen evaluación si existe esa columna
        if "EVALUACIÓN" in df.columns:
            df = df[df['EVALUACIÓN'] == 1]  # Suponiendo que 1 = cumple
        
        logger.info(f"Se leyeron {len(df)} ofertas evaluadas de {archivo_ofertas}")
        return df
    except Exception as e:
        logger.error(f"Error al leer ofertas evaluadas: {e}")
        return pd.DataFrame()