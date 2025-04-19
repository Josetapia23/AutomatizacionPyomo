"""
Módulo para evaluación de ofertas y preparación para optimización.
Incluye funciones para evaluar ofertas y preparar datos para el modelo de optimización.
"""

import pandas as pd
import logging
from pathlib import Path
from core.utils import verificar_archivo_existe, leer_excel_seguro

logger = logging.getLogger(__name__)

def evaluar_ofertas_para_optimizacion(archivo_ofertas):
    """
    Lee el archivo de ofertas y prepara los datos para la optimización.
    
    Args:
        archivo_ofertas (Path): Ruta al archivo Excel con las ofertas procesadas
        
    Returns:
        DataFrame: DataFrame con las ofertas evaluadas, o None en caso de error
    """
    logger.info(f"Evaluando ofertas para optimización desde {archivo_ofertas}")
    
    # Usar la función existente para leer el archivo
    ofertas_df = leer_ofertas_evaluadas(archivo_ofertas)
    
    if ofertas_df.empty:
        logger.warning("No hay ofertas válidas para optimización")
        return None
        
    return ofertas_df

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
        if oferta == "SIN ASIGNACIÓN":
            continue
            
        df_of = ofertas_df[ofertas_df["CÓDIGO OFERTA"] == oferta]
        
        # Determinar la columna que contiene la asignación (podría ser CANTIDAD o ENERGÍA ASIGNADA)
        if "ENERGÍA ASIGNADA" in df_of.columns:
            total_asignado = df_of["ENERGÍA ASIGNADA"].sum()
            precio_columna = "PRECIO"
        else:
            total_asignado = df_of["CANTIDAD"].sum()
            precio_columna = "PRECIO INDEXADO"
        
        precio_promedio = 0
        
        # Calcular precio promedio ponderado
        if precio_columna in df_of.columns and not df_of[precio_columna].isnull().all():
            precio_ponderado_sum = 0
            asignacion_sum = 0
            
            for _, row in df_of.iterrows():
                precio = row.get(precio_columna, 0)
                asignacion = row.get("ENERGÍA ASIGNADA" if "ENERGÍA ASIGNADA" in df_of.columns else "CANTIDAD", 0)
                
                if pd.notna(precio) and pd.notna(asignacion) and asignacion > 0:
                    precio_ponderado_sum += precio * asignacion
                    asignacion_sum += asignacion
            
            precio_promedio = precio_ponderado_sum / asignacion_sum if asignacion_sum > 0 else 0
        
        stats.append({
            "TIPO": "OFERTA",
            "IDENTIFICADOR": oferta,
            "TOTAL ASIGNADO (kWh)": total_asignado,
            "PRECIO PROMEDIO": precio_promedio,
            "COSTO TOTAL": total_asignado * precio_promedio
        })
    
    # Estadísticas generales
    if stats:
        total_general = sum(s["TOTAL ASIGNADO (kWh)"] for s in stats)
        costo_general = sum(s["COSTO TOTAL"] for s in stats)
        precio_promedio_general = costo_general / total_general if total_general > 0 else 0
        
        stats.append({
            "TIPO": "TOTAL",
            "IDENTIFICADOR": "TODAS LAS OFERTAS",
            "TOTAL ASIGNADO (kWh)": total_general,
            "PRECIO PROMEDIO": precio_promedio_general,
            "COSTO TOTAL": costo_general
        })
    
    # Estadísticas por fecha si existe la columna FECHA
    if "FECHA" in ofertas_df.columns:
        for fecha in ofertas_df["FECHA"].unique():
            df_fecha = ofertas_df[ofertas_df["FECHA"] == fecha]
            
            if "ENERGÍA ASIGNADA" in df_fecha.columns:
                total_cantidad = df_fecha[df_fecha["CÓDIGO OFERTA"] != "SIN ASIGNACIÓN"]["ENERGÍA ASIGNADA"].sum()
                deficit = df_fecha["DÉFICIT"].sum() if "DÉFICIT" in df_fecha.columns else 0
                demanda = df_fecha["DEMANDA TOTAL"].sum() if "DEMANDA TOTAL" in df_fecha.columns else total_cantidad
            else:
                total_cantidad = df_fecha["CANTIDAD"].sum()
                deficit = 0
                demanda = total_cantidad
            
            stats.append({
                "TIPO": "FECHA",
                "IDENTIFICADOR": fecha,
                "TOTAL ASIGNADO (kWh)": total_cantidad,
                "DEMANDA (kWh)": demanda,
                "DÉFICIT (kWh)": deficit,
                "COBERTURA (%)": (total_cantidad / demanda * 100) if demanda > 0 else 0
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
        
        # Determinar las columnas para usar en el pivot
        hora_col = "HORA" if "HORA" in df.columns else "Atributo"
        valor_col = "ENERGÍA ASIGNADA" if "ENERGÍA ASIGNADA" in df.columns else "CANTIDAD"
        
        # Usar ExcelWriter para crear/modificar el archivo
        with pd.ExcelWriter(output_file, engine="openpyxl", mode="a", 
                          if_sheet_exists="replace") as writer:
            # Para cada oferta, crear una hoja
            for oferta in df["CÓDIGO OFERTA"].unique():
                df_oferta = df[df["CÓDIGO OFERTA"] == oferta].copy()
                
                # Pivotar los datos para tener fechas en filas y horas en columnas
                pivot_df = df_oferta.pivot_table(
                    index="FECHA", 
                    columns=hora_col, 
                    values=valor_col,
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
                sheet_name = f"DEMANDA ASIGNADA {oferta}"
                if len(sheet_name) > 31:  # Excel limita nombres de hojas a 31 caracteres
                    sheet_name = sheet_name[:31]
                
                # Exportar a Excel
                pivot_df.to_excel(writer, sheet_name=sheet_name)
                logger.info(f"Hoja '{sheet_name}' creada en el archivo '{output_file}'")
            
            # Exportar también la tabla de asignaciones completa
            asignaciones_df.to_excel(writer, sheet_name="ASIGNACIONES", index=False)
        
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
            
            # Crear mensaje de éxito
            mensaje_df = pd.DataFrame({
                "MENSAJE": ["No hay demanda faltante. Toda la demanda fue satisfecha."]
            })
            
            # Guardar en Excel
            with pd.ExcelWriter(output_file, engine="openpyxl", mode="a", 
                              if_sheet_exists="replace") as writer:
                mensaje_df.to_excel(writer, sheet_name="DEMANDA FALTANTE", index=False)
                logger.info(f"Hoja 'DEMANDA FALTANTE' creada en el archivo '{output_file}'")
            
            return True
        
        # Calcular porcentaje de déficit
        if "DEMANDA TOTAL" in df_faltante.columns:
            df_faltante["PORCENTAJE DÉFICIT"] = df_faltante.apply(
                lambda row: (row["DÉFICIT"] / row["DEMANDA TOTAL"] * 100) if row["DEMANDA TOTAL"] > 0 else 0,
                axis=1
            )
        
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
        # Verificar que el archivo existe
        if not verificar_archivo_existe(archivo_ofertas):
            logger.error(f"No se encontró el archivo de ofertas: {archivo_ofertas}")
            return pd.DataFrame()
            
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
            df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce').dt.date
        
        if "Atributo" in df.columns:
            df['Atributo'] = df['Atributo'].astype(int)
        
        if "CANTIDAD" in df.columns:
            df['CANTIDAD'] = pd.to_numeric(df['CANTIDAD'], errors='coerce')
        
        if "PRECIO INDEXADO" in df.columns:
            df['PRECIO INDEXADO'] = pd.to_numeric(df['PRECIO INDEXADO'], errors='coerce')
        
        # Filtrar ofertas válidas
        if "PRECIO INDEXADO" in df.columns and "CANTIDAD" in df.columns:
            df_filtrada = df.dropna(subset=['PRECIO INDEXADO'])
            df_filtrada = df_filtrada[df_filtrada['CANTIDAD'] > 0]
        
            # Filtrar ofertas que cumplen evaluación si existe esa columna
            if "EVALUACIÓN" in df_filtrada.columns:
                df_filtrada = df_filtrada[df_filtrada['EVALUACIÓN'] == 1]  # Suponiendo que 1 = cumple
            
            logger.info(f"Se leyeron {len(df)} ofertas, de las cuales {len(df_filtrada)} son válidas para optimización")
            print(f"Se leyeron {len(df)} ofertas, de las cuales {len(df_filtrada)} son válidas para optimización")
            
            return df_filtrada
        else:
            return df
    except Exception as e:
        logger.error(f"Error al leer ofertas evaluadas: {e}")
        print(f"Error al leer ofertas evaluadas: {e}")
        return pd.DataFrame()

def exportar_resultados_por_oferta(resultados_dict, archivo_salida):
    """
    Exporta los resultados de la optimización al formato específico requerido,
    preservando las hojas existentes como TABLA MAESTRA y CANTIDADES Y PRECIOS.
    
    Args:
        resultados_dict (dict): Diccionario con los DataFrames de resultados
        archivo_salida (str o Path): Ruta donde se guardará el archivo Excel
        
    Returns:
        bool: True si la exportación fue exitosa, False en caso contrario
    """
    archivo_salida = Path(archivo_salida)
    logger.info(f"Exportando resultados al archivo: {archivo_salida}")
    print(f"Exportando resultados al archivo: {archivo_salida}")
    
    # Crear directorio si no existe
    archivo_salida.parent.mkdir(parents=True, exist_ok=True)
    
    # Verificar si el archivo existe
    archivo_existe = archivo_salida.exists()
    
    try:
        # Usar modo 'a' (append) si el archivo existe, 'w' (write) si no existe
        modo = 'a' if archivo_existe else 'w'
        
        with pd.ExcelWriter(archivo_salida, engine='openpyxl', mode=modo, if_sheet_exists='replace') as writer:
            # 1. Exportar hojas de DEMANDA ASIGNADA por oferta
            # Agrupar resultados por hoja
            hojas_por_nombre = {}
            
            for nombre_hoja, df in resultados_dict.items():
                if "_COMPRAR" in nombre_hoja or "_NO_COMPRADA" in nombre_hoja:
                    # Extraer nombre base de la hoja
                    nombre_base = nombre_hoja.split("_COMPRAR")[0] if "_COMPRAR" in nombre_hoja else nombre_hoja.split("_NO_COMPRADA")[0]
                    tipo = "COMPRAR" if "_COMPRAR" in nombre_hoja else "NO_COMPRADA"
                    
                    if nombre_base not in hojas_por_nombre:
                        hojas_por_nombre[nombre_base] = {}
                    
                    hojas_por_nombre[nombre_base][tipo] = df
            
            # Para cada nombre de hoja, crear la hoja con ambos cuadros
            for nombre_hoja, datos in hojas_por_nombre.items():
                df_comprar = datos.get("COMPRAR", pd.DataFrame())
                df_no_comprada = datos.get("NO_COMPRADA", pd.DataFrame())
                
                if not df_comprar.empty and not df_no_comprada.empty:
                    # Mantener el orden cronológico original sin ordenar por X
                    df_comprar_ordenado = df_comprar.copy()
                    df_no_comprada_ordenado = df_no_comprada.copy()
                    
                    # Convertir fechas a formato string DD/MM/YYYY manteniendo el orden original
                    df_comprar_ordenado["X"] = df_comprar_ordenado["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                    df_no_comprada_ordenado["X"] = df_no_comprada_ordenado["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                    
                    # Eliminar columna FECHA (mantener sólo X)
                    df_comprar_ordenado = df_comprar_ordenado.drop(columns=["FECHA"])
                    df_no_comprada_ordenado = df_no_comprada_ordenado.drop(columns=["FECHA"])
                    
                    # Crear un nuevo DataFrame con ambos cuadros
                    # Primero añadir un título para el primer cuadro
                    titulo_comprar = pd.DataFrame({
                        "X": ["ENERGÍA A COMPRAR AL VENDEDOR"],
                        **{i: [None] for i in range(1, 25)}  # Columnas del 1 al 24
                    })
                    
                    # Añadir algunas filas en blanco entre los cuadros
                    filas_blanco = pd.DataFrame({
                        "X": [None, None],
                        **{i: [None, None] for i in range(1, 25)}  # Columnas del 1 al 24
                    })
                    
                    # Título para el segundo cuadro
                    titulo_no_comprada = pd.DataFrame({
                        "X": ["ENERGÍA NO COMPRADA AL VENDEDOR"],
                        **{i: [None] for i in range(1, 25)}  # Columnas del 1 al 24
                    })
                    
                    # Concatenar todo
                    df_final = pd.concat([
                        titulo_comprar, 
                        df_comprar_ordenado, 
                        filas_blanco, 
                        titulo_no_comprada, 
                        df_no_comprada_ordenado
                    ], ignore_index=True)
                    
                    # Asegurar que el nombre de la hoja no exceda los 31 caracteres
                    sheet_name = nombre_hoja
                    if len(sheet_name) > 31:
                        sheet_name = sheet_name[:31]
                    
                    # Exportar sin el índice
                    df_final.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(f"Hoja exportada: {sheet_name}")
            
            # 2. Exportar hoja de DEMANDA FALTANTE
            if "DEMANDA_FALTANTE" in resultados_dict:
                df_export = resultados_dict["DEMANDA_FALTANTE"].copy()
                
                # Mantener el orden cronológico original
                # Convertir fechas a formato string DD/MM/YYYY sin ordenar
                df_export["X"] = df_export["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                df_export = df_export.drop(columns=["FECHA"])
                
                # Añadir un título a la hoja DEMANDA FALTANTE
                titulo_faltante = pd.DataFrame({
                    "X": ["DEMANDA FALTANTE POR HORA Y DÍA"],
                    **{i: [None] for i in range(1, 25)}  # Columnas del 1 al 24
                })
                
                # Concatenar título y datos
                df_final = pd.concat([titulo_faltante, df_export], ignore_index=True)
                
                df_final.to_excel(writer, sheet_name="DEMANDA FALTANTE", index=False)
                logger.info(f"Hoja exportada: DEMANDA FALTANTE")
            
            # 3. Exportar hoja de RESUMEN
            if "RESUMEN" in resultados_dict:
                df_export = resultados_dict["RESUMEN"].copy()
                
                # El formato de fecha ya está establecido como MM/YYYY en extraer_resultados
                # No reordenar, preservar el orden original
                
                # Crear títulos dinámicamente según las columnas disponibles
                titulos = {}
                titulos["FECHA"] = ""
                for col in df_export.columns:
                    if col != "FECHA":
                        titulos[col] = "CANTIDAD"
                
                # Añadir la fila de títulos
                titulo_df = pd.DataFrame([titulos])
                
                # Concatenar título y datos
                df_final = pd.concat([titulo_df, df_export], ignore_index=True)
                
                df_final.to_excel(writer, sheet_name="RESUMEN", index=False)
                logger.info(f"Hoja exportada: RESUMEN")
        
        print(f"Resultados exportados exitosamente a: {archivo_salida}")
        return True
    
    except Exception as e:
        logger.exception(f"Error al exportar resultados: {e}")
        print(f"ERROR: No se pudieron exportar los resultados: {e}")
        
        try:
            # Intentar con un archivo nuevo
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nueva_ruta = archivo_salida.parent / f"{archivo_salida.stem}_nuevo_{timestamp}{archivo_salida.suffix}"
            
            print(f"Intentando crear un archivo nuevo en: {nueva_ruta}")
            
            with pd.ExcelWriter(nueva_ruta, engine='openpyxl') as writer:
                # Código similar al anterior para las hojas
                for nombre_hoja, datos in hojas_por_nombre.items():
                    df_comprar = datos.get("COMPRAR", pd.DataFrame())
                    df_no_comprada = datos.get("NO_COMPRADA", pd.DataFrame())
                    
                    if not df_comprar.empty and not df_no_comprada.empty:
                        # Mantener el orden cronológico
                        df_comprar_ordenado = df_comprar.copy()
                        df_no_comprada_ordenado = df_no_comprada.copy()
                        
                        # Convertir fechas a formato string DD/MM/YYYY
                        df_comprar_ordenado["X"] = df_comprar_ordenado["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                        df_no_comprada_ordenado["X"] = df_no_comprada_ordenado["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                        
                        # Eliminar columna FECHA (mantener sólo X)
                        df_comprar_ordenado = df_comprar_ordenado.drop(columns=["FECHA"])
                        df_no_comprada_ordenado = df_no_comprada_ordenado.drop(columns=["FECHA"])
                        
                        # Resto del procesamiento...
                        titulo_comprar = pd.DataFrame({
                            "X": ["ENERGÍA A COMPRAR AL VENDEDOR"],
                            **{i: [None] for i in range(1, 25)}
                        })
                        
                        filas_blanco = pd.DataFrame({
                            "X": [None, None],
                            **{i: [None, None] for i in range(1, 25)}
                        })
                        
                        titulo_no_comprada = pd.DataFrame({
                            "X": ["ENERGÍA NO COMPRADA AL VENDEDOR"],
                            **{i: [None] for i in range(1, 25)}
                        })
                        
                        df_final = pd.concat([
                            titulo_comprar, 
                            df_comprar_ordenado, 
                            filas_blanco, 
                            titulo_no_comprada, 
                            df_no_comprada_ordenado
                        ], ignore_index=True)
                        
                        # Asegurar que el nombre de la hoja no exceda los 31 caracteres
                        sheet_name = nombre_hoja
                        if len(sheet_name) > 31:
                            sheet_name = sheet_name[:31]
                        
                        df_final.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Exportar demanda faltante con título
                if "DEMANDA_FALTANTE" in resultados_dict:
                    df_export = resultados_dict["DEMANDA_FALTANTE"].copy()
                    df_export["X"] = df_export["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                    df_export = df_export.drop(columns=["FECHA"])
                    
                    # Añadir un título a la hoja DEMANDA FALTANTE
                    titulo_faltante = pd.DataFrame({
                        "X": ["DEMANDA FALTANTE POR HORA Y DÍA"],
                        **{i: [None] for i in range(1, 25)}
                    })
                    
                    # Concatenar título y datos
                    df_final = pd.concat([titulo_faltante, df_export], ignore_index=True)
                    
                    df_final.to_excel(writer, sheet_name="DEMANDA FALTANTE", index=False)
                
                # Exportar resumen
                if "RESUMEN" in resultados_dict:
                    df_export = resultados_dict["RESUMEN"].copy()
                    
                    # Crear títulos dinámicamente según las columnas disponibles
                    titulos = {}
                    titulos["FECHA"] = ""
                    for col in df_export.columns:
                        if col != "FECHA":
                            titulos[col] = "CANTIDAD"
                    
                    # Añadir la fila de títulos
                    titulo_df = pd.DataFrame([titulos])
                    
                    # Concatenar título y datos
                    df_final = pd.concat([titulo_df, df_export], ignore_index=True)
                    
                    df_final.to_excel(writer, sheet_name="RESUMEN", index=False)
            
            print(f"Resultados exportados a un nuevo archivo: {nueva_ruta}")
            return True
            
        except Exception as alt_e:
            logger.exception(f"Error al crear archivo alternativo: {alt_e}")
            print(f"ERROR: No se pudo crear archivo alternativo: {alt_e}")
            return False
        