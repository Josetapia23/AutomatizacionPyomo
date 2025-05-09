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
    Exporta los resultados de la optimización al formato específico requerido.
    Consolida todas las iteraciones en una sola hoja por oferta.
    También crea un archivo secundario con todas las iteraciones para análisis.
    
    Args:
        resultados_dict (dict): Diccionario con los DataFrames de resultados
        archivo_salida (str o Path): Ruta donde se guardará el archivo Excel
        
    Returns:
        bool: True si la exportación fue exitosa, False en caso contrario
    """
    archivo_salida = Path(archivo_salida)
    logger.info(f"Exportando resultados al archivo: {archivo_salida}")
    print(f"Exportando resultados al archivo: {archivo_salida}")
    
    # También crear un archivo secundario para análisis detallado
    archivo_analisis = archivo_salida.parent / f"{archivo_salida.stem}_analisis{archivo_salida.suffix}"
    print(f"Creando archivo de análisis detallado: {archivo_analisis}")
    
    # Crear directorios si no existen
    archivo_salida.parent.mkdir(parents=True, exist_ok=True)
    
    # Verificar si el archivo existe
    archivo_existe = archivo_salida.exists()
    
    try:
        # Primero, leer los datos originales de las ofertas para obtener las cantidades totales
        ofertas_originales = {}
        ofertas_rechazadas_por_precio = {}  # Para almacenar ofertas que no cumplieron evaluación
        
        try:
            # Leer la hoja CANTIDADES Y PRECIOS para obtener información de ofertas originales
            ofertas_df = pd.read_excel(archivo_salida, sheet_name="CANTIDADES Y PRECIOS")
            if not ofertas_df.empty:
                for idx, row in ofertas_df.iterrows():
                    oferta = row.get('CÓDIGO OFERTA', '')
                    fecha = row.get('FECHA', None)
                    hora = row.get('Atributo', None)
                    cantidad = row.get('CANTIDAD', 0)
                    evaluacion = row.get('EVALUACIÓN', 0)
                    precio = row.get('PRECIO INDEXADO', 0)
                    
                    if oferta and fecha and hora is not None:
                        clave = (oferta, fecha, hora)
                        ofertas_originales[clave] = {
                            'cantidad': cantidad,
                            'evaluacion': evaluacion,
                            'precio': precio
                        }
                        
                        # Si la evaluación es 0, guardar para reporte de rechazadas
                        if evaluacion == 0:
                            if oferta not in ofertas_rechazadas_por_precio:
                                ofertas_rechazadas_por_precio[oferta] = []
                            
                            ofertas_rechazadas_por_precio[oferta].append({
                                'FECHA': fecha,
                                'HORA': hora,
                                'CANTIDAD': cantidad,
                                'PRECIO': precio
                            })
            
            if ofertas_originales:
                print(f"Información de {len(ofertas_originales)} registros originales cargada correctamente")
                
                # Mostrar cuántas ofertas no cumplieron evaluación
                rechazadas_count = sum(len(items) for items in ofertas_rechazadas_por_precio.values())
                if rechazadas_count > 0:
                    print(f"Se encontraron {rechazadas_count} registros que no cumplieron la evaluación de precio")
            else:
                print("No se encontró información de ofertas originales")
        except Exception as e:
            logger.warning(f"No se pudo leer información original de ofertas: {e}")
            print(f"No se pudo leer información original de ofertas: {e}")
        
        # 1. ARCHIVO PRINCIPAL PARA CLIENTE (CONSOLIDADO)
        # Usar modo 'a' (append) si el archivo existe, 'w' (write) si no existe
        modo = 'a' if archivo_existe else 'w'
        
        with pd.ExcelWriter(archivo_salida, engine='openpyxl', mode=modo, if_sheet_exists='replace') as writer:
            # Identificar todas las ofertas únicas
            ofertas_unicas = set()
            for key in resultados_dict.keys():
                if "_COMPRAR" in key:
                    # Extraer nombre de la oferta (sin "DEMANDA ASIGNADA" y sin "IT#_COMPRAR")
                    nombre_oferta = key.split("DEMANDA ASIGNADA ")[1].split(" IT")[0]
                    ofertas_unicas.add(nombre_oferta)
            
            # Para cada oferta, consolidar todas las iteraciones
            for oferta in ofertas_unicas:
                # Consolidar datos de compras
                df_comprar_consolidado = pd.DataFrame()
                
                # Para la energía no comprada, usar solo la última iteración
                df_no_comprado_consolidado = None
                ultima_iteracion = 0
                
                # Buscar todas las iteraciones para esta oferta
                for key in resultados_dict.keys():
                    if f"DEMANDA ASIGNADA {oferta}" in key and "_COMPRAR" in key:
                        df_iter = resultados_dict[key].copy()
                        
                        # Extraer número de iteración
                        try:
                            it_num = int(key.split("IT")[1].split("_")[0])
                            ultima_iteracion = max(ultima_iteracion, it_num)
                        except:
                            logger.warning(f"No se pudo extraer número de iteración de {key}")
                        
                        # Sumar a la consolidación si ya existen datos, o inicializar
                        if df_comprar_consolidado.empty:
                            df_comprar_consolidado = df_iter.copy()
                        else:
                            # Solo sumar los valores numéricos (horas), mantener FECHA
                            for hora in range(1, 25):
                                if hora in df_iter.columns and hora in df_comprar_consolidado.columns:
                                    # Suma hora por hora
                                    for idx, row in df_iter.iterrows():
                                        fecha = row['FECHA']
                                        # Buscar la fila correspondiente en el df consolidado
                                        fecha_rows = df_comprar_consolidado[df_comprar_consolidado['FECHA'] == fecha]
                                        if not fecha_rows.empty:
                                            df_comprar_consolidado.loc[df_comprar_consolidado['FECHA'] == fecha, hora] += row[hora]
                
                # Para la energía no comprada, usar solo la última iteración
                key_ultima_it_no_comprada = f"DEMANDA ASIGNADA {oferta} IT{ultima_iteracion}_NO_COMPRADA"
                if key_ultima_it_no_comprada in resultados_dict:
                    df_no_comprado_consolidado = resultados_dict[key_ultima_it_no_comprada].copy()
                else:
                    # Si no se encuentra la última iteración, buscar la mayor disponible
                    for key in resultados_dict.keys():
                        if f"DEMANDA ASIGNADA {oferta}" in key and "_NO_COMPRADA" in key:
                            df_iter = resultados_dict[key].copy()
                            if df_no_comprado_consolidado is None:
                                df_no_comprado_consolidado = df_iter.copy()
                                # Guardar el nombre para comparaciones posteriores
                                df_no_comprado_consolidado.name = key
                            else:
                                # Comparar iteraciones
                                try:
                                    it_actual = int(key.split("IT")[1].split("_")[0])
                                    it_guardada = int(df_no_comprado_consolidado.name.split("IT")[1].split("_")[0])
                                    if it_actual > it_guardada:
                                        df_no_comprado_consolidado = df_iter.copy()
                                        df_no_comprado_consolidado.name = key
                                except:
                                    logger.warning(f"No se pudo comparar iteraciones entre {key} y {df_no_comprado_consolidado.name}")
                
                # Si no se encontró ninguna, crear un DataFrame vacío
                if df_no_comprado_consolidado is None:
                    logger.warning(f"No se encontró información de energía no comprada para oferta {oferta}")
                    # Crear DataFrame vacío con la misma estructura que el consolidado de compras
                    if not df_comprar_consolidado.empty:
                        df_no_comprado_consolidado = df_comprar_consolidado.copy()
                        for col in df_no_comprado_consolidado.columns:
                            if col not in ['FECHA', 'X']:
                                df_no_comprado_consolidado[col] = 0
                    else:
                        # Si no hay datos de compras, no hay datos para no compradas
                        continue
                
                # Ahora, combinar la energía no asignada de la optimización con la rechazada por precio
                # Crear una copia del DataFrame no comprado para añadir lo rechazado por precio
                df_no_comprado_total = df_no_comprado_consolidado.copy()
                
                # Añadir las ofertas rechazadas por precio
                if oferta in ofertas_rechazadas_por_precio:
                    rechazadas = ofertas_rechazadas_por_precio[oferta]
                    for item in rechazadas:
                        fecha = item['FECHA']
                        hora = item['HORA']
                        cantidad = item['CANTIDAD']
                        
                        # Buscar la fila para esta fecha
                        fecha_rows = df_no_comprado_total[df_no_comprado_total['FECHA'] == fecha]
                        if len(fecha_rows) > 0:
                            # Si existe la fila, actualizar el valor para esta hora
                            df_no_comprado_total.loc[df_no_comprado_total['FECHA'] == fecha, hora] = cantidad
                        else:
                            # Si no existe la fila, crear una nueva
                            nueva_fila = {'FECHA': fecha}
                            for h in range(1, 25):
                                nueva_fila[h] = cantidad if h == hora else 0
                            
                            # Añadir la fila al DataFrame
                            df_no_comprado_total = pd.concat([df_no_comprado_total, pd.DataFrame([nueva_fila])], ignore_index=True)
                
                # Si tenemos datos consolidados, exportar
                if not df_comprar_consolidado.empty:
                    # Mantener el orden cronológico original
                    df_comprar_ordenado = df_comprar_consolidado.copy()
                    
                    # Convertir fechas a formato string DD/MM/YYYY
                    df_comprar_ordenado["X"] = df_comprar_ordenado["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                    
                    # Eliminar columna FECHA (mantener sólo X)
                    df_comprar_ordenado = df_comprar_ordenado.drop(columns=["FECHA"])
                    
                    # Añadir un título para el cuadro
                    titulo_comprar = pd.DataFrame({
                        "X": ["ENERGÍA A COMPRAR AL VENDEDOR"],
                        **{i: [None] for i in range(1, 25)}  # Columnas del 1 al 24
                    })
                    
                    # Concatenar título y datos
                    df_final_comprar = pd.concat([titulo_comprar, df_comprar_ordenado], ignore_index=True)
                    
                    # Asegurar que el nombre de la hoja no exceda los 31 caracteres
                    sheet_name = f"DA-{oferta}"
                    if len(sheet_name) > 31:
                        sheet_name = sheet_name[:31]
                    
                    # Exportar sin el índice
                    df_final_comprar.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(f"Hoja exportada: {sheet_name}")
                
                # Exportar la energía no comprada (total)
                if df_no_comprado_total is not None and not df_no_comprado_total.empty:
                    # Mantener el orden cronológico original
                    df_no_comprado_ordenado = df_no_comprado_total.copy()
                    
                    # Convertir fechas a formato string DD/MM/YYYY
                    if "FECHA" in df_no_comprado_ordenado.columns:
                        df_no_comprado_ordenado["X"] = df_no_comprado_ordenado["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                        
                        # Eliminar columna FECHA (mantener sólo X)
                        df_no_comprado_ordenado = df_no_comprado_ordenado.drop(columns=["FECHA"])
                    
                    # Añadir un título para el cuadro
                    titulo_no_comprada = pd.DataFrame({
                        "X": ["ENERGÍA NO COMPRADA AL VENDEDOR"],
                        **{i: [None] for i in range(1, 25)}  # Columnas del 1 al 24
                    })
                    
                    # Concatenar título y datos
                    df_final_no_comprada = pd.concat([titulo_no_comprada, df_no_comprado_ordenado], ignore_index=True)
                    
                    # Nombre de la hoja
                    sheet_name_ena = f"ENA-{oferta}"
                    if len(sheet_name_ena) > 31:
                        sheet_name_ena = sheet_name_ena[:31]
                    
                    # Exportar sin el índice
                    df_final_no_comprada.to_excel(writer, sheet_name=sheet_name_ena, index=False)
                    logger.info(f"Hoja exportada: {sheet_name_ena}")
            
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
            
            # 4. Exportar hoja de RESUMEN SIN INDEXAR
            if "RESUMEN SIN INDEXAR" in resultados_dict:
                df_export = resultados_dict["RESUMEN SIN INDEXAR"].copy()
                
                # El formato de fecha ya está establecido como MM/YYYY
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
                
                df_final.to_excel(writer, sheet_name="RESUMEN SIN INDEXAR", index=False)
                logger.info(f"Hoja exportada: RESUMEN SIN INDEXAR")
            
            # NUEVO: Exportar un resumen de ofertas rechazadas por precio
            if ofertas_rechazadas_por_precio:
                # Datos para el resumen
                resumen_datos = []
                
                # Para cada oferta con rechazos por precio
                for oferta, rechazadas in ofertas_rechazadas_por_precio.items():
                    # Calcular estadísticas
                    total_rechazado = sum(item['CANTIDAD'] for item in rechazadas)
                    precio_promedio = sum(item['PRECIO'] * item['CANTIDAD'] for item in rechazadas) / total_rechazado if total_rechazado > 0 else 0
                    
                    resumen_datos.append({
                        'OFERTA': oferta,
                        'REGISTROS RECHAZADOS': len(rechazadas),
                        'CANTIDAD TOTAL RECHAZADA': total_rechazado,
                        'PRECIO PROMEDIO': precio_promedio
                    })
                
                # Crear DataFrame con el resumen
                if resumen_datos:
                    df_resumen_rechazos = pd.DataFrame(resumen_datos)
                    
                    # Ordenar por cantidad total rechazada (descendente)
                    df_resumen_rechazos = df_resumen_rechazos.sort_values(by='CANTIDAD TOTAL RECHAZADA', ascending=False)
                    
                    # Exportar resumen
                    df_resumen_rechazos.to_excel(writer, sheet_name="RESUMEN RECHAZOS PRECIO", index=False)
                    logger.info("Hoja de resumen de rechazos por precio exportada")
        
        print(f"Resultados consolidados exportados exitosamente a: {archivo_salida}")
        
        # 2. ARCHIVO SECUNDARIO PARA ANÁLISIS (INCLUYE TODAS LAS ITERACIONES SEPARADAS)
        # Este archivo sigue igual porque debe contener todas las iteraciones por separado
        with pd.ExcelWriter(archivo_analisis, engine='openpyxl') as writer:
            # Para cada hoja en el diccionario de resultados, exportar la hoja tal cual (sin consolidar)
            for nombre_hoja, df in resultados_dict.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # Para hojas de demanda asignada y no asignada
                    if "DEMANDA ASIGNADA" in nombre_hoja:
                        df_export = df.copy()
                        
                        # Convertir fechas a formato string DD/MM/YYYY
                        if "FECHA" in df_export.columns:
                            df_export["X"] = df_export["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                            df_export = df_export.drop(columns=["FECHA"])
                            
                            # Determinar título apropiado basado en el tipo de hoja
                            if "_COMPRAR" in nombre_hoja:
                                titulo = pd.DataFrame({
                                    "X": ["ENERGÍA A COMPRAR AL VENDEDOR"],
                                    **{i: [None] for i in range(1, 25)}
                                })
                            elif "_NO_COMPRADA" in nombre_hoja:
                                titulo = pd.DataFrame({
                                    "X": ["ENERGÍA NO COMPRADA AL VENDEDOR"],
                                    **{i: [None] for i in range(1, 25)}
                                })
                            
                            # Concatenar título y datos
                            df_final = pd.concat([titulo, df_export], ignore_index=True)
                            
                            # Crear nombre de hoja en el formato solicitado: DA-OP1_Wide- EPM-IT1 o ENA-OP1_Wide- EPM-IT1
                            try:
                                # Extraer la oferta del nombre de la hoja
                                oferta_part = nombre_hoja.split("DEMANDA ASIGNADA ")[1].split(" IT")[0]
                                
                                # Extraer el número de iteración
                                it_part = "IT1"  # Valor predeterminado
                                if "IT" in nombre_hoja:
                                    it_match = nombre_hoja.split(" IT")[1].split("_")[0]
                                    if it_match:
                                        it_part = f"IT{it_match}"
                                
                                # Determinar el prefijo según el tipo
                                if "_COMPRAR" in nombre_hoja:
                                    prefix = "DA"
                                else:
                                    prefix = "ENA"
                                
                                # Construir el nombre de la hoja con el formato deseado
                                sheet_name = f"{prefix}-{oferta_part}-{it_part}"
                                
                                # Limitar a 31 caracteres si es necesario
                                if len(sheet_name) > 31:
                                    sheet_name = sheet_name[:31]
                                
                            except Exception as e:
                                # Si hay algún error en la extracción, usar un nombre simplificado
                                logger.warning(f"Error al crear nombre de hoja para {nombre_hoja}: {e}")
                                sheet_name = nombre_hoja[:31]
                            
                            # Exportar sin el índice
                            df_final.to_excel(writer, sheet_name=sheet_name, index=False)
                            logger.info(f"Hoja exportada a análisis: {sheet_name}")
                    
                    # Para hoja de demanda faltante
                    elif nombre_hoja == "DEMANDA_FALTANTE":
                        df_export = df.copy()
                        
                        # Convertir fechas a formato string DD/MM/YYYY
                        if "FECHA" in df_export.columns:
                            df_export["X"] = df_export["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                            df_export = df_export.drop(columns=["FECHA"])
                            
                            titulo = pd.DataFrame({
                                "X": ["DEMANDA FALTANTE POR HORA Y DÍA"],
                                **{i: [None] for i in range(1, 25)}
                            })
                            
                            df_final = pd.concat([titulo, df_export], ignore_index=True)
                            df_final.to_excel(writer, sheet_name="DEMANDA FALTANTE", index=False)
                            logger.info("Hoja DEMANDA FALTANTE exportada a análisis")
                    
                    # Para hojas de resumen
                    elif "RESUMEN" in nombre_hoja:
                        df_export = df.copy()
                        
                        # Crear títulos para resumen
                        titulos = {}
                        titulos["FECHA"] = ""
                        for col in df_export.columns:
                            if col != "FECHA":
                                titulos[col] = "CANTIDAD"
                        
                        # Añadir la fila de títulos
                        titulo_df = pd.DataFrame([titulos])
                        df_final = pd.concat([titulo_df, df_export], ignore_index=True)
                        
                        # Usar el nombre original para las hojas de resumen
                        df_final.to_excel(writer, sheet_name=nombre_hoja, index=False)
                        logger.info(f"Hoja {nombre_hoja} exportada a análisis")
                    
                    # Otras hojas (por si acaso)
                    else:
                        df.to_excel(writer, sheet_name=nombre_hoja[:31], index=False)
                        logger.info(f"Otra hoja exportada a análisis: {nombre_hoja[:31]}")
            
            logger.info(f"Análisis detallado exportado a: {archivo_analisis}")
            print(f"Archivo de análisis detallado creado: {archivo_analisis}")
        
        return True
    
    except Exception as e:
        logger.exception(f"Error al exportar resultados: {e}")
        print(f"ERROR: No se pudieron exportar los resultados: {e}")
        
        try:
            # Intentar con un archivo nuevo en caso de error
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nueva_ruta = archivo_salida.parent / f"{archivo_salida.stem}_nuevo_{timestamp}{archivo_salida.suffix}"
            
            print(f"Intentando crear un archivo nuevo en: {nueva_ruta}")
            
            # En un entorno de producción, aquí se implementaría la misma lógica de exportación
            # Por simplicidad, solo registramos el error
            
            logger.exception(f"Error al crear archivo alternativo: {e}")
            print(f"ERROR: No se pudo crear archivo alternativo: {e}")
            return False
            
        except Exception as alt_e:
            logger.exception(f"Error al crear archivo alternativo: {alt_e}")
            print(f"ERROR: No se pudo crear archivo alternativo: {alt_e}")
            return False