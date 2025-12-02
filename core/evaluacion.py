"""
Módulo para evaluación de ofertas y preparación para optimización.
Incluye funciones para evaluar ofertas y preparar datos para el modelo de optimización.
"""

import pandas as pd
import logging
from pathlib import Path
from core.utils import verificar_archivo_existe, leer_excel_seguro

logger = logging.getLogger(__name__)

# Límite seguro de filas para Excel (dejamos margen de seguridad)
MAX_FILAS_EXCEL = 1_000_000  # Excel soporta hasta 1,048,576 filas

def dividir_y_exportar_si_necesario(df, writer, nombre_hoja_base, incluir_titulo=None):
    """
    Exporta un DataFrame a Excel, dividiéndolo en múltiples hojas si excede el límite de filas.

    Args:
        df (DataFrame): DataFrame a exportar
        writer (ExcelWriter): Objeto ExcelWriter donde se escribirá
        nombre_hoja_base (str): Nombre base para la(s) hoja(s)
        incluir_titulo (DataFrame, opcional): DataFrame con título a incluir en cada hoja

    Returns:
        list: Lista de nombres de hojas creadas
    """
    # Si el DataFrame está dentro del límite, exportar normalmente
    total_filas = len(df)
    filas_titulo = len(incluir_titulo) if incluir_titulo is not None else 0

    if total_filas + filas_titulo <= MAX_FILAS_EXCEL:
        # Caso normal: exportar en una sola hoja (comportamiento original)
        if incluir_titulo is not None:
            df_final = pd.concat([incluir_titulo, df], ignore_index=True)
        else:
            df_final = df

        # Asegurar que el nombre no exceda 31 caracteres
        nombre_hoja = nombre_hoja_base[:31]
        df_final.to_excel(writer, sheet_name=nombre_hoja, index=False)
        logger.info(f"Hoja exportada: {nombre_hoja} ({total_filas} filas)")
        return [nombre_hoja]

    # Caso especial: necesitamos dividir en múltiples hojas
    logger.warning(f"⚠️ El DataFrame para '{nombre_hoja_base}' tiene {total_filas} filas, "
                   f"excediendo el límite de {MAX_FILAS_EXCEL}. Dividiendo en múltiples hojas...")
    print(f"⚠️ ADVERTENCIA: {nombre_hoja_base} tiene {total_filas} filas")
    print(f"   Dividiendo en múltiples hojas para no exceder el límite de Excel...")

    hojas_creadas = []
    filas_por_hoja = MAX_FILAS_EXCEL - filas_titulo  # Dejar espacio para el título
    num_hojas = (total_filas + filas_por_hoja - 1) // filas_por_hoja  # Redondeo hacia arriba

    for i in range(num_hojas):
        inicio = i * filas_por_hoja
        fin = min((i + 1) * filas_por_hoja, total_filas)

        # Extraer subset del DataFrame
        df_subset = df.iloc[inicio:fin].copy()

        # Agregar título si se proporcionó
        if incluir_titulo is not None:
            df_subset = pd.concat([incluir_titulo, df_subset], ignore_index=True)

        # Crear nombre de hoja con sufijo de parte
        if num_hojas > 1:
            nombre_hoja = f"{nombre_hoja_base}-P{i+1}"
        else:
            nombre_hoja = nombre_hoja_base

        # Asegurar que no exceda 31 caracteres
        nombre_hoja = nombre_hoja[:31]

        # Exportar
        df_subset.to_excel(writer, sheet_name=nombre_hoja, index=False)
        logger.info(f"Hoja exportada: {nombre_hoja} (filas {inicio+1}-{fin} de {total_filas})")
        print(f"   ✅ Hoja creada: {nombre_hoja} ({len(df_subset) - filas_titulo} filas de datos)")
        hojas_creadas.append(nombre_hoja)

    print(f"   📊 Total: {num_hojas} hoja(s) creada(s) para {nombre_hoja_base}")
    return hojas_creadas

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

# En core/evaluacion.py, modifica la función leer_ofertas_evaluadas

def leer_ofertas_evaluadas(archivo_ofertas, sheet_name="CANTIDADES Y PRECIOS", solo_validas=True):
    """
    Lee las ofertas evaluadas desde un archivo Excel.
    
    Args:
        archivo_ofertas (str o Path): Ruta al archivo Excel con ofertas
        sheet_name (str): Nombre de la hoja a leer
        solo_validas (bool): Si True, filtra solo ofertas válidas. Si False, retorna TODAS.
        
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
        
        # NUEVO: Retornar todas las ofertas si solo_validas=False
        if not solo_validas:
            logger.info(f"Se leyeron {len(df)} ofertas (incluyendo rechazadas)")
            print(f"Se leyeron {len(df)} ofertas (incluyendo rechazadas)")
            return df
        
        # Filtrar ofertas válidas (comportamiento original)
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
    VERSIÓN CORREGIDA: SIN deduplicación incorrecta - mantiene registros válidos.
    Consolida todas las iteraciones en una sola hoja por oferta.
    También crea un archivo secundario con todas las iteraciones para análisis.
    
    Args:
        resultados_dict (dict): Diccionario con los DataFrames de resultados
        archivo_salida (str o Path): Ruta donde se guardará el archivo Excel
        
    Returns:
        bool: True si la exportación fue exitosa, False en caso contrario
    """
    import pandas as pd
    import logging
    from pathlib import Path
    
    logger = logging.getLogger(__name__)
    
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
                
                # CORRECCIÓN: NO deduplicar datos base - son registros válidos
                print(f"Procesando {len(ofertas_df)} registros originales (manteniendo todos los válidos)")
                
                for idx, row in ofertas_df.iterrows():
                    oferta = row.get('CÓDIGO OFERTA', '')
                    fecha = row.get('FECHA', None)
                    hora = row.get('Atributo', None)
                    cantidad = row.get('CANTIDAD', 0)
                    evaluacion = row.get('EVALUACIÓN', 0)
                    precio = row.get('PRECIO INDEXADO', 0)
                    
                    if oferta and fecha and hora is not None:
                        # USAR TODAS LAS CLAVES - pueden existir múltiples registros válidos
                        clave = (oferta, fecha, hora, idx)  # Agregar índice para permitir múltiples
                        ofertas_originales[clave] = {
                            'cantidad': cantidad,
                            'evaluacion': evaluacion,
                            'precio': precio
                        }
                        
                        # Si la evaluación es 0, guardar para reporte de rechazadas
                        if evaluacion == 0:
                            if oferta not in ofertas_rechazadas_por_precio:
                                ofertas_rechazadas_por_precio[oferta] = []
                            
                            # MANTENER TODOS los registros rechazados (son válidos)
                            ofertas_rechazadas_por_precio[oferta].append({
                                'FECHA': fecha,
                                'HORA': hora,
                                'CANTIDAD': cantidad,
                                'PRECIO': precio
                            })
                
                if ofertas_originales:
                    print(f"Información de {len(ofertas_originales)} registros cargada (sin deduplicar)")
                    
                    # Mostrar estadísticas de rechazo
                    total_rechazados = sum(len(items) for items in ofertas_rechazadas_por_precio.values())
                    if total_rechazados > 0:
                        print(f"Ofertas rechazadas: {len(ofertas_rechazadas_por_precio)} ofertas con {total_rechazados} registros")
                else:
                    print("No se encontró información de ofertas originales")
                    
        except Exception as e:
            logger.warning(f"No se pudo leer información original de ofertas: {e}")
            print(f"No se pudo leer información original de ofertas: {e}")
        
        # 1. ARCHIVO PRINCIPAL PARA CLIENTE (CONSOLIDADO)
        # Usar modo 'a' (append) si el archivo existe, 'w' (write) si no existe
        modo = 'a' if archivo_existe else 'w'
        
        with pd.ExcelWriter(archivo_salida, engine='openpyxl', mode=modo, if_sheet_exists='replace') as writer:
            # Identificar todas las ofertas únicas en los resultados
            ofertas_unicas = set()
            for key in resultados_dict.keys():
                if "_COMPRAR" in key:
                    # Extraer nombre de la oferta (sin "DEMANDA ASIGNADA" y sin "IT#_COMPRAR")
                    nombre_oferta = key.split("DEMANDA ASIGNADA ")[1].split(" IT")[0]
                    ofertas_unicas.add(nombre_oferta)
            
            # Agregar las ofertas rechazadas que no aparecen en los resultados
            for oferta in ofertas_rechazadas_por_precio.keys():
                if oferta not in ofertas_unicas:
                    print(f"Añadiendo oferta completamente rechazada: {oferta}")
                    ofertas_unicas.add(oferta)
            
            # Para cada oferta, consolidar todas las iteraciones o crear hojas nuevas para rechazadas
            for oferta in ofertas_unicas:
                # Verificar si la oferta tiene asignaciones o solo fue rechazada por precio
                tiene_asignaciones = False
                for key in resultados_dict.keys():
                    if f"DEMANDA ASIGNADA {oferta}" in key and "_COMPRAR" in key:
                        tiene_asignaciones = True
                        break
                
                if tiene_asignaciones:
                    # CASO 1: La oferta tiene asignaciones en la optimización
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
                    
                    # USAR DATOS DEL OPTIMIZADOR SIN MEZCLAR
                    df_no_comprado_total = df_no_comprado_consolidado.copy()
                    
                    print(f"ENA para {oferta}: usando datos del optimizador")
                    
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

                        # Asegurar que el nombre de la hoja no exceda los 31 caracteres
                        sheet_name = f"DA-{oferta}"

                        # Exportar usando la nueva función con validación de límite
                        dividir_y_exportar_si_necesario(
                            df_comprar_ordenado,
                            writer,
                            sheet_name,
                            incluir_titulo=titulo_comprar
                        )
                    
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

                        # Nombre de la hoja
                        sheet_name_ena = f"ENA-{oferta}"

                        # Exportar usando la nueva función con validación de límite
                        dividir_y_exportar_si_necesario(
                            df_no_comprado_ordenado,
                            writer,
                            sheet_name_ena,
                            incluir_titulo=titulo_no_comprada
                        )
                else:
                    # CASO 2: La oferta fue completamente rechazada por precio
                    if oferta in ofertas_rechazadas_por_precio:
                        rechazadas = ofertas_rechazadas_por_precio[oferta]  # Lista de registros
                        
                        print(f"Procesando oferta completamente rechazada: {oferta} ({len(rechazadas)} registros)")
                        
                        # 1. Crear DataFrame para DA (todos ceros)
                        fechas_unicas = sorted(list(set(item['FECHA'] for item in rechazadas)))
                        
                        # Crear DataFrame vacío para DA (todo ceros)
                        da_rows = []
                        for fecha in fechas_unicas:
                            row = {"FECHA": fecha}
                            for hora in range(1, 25):
                                row[hora] = 0  # Todos los valores son cero
                            da_rows.append(row)
                        
                        if da_rows:
                            da_df = pd.DataFrame(da_rows)

                            # Convertir fechas a formato string DD/MM/YYYY
                            da_df["X"] = da_df["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                            da_df = da_df.drop(columns=["FECHA"])

                            # Añadir título
                            titulo_da = pd.DataFrame({
                                "X": ["ENERGÍA A COMPRAR AL VENDEDOR"],
                                **{i: [None] for i in range(1, 25)}
                            })

                            sheet_name = f"DA-{oferta}"

                            # Exportar usando la nueva función con validación de límite
                            dividir_y_exportar_si_necesario(
                                da_df,
                                writer,
                                sheet_name,
                                incluir_titulo=titulo_da
                            )
                            logger.info(f"Hoja DA exportada para oferta rechazada: {oferta}")
                            print(f"Hoja DA exportada para oferta rechazada: {oferta}")
                        
                        # 2. Crear DataFrame para ENA (valores originales) - AGREGANDO CORRECTAMENTE
                        ena_rows = []
                        for fecha in fechas_unicas:
                            row = {"FECHA": fecha}
                            # Inicializar todas las horas con cero
                            for hora in range(1, 25):
                                row[hora] = 0
                            
                            # CORRECCIÓN: SUMAR todos los registros para la misma fecha-hora
                            for item in rechazadas:
                                if item['FECHA'] == fecha:
                                    hora = item['HORA']
                                    if 1 <= hora <= 24:
                                        # SUMAR en lugar de sobrescribir
                                        row[hora] += item['CANTIDAD']
                            
                            ena_rows.append(row)
                        
                        if ena_rows:
                            ena_df = pd.DataFrame(ena_rows)

                            # Convertir fechas a formato string DD/MM/YYYY
                            ena_df["X"] = ena_df["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                            ena_df = ena_df.drop(columns=["FECHA"])

                            # Añadir título
                            titulo_ena = pd.DataFrame({
                                "X": ["ENERGÍA NO COMPRADA AL VENDEDOR"],
                                **{i: [None] for i in range(1, 25)}
                            })

                            sheet_name_ena = f"ENA-{oferta}"

                            # Exportar usando la nueva función con validación de límite
                            dividir_y_exportar_si_necesario(
                                ena_df,
                                writer,
                                sheet_name_ena,
                                incluir_titulo=titulo_ena
                            )
                            logger.info(f"Hoja ENA exportada para oferta rechazada: {oferta}")
                            print(f"Hoja ENA exportada para oferta rechazada: {oferta}")
                    else:
                        print(f"Advertencia: Oferta {oferta} no tiene datos de rechazo disponibles")
            
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

                # Exportar usando la nueva función con validación de límite
                dividir_y_exportar_si_necesario(
                    df_export,
                    writer,
                    "DEMANDA FALTANTE",
                    incluir_titulo=titulo_faltante
                )
            
            # Exportar hoja de RESUMEN EJECUTIVO (reemplaza a las hojas RESUMEN y RESUMEN SIN INDEXAR)
            if "RESUMEN EJECUTIVO" in resultados_dict:
                df_export = resultados_dict["RESUMEN EJECUTIVO"].copy()
                print(f"DEBUG - Columnas en RESUMEN EJECUTIVO antes de exportar: {df_export.columns.tolist()}")
                print(f"DEBUG - ¿Contiene BTG? {any('BTG' in str(col) for col in df_export.columns)}")
                print(f"DEBUG - Número de filas en resumen: {len(df_export)}")
                if len(df_export) > 0:
                    print(f"DEBUG - Primera fila de datos: {df_export.iloc[0].to_dict()}")

                # El formato de fecha ya está establecido como MM/YYYY
                # No reordenar, preservar el orden original

                # Crear títulos dinámicamente según las columnas disponibles
                titulos = {}
                titulos["FECHA"] = ""
                for col in df_export.columns:
                    if col != "FECHA":
                        # Las columnas ya incluyen las unidades en sus nombres
                        titulos[col] = ""

                # Añadir la fila de títulos
                titulo_df = pd.DataFrame([titulos])

                # Exportar usando la nueva función con validación de límite
                dividir_y_exportar_si_necesario(
                    df_export,
                    writer,
                    "RESUMEN EJECUTIVO",
                    incluir_titulo=titulo_df
                )
            
            # NUEVO: Exportar un resumen de ofertas rechazadas por precio
            if ofertas_rechazadas_por_precio:
                resumen_datos = []
                
                # Para cada oferta con rechazos por precio
                for oferta, rechazadas in ofertas_rechazadas_por_precio.items():
                    
                    # Calcular estadísticas SUMANDO todos los registros válidos
                    total_rechazado = sum(item['CANTIDAD'] for item in rechazadas)
                    precio_promedio = (
                        sum(item['PRECIO'] * item['CANTIDAD'] for item in rechazadas) / total_rechazado 
                        if total_rechazado > 0 else 0
                    )
                    
                    resumen_datos.append({
                        'OFERTA': oferta,
                        'REGISTROS RECHAZADOS': len(rechazadas),
                        'CANTIDAD TOTAL RECHAZADA (KWh)': total_rechazado,
                        'PRECIO PROMEDIO ($/KWh)': precio_promedio
                    })
                
                if resumen_datos:
                    df_resumen_rechazos = pd.DataFrame(resumen_datos)
                    df_resumen_rechazos = df_resumen_rechazos.sort_values(
                        by='CANTIDAD TOTAL RECHAZADA (KWh)', 
                        ascending=False
                    )
                    
                    df_resumen_rechazos.to_excel(writer, sheet_name="RESUMEN RECHAZOS PRECIO", index=False)
                    logger.info("Hoja de resumen de rechazos por precio exportada")
                    print(f"Resumen de rechazos generado: {len(resumen_datos)} ofertas")
        
        print(f"Resultados consolidados exportados exitosamente a: {archivo_salida}")
        
        # 2. ARCHIVO SECUNDARIO PARA ANÁLISIS (sin cambios - mantiene iteraciones separadas)
        with pd.ExcelWriter(archivo_analisis, engine='openpyxl') as writer:
            for nombre_hoja, df in resultados_dict.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    if "DEMANDA ASIGNADA" in nombre_hoja:
                        df_export = df.copy()
                        
                        if "FECHA" in df_export.columns:
                            df_export["X"] = df_export["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                            df_export = df_export.drop(columns=["FECHA"])
                            
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
                            
                            try:
                                oferta_part = nombre_hoja.split("DEMANDA ASIGNADA ")[1].split(" IT")[0]
                                it_part = "IT1"
                                if "IT" in nombre_hoja:
                                    it_match = nombre_hoja.split(" IT")[1].split("_")[0]
                                    if it_match:
                                        it_part = f"IT{it_match}"

                                if "_COMPRAR" in nombre_hoja:
                                    prefix = "DA"
                                else:
                                    prefix = "ENA"

                                sheet_name = f"{prefix}-{oferta_part}-{it_part}"

                            except Exception as e:
                                logger.warning(f"Error al crear nombre de hoja para {nombre_hoja}: {e}")
                                sheet_name = nombre_hoja[:31]

                            # Exportar usando la nueva función con validación de límite
                            dividir_y_exportar_si_necesario(
                                df_export,
                                writer,
                                sheet_name,
                                incluir_titulo=titulo
                            )
                    
                    elif nombre_hoja == "DEMANDA_FALTANTE":
                        df_export = df.copy()

                        if "FECHA" in df_export.columns:
                            df_export["X"] = df_export["FECHA"].apply(lambda x: x.strftime('%d/%m/%Y'))
                            df_export = df_export.drop(columns=["FECHA"])

                            titulo = pd.DataFrame({
                                "X": ["DEMANDA FALTANTE POR HORA Y DÍA"],
                                **{i: [None] for i in range(1, 25)}
                            })

                            # Exportar usando la nueva función con validación de límite
                            dividir_y_exportar_si_necesario(
                                df_export,
                                writer,
                                "DEMANDA FALTANTE",
                                incluir_titulo=titulo
                            )
                    
                    elif nombre_hoja == "RESUMEN EJECUTIVO":
                        df_export = df.copy()

                        titulos = {}
                        titulos["FECHA"] = ""
                        for col in df_export.columns:
                            if col != "FECHA":
                                titulos[col] = ""

                        titulo_df = pd.DataFrame([titulos])

                        # Exportar usando la nueva función con validación de límite
                        dividir_y_exportar_si_necesario(
                            df_export,
                            writer,
                            nombre_hoja,
                            incluir_titulo=titulo_df
                        )
                    
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
            
            logger.exception(f"Error al crear archivo alternativo: {e}")
            print(f"ERROR: No se pudo crear archivo alternativo: {e}")
            return False
            
        except Exception as alt_e:
            logger.exception(f"Error al crear archivo alternativo: {alt_e}")
            print(f"ERROR: No se pudo crear archivo alternativo: {alt_e}")
            return False
                      
def cargar_resultados_desde_excel(archivo_resultados):
    """
    Carga los resultados de optimización desde un archivo Excel existente
    y reconstruye el diccionario de resultados en el formato esperado por las visualizaciones.
    
    Args:
        archivo_resultados (str o Path): Ruta al archivo Excel con los resultados
        
    Returns:
        dict: Diccionario con los resultados en formato compatible con visualizaciones,
              o diccionario vacío en caso de error
    """
    logger.info(f"Cargando resultados desde {archivo_resultados}")
    print(f"📊 Cargando resultados desde {archivo_resultados}")
    
    try:
        # Verificar que el archivo existe
        if not verificar_archivo_existe(archivo_resultados):
            logger.error(f"No se encontró el archivo de resultados: {archivo_resultados}")
            print(f"❌ ERROR: No se encontró el archivo de resultados: {archivo_resultados}")
            return {}
        
        # Leer todas las hojas del archivo
        try:
            excel_file = pd.ExcelFile(archivo_resultados)
            hojas_disponibles = excel_file.sheet_names
            print(f"📋 Hojas encontradas: {len(hojas_disponibles)}")
            
        except Exception as e:
            logger.error(f"Error al abrir archivo Excel: {e}")
            print(f"❌ ERROR: No se pudo abrir el archivo Excel: {e}")
            return {}
        
        # Diccionario para almacenar los resultados reconstruidos
        resultados_dict = {}
        
        # 1. CARGAR HOJAS DE DEMANDA ASIGNADA (DA-* y ENA-*)
        hojas_da = [h for h in hojas_disponibles if h.startswith('DA-')]
        hojas_ena = [h for h in hojas_disponibles if h.startswith('ENA-')]
        
        print(f"🔍 Encontradas {len(hojas_da)} hojas DA y {len(hojas_ena)} hojas ENA")
        
        # Procesar hojas DA (Demanda Asignada - COMPRAR)
        for hoja_da in hojas_da:
            try:
                # Leer la hoja
                df_raw = pd.read_excel(archivo_resultados, sheet_name=hoja_da)
                
                if df_raw.empty:
                    continue
                
                # Extraer nombre de la oferta del nombre de la hoja (formato: "DA-NombreOferta")
                nombre_oferta = hoja_da.replace('DA-', '')
                
                # Buscar la fila que no sea el título
                df_datos = None
                for idx, row in df_raw.iterrows():
                    # Saltar filas de título que contienen "ENERGÍA A COMPRAR"
                    if pd.notna(row.iloc[0]) and "ENERGÍA A COMPRAR" not in str(row.iloc[0]):
                        # Esta es una fila de datos
                        if df_datos is None:
                            # Crear DataFrame con las columnas correctas
                            columnas = ['X'] + list(range(1, 25))  # X + horas 1-24
                            df_datos = pd.DataFrame(columns=['FECHA'] + list(range(1, 25)))
                        
                        # Convertir la fila a datos numéricos
                        fecha_str = row.iloc[0]  # Primera columna es la fecha
                        
                        # Convertir fecha de string DD/MM/YYYY a datetime.date
                        try:
                            if isinstance(fecha_str, str):
                                fecha = pd.to_datetime(fecha_str, format='%d/%m/%Y').date()
                            else:
                                fecha = fecha_str
                        except:
                            continue
                        
                        # Crear fila de datos
                        nueva_fila = {'FECHA': fecha}
                        
                        # Agregar valores por hora (columnas 1-24)
                        for hora in range(1, 25):
                            try:
                                valor = float(row.iloc[hora]) if pd.notna(row.iloc[hora]) else 0.0
                                nueva_fila[hora] = valor
                            except:
                                nueva_fila[hora] = 0.0
                        
                        # Agregar la fila al DataFrame
                        df_datos = pd.concat([df_datos, pd.DataFrame([nueva_fila])], ignore_index=True)
                
                if df_datos is not None and not df_datos.empty:
                    # Clave para el diccionario (simulando iteración 1)
                    clave = f"DEMANDA ASIGNADA {nombre_oferta} IT1_COMPRAR"
                    resultados_dict[clave] = df_datos
                    print(f"  ✅ Cargada hoja DA: {nombre_oferta} ({len(df_datos)} registros)")
                
            except Exception as e:
                logger.warning(f"Error al procesar hoja DA {hoja_da}: {e}")
                print(f"  ⚠️ Error en hoja DA {hoja_da}: {e}")
        
        # Procesar hojas ENA (Energía No Asignada - NO_COMPRADA)
        for hoja_ena in hojas_ena:
            try:
                # Leer la hoja
                df_raw = pd.read_excel(archivo_resultados, sheet_name=hoja_ena)
                
                if df_raw.empty:
                    continue
                
                # Extraer nombre de la oferta del nombre de la hoja (formato: "ENA-NombreOferta")
                nombre_oferta = hoja_ena.replace('ENA-', '')
                
                # Buscar la fila que no sea el título
                df_datos = None
                for idx, row in df_raw.iterrows():
                    # Saltar filas de título que contienen "ENERGÍA NO COMPRADA"
                    if pd.notna(row.iloc[0]) and "ENERGÍA NO COMPRADA" not in str(row.iloc[0]):
                        # Esta es una fila de datos
                        if df_datos is None:
                            # Crear DataFrame con las columnas correctas
                            df_datos = pd.DataFrame(columns=['FECHA'] + list(range(1, 25)))
                        
                        # Convertir la fila a datos numéricos
                        fecha_str = row.iloc[0]  # Primera columna es la fecha
                        
                        # Convertir fecha de string DD/MM/YYYY a datetime.date
                        try:
                            if isinstance(fecha_str, str):
                                fecha = pd.to_datetime(fecha_str, format='%d/%m/%Y').date()
                            else:
                                fecha = fecha_str
                        except:
                            continue
                        
                        # Crear fila de datos
                        nueva_fila = {'FECHA': fecha}
                        
                        # Agregar valores por hora (columnas 1-24)
                        for hora in range(1, 25):
                            try:
                                valor = float(row.iloc[hora]) if pd.notna(row.iloc[hora]) else 0.0
                                nueva_fila[hora] = valor
                            except:
                                nueva_fila[hora] = 0.0
                        
                        # Agregar la fila al DataFrame
                        df_datos = pd.concat([df_datos, pd.DataFrame([nueva_fila])], ignore_index=True)
                
                if df_datos is not None and not df_datos.empty:
                    # Clave para el diccionario (simulando iteración 1)
                    clave = f"DEMANDA ASIGNADA {nombre_oferta} IT1_NO_COMPRADA"
                    resultados_dict[clave] = df_datos
                    print(f"  ✅ Cargada hoja ENA: {nombre_oferta} ({len(df_datos)} registros)")
                
            except Exception as e:
                logger.warning(f"Error al procesar hoja ENA {hoja_ena}: {e}")
                print(f"  ⚠️ Error en hoja ENA {hoja_ena}: {e}")
        
        # 2. CARGAR HOJA DE DEMANDA FALTANTE
        if "DEMANDA FALTANTE" in hojas_disponibles:
            try:
                df_raw = pd.read_excel(archivo_resultados, sheet_name="DEMANDA FALTANTE")
                
                if not df_raw.empty:
                    # Buscar filas que no sean títulos
                    df_datos = None
                    for idx, row in df_raw.iterrows():
                        # Saltar filas de título
                        if pd.notna(row.iloc[0]) and "DEMANDA FALTANTE" not in str(row.iloc[0]):
                            if df_datos is None:
                                df_datos = pd.DataFrame(columns=['FECHA'] + list(range(1, 25)))
                            
                            # Procesar fecha
                            fecha_str = row.iloc[0]
                            try:
                                if isinstance(fecha_str, str):
                                    fecha = pd.to_datetime(fecha_str, format='%d/%m/%Y').date()
                                else:
                                    fecha = fecha_str
                            except:
                                continue
                            
                            # Crear fila de datos
                            nueva_fila = {'FECHA': fecha}
                            for hora in range(1, 25):
                                try:
                                    valor = float(row.iloc[hora]) if pd.notna(row.iloc[hora]) else 0.0
                                    nueva_fila[hora] = valor
                                except:
                                    nueva_fila[hora] = 0.0
                            
                            df_datos = pd.concat([df_datos, pd.DataFrame([nueva_fila])], ignore_index=True)
                    
                    if df_datos is not None and not df_datos.empty:
                        resultados_dict["DEMANDA_FALTANTE"] = df_datos
                        print(f"  ✅ Cargada DEMANDA FALTANTE ({len(df_datos)} registros)")
                
            except Exception as e:
                logger.warning(f"Error al procesar DEMANDA FALTANTE: {e}")
                print(f"  ⚠️ Error en DEMANDA FALTANTE: {e}")
        
        # 3. CARGAR HOJA DE RESUMEN EJECUTIVO
        if "RESUMEN EJECUTIVO" in hojas_disponibles:
            try:
                # Leer toda la hoja sin procesar
                df_raw = pd.read_excel(archivo_resultados, sheet_name="RESUMEN EJECUTIVO", header=None)
                
                if not df_raw.empty:
                    print(f"  🔍 DEBUG - Forma del resumen ejecutivo raw: {df_raw.shape}")
                    
                    # Buscar la fila que contiene los títulos (primera fila no vacía)
                    fila_titulos = None
                    for idx, row in df_raw.iterrows():
                        # Verificar si esta fila tiene contenido útil como títulos
                        valores_no_nulos = [val for val in row.values if pd.notna(val) and str(val).strip() != '']
                        if len(valores_no_nulos) > 5:  # Si tiene más de 5 valores, probablemente son títulos
                            fila_titulos = idx
                            break
                    
                    if fila_titulos is not None:
                        # Usar esa fila como nombres de columnas
                        titulos = df_raw.iloc[fila_titulos].values
                        print(f"  🔍 DEBUG - Títulos encontrados en fila {fila_titulos}: {titulos}")
                        
                        # Crear DataFrame con los datos (filas después de los títulos)
                        if fila_titulos + 1 < len(df_raw):
                            df_datos = df_raw.iloc[fila_titulos + 1:].copy()
                            df_datos.columns = titulos
                            df_datos = df_datos.reset_index(drop=True)
                            
                            # Limpiar columnas con nombres NaN
                            nuevas_columnas = []
                            for i, col in enumerate(df_datos.columns):
                                if pd.isna(col) or str(col).strip() == '':
                                    nuevas_columnas.append(f"COLUMNA_{i}")
                                else:
                                    nuevas_columnas.append(str(col).strip())
                            
                            df_datos.columns = nuevas_columnas
                            
                            # Solo mantener filas con datos válidos
                            df_datos = df_datos.dropna(how='all')
                            
                            if not df_datos.empty:
                                resultados_dict["RESUMEN EJECUTIVO"] = df_datos
                                print(f"  ✅ Cargado RESUMEN EJECUTIVO ({len(df_datos)} registros)")
                                print(f"  📋 Columnas del resumen: {df_datos.columns.tolist()}")
                            else:
                                print(f"  ⚠️ RESUMEN EJECUTIVO sin datos válidos")
                        else:
                            print(f"  ⚠️ No hay datos después de los títulos en RESUMEN EJECUTIVO")
                    else:
                        # Si no encontramos títulos, usar la primera fila como datos
                        print(f"  ⚠️ No se encontraron títulos claros, usando toda la hoja")
                        
                        # Crear nombres de columnas genéricos
                        columnas_genericas = [f"COL_{i}" for i in range(len(df_raw.columns))]
                        df_raw.columns = columnas_genericas
                        
                        resultados_dict["RESUMEN EJECUTIVO"] = df_raw
                        print(f"  ✅ Cargado RESUMEN EJECUTIVO con columnas genéricas ({len(df_raw)} registros)")
                
            except Exception as e:
                logger.warning(f"Error al procesar RESUMEN EJECUTIVO: {e}")
                print(f"  ⚠️ Error en RESUMEN EJECUTIVO: {e}")
        
        # Mostrar resumen de lo que se cargó
        total_hojas_cargadas = len(resultados_dict)
        print(f"\n📊 Resumen de carga:")
        print(f"   ✅ Total hojas cargadas: {total_hojas_cargadas}")
        
        if total_hojas_cargadas > 0:
            print(f"   📋 Tipos de datos cargados:")
            tipos = {
                "DA (Demanda Asignada)": len([k for k in resultados_dict.keys() if "_COMPRAR" in k]),
                "ENA (Energía No Asignada)": len([k for k in resultados_dict.keys() if "_NO_COMPRADA" in k]),
                "Demanda Faltante": 1 if "DEMANDA_FALTANTE" in resultados_dict else 0,
                "Resumen Ejecutivo": 1 if "RESUMEN EJECUTIVO" in resultados_dict else 0
            }
            
            for tipo, cantidad in tipos.items():
                if cantidad > 0:
                    print(f"     - {tipo}: {cantidad}")
            
            logger.info(f"Resultados cargados exitosamente: {total_hojas_cargadas} hojas")
            return resultados_dict
        else:
            logger.warning("No se pudieron cargar datos del archivo Excel")
            print("⚠️ ADVERTENCIA: No se pudieron cargar datos del archivo Excel")
            return {}
            
    except Exception as e:
        logger.exception(f"Error general al cargar resultados: {e}")
        print(f"❌ ERROR GENERAL: {e}")
        return {}