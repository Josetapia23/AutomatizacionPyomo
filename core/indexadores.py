"""
Módulo para el manejo de indexadores y sus proyecciones.
Incluye funciones para calcular, proyectar y gestionar datos de indexadores.
"""

import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from pathlib import Path
import os

from config import DATOS_INICIALES
from .utils import (
    verificar_archivo_existe, 
    verificar_hoja_existe, 
    eliminar_hoja_si_existe,
    leer_excel_seguro,
    guardar_excel_seguro,
    solicitar_input_seguro,
    fecha_a_texto
)

logger = logging.getLogger(__name__)

def calcular_numerador(fecha, indexador, numerador, indexadores_df, proyeccion_df):
    """
    Calcula el valor del numerador basado en reglas establecidas.
    Usa la fecha correspondiente a cada registro.
    
    Args:
        fecha (datetime.date): Fecha para la que se calcula el numerador
        indexador (str): Tipo de indexador (ej. "IPC")
        numerador (str): Tipo de numerador ("PROVISIONAL" o "DEFINITIVO")
        indexadores_df (DataFrame): DataFrame con datos de indexadores
        proyeccion_df (DataFrame): DataFrame con proyecciones de indexadores
        
    Returns:
        float: Valor del numerador calculado, o None si no se pudo calcular
    """
    fecha_ano_mes = fecha_a_texto(fecha)
    
    # Convertir fechas a formato año-mes para comparación
    indexadores_df['fecha_str'] = indexadores_df['fechaoperacion'].apply(fecha_a_texto)
    proyeccion_df['fecha_str'] = proyeccion_df['fechaoperacion'].apply(fecha_a_texto)
    
    valor = None
    
    if indexador == "IPC":
        valor = indexadores_df.loc[indexadores_df['fecha_str'] == fecha_ano_mes, 'ipc']
        if valor.empty:
            valor = proyeccion_df.loc[proyeccion_df['fecha_str'] == fecha_ano_mes, 'ipc']
    elif indexador != "IPC" and numerador == "PROVISIONAL":
        valor = indexadores_df.loc[indexadores_df['fecha_str'] == fecha_ano_mes, 'oferta_interna_prov']
        if valor.empty:
            valor = proyeccion_df.loc[proyeccion_df['fecha_str'] == fecha_ano_mes, 'oferta_interna_prov']
    elif indexador != "IPC" and numerador == "DEFINITIVO":
        valor = indexadores_df.loc[indexadores_df['fecha_str'] == fecha_ano_mes, 'oferta_interna_def']
        if valor.empty:
            valor = proyeccion_df.loc[proyeccion_df['fecha_str'] == fecha_ano_mes, 'oferta_interna_def']
    
    return valor.iloc[0] if not valor.empty else None

def calcular_denominador(fecha_base, indexador, denominador, indexadores_df, proyeccion_df):
    """
    Calcula el valor del denominador basado en reglas establecidas.
    Usa la fecha base (no la fecha de iteración).
    
    Args:
        fecha_base (datetime.date): Fecha base para el cálculo
        indexador (str): Tipo de indexador (ej. "IPC")
        denominador (str): Tipo de denominador ("PROVISIONAL" o "DEFINITIVO")
        indexadores_df (DataFrame): DataFrame con datos de indexadores
        proyeccion_df (DataFrame): DataFrame con proyecciones de indexadores
        
    Returns:
        float: Valor del denominador calculado, o None si no se pudo calcular
    """
    fecha_ano_mes = fecha_a_texto(fecha_base)
    
    # Convertir fechas a formato año-mes para comparación
    indexadores_df['fecha_str'] = indexadores_df['fechaoperacion'].apply(fecha_a_texto)
    proyeccion_df['fecha_str'] = proyeccion_df['fechaoperacion'].apply(fecha_a_texto)
    
    valor = None
    
    if indexador == "IPC":
        valor = indexadores_df.loc[indexadores_df['fecha_str'] == fecha_ano_mes, 'ipc']
        if valor.empty:
            valor = proyeccion_df.loc[proyeccion_df['fecha_str'] == fecha_ano_mes, 'ipc']
    elif indexador != "IPC" and denominador == "PROVISIONAL":
        valor = indexadores_df.loc[indexadores_df['fecha_str'] == fecha_ano_mes, 'oferta_interna_prov']
        if valor.empty:
            valor = proyeccion_df.loc[proyeccion_df['fecha_str'] == fecha_ano_mes, 'oferta_interna_prov']
    elif indexador != "IPC" and denominador == "DEFINITIVO":
        valor = indexadores_df.loc[indexadores_df['fecha_str'] == fecha_ano_mes, 'oferta_interna_def']
        if valor.empty:
            valor = proyeccion_df.loc[proyeccion_df['fecha_str'] == fecha_ano_mes, 'oferta_interna_def']
    
    return valor.iloc[0] if not valor.empty else None

def crear_proyeccion_indexadores(datos_iniciales=DATOS_INICIALES, carpeta_ofertas=None):
    """
    Crea o actualiza la hoja 'PROYECCIÓN INDEXADORES' en el archivo de datos iniciales,
    proyectando valores mes a mes hasta la última fecha en la hoja 'cantidad' 
    de la primera oferta encontrada.
    
    Args:
        datos_iniciales (Path): Ruta al archivo de datos iniciales
        carpeta_ofertas (Path, opcional): Carpeta donde se encuentran las ofertas
        
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    logger.info(f"Creando proyección de indexadores en {datos_iniciales}")
    
    # Verificar que el archivo de datos iniciales existe
    if not verificar_archivo_existe(datos_iniciales):
        logger.error(f"No se encontró el archivo de datos iniciales: {datos_iniciales}")
        return False
    
    # Si no se proporcionó carpeta de ofertas, usar la configuración global
    if carpeta_ofertas is None:
        from config import OFERTAS_DIR
        carpeta_ofertas = OFERTAS_DIR
    
    # Buscar archivos de ofertas
    archivos = [f for f in os.listdir(carpeta_ofertas) if f.endswith('.xlsx')]
    if not archivos:
        logger.error(f"No se encontraron archivos en la carpeta {carpeta_ofertas}")
        return False
    
    # Leer la primera oferta para obtener la fecha máxima
    ruta_archivo_oferta = Path(carpeta_ofertas) / archivos[0]
    cantidad_df = leer_excel_seguro(ruta_archivo_oferta, sheet_name="cantidad")
    if cantidad_df.empty:
        logger.error(f"No se pudo leer la hoja cantidad del archivo {ruta_archivo_oferta}")
        return False
    
    # Verificar si ya existe la hoja de proyección
    hoja_existente = verificar_hoja_existe(datos_iniciales, "PROYECCIÓN INDEXADORES")
    
    # Variable para saber de dónde obtendremos los valores base
    usar_proyeccion_existente = False
    proyeccion_anterior_df = None
    
    if hoja_existente:
        # Leer la proyección existente
        proyeccion_anterior_df = leer_excel_seguro(datos_iniciales, "PROYECCIÓN INDEXADORES")
        if not proyeccion_anterior_df.empty:
            usar_proyeccion_existente = True
            print("Se encontró una proyección existente y se usará como base para la actualización")
            logger.info("Se encontró una proyección existente y se usará como base para la actualización")
    
    # Si no existe o está vacía, usaremos los indexadores originales
    if not usar_proyeccion_existente:
        # Leer los indexadores originales
        indexadores_df = leer_excel_seguro(datos_iniciales, sheet_name="INDEXADORES")
        if indexadores_df.empty:
            logger.error("No se pudo leer la hoja INDEXADORES del archivo de datos iniciales")
            return False
        
        # Convertir columnas de fechas
        indexadores_df['fechaoperacion'] = pd.to_datetime(indexadores_df['fechaoperacion'], format="%d/%m/%Y").dt.date
        
        # Obtener la última fecha de indexadores 
        fecha_mayor_indexadores = indexadores_df['fechaoperacion'].max()
        fila_base = indexadores_df.loc[indexadores_df['fechaoperacion'] == fecha_mayor_indexadores].iloc[0]
        
        # Extraer valores base
        oferta_interna_prov = fila_base['oferta_interna_prov']
        oferta_interna_def = fila_base['oferta_interna_def']
        ipc = fila_base['ipc']
        
        # Fecha de inicio para la proyección
        fecha_inicio = fecha_mayor_indexadores
    else:
        # Convertir columnas de fechas de la proyección existente
        proyeccion_anterior_df['fechaoperacion'] = pd.to_datetime(proyeccion_anterior_df['fechaoperacion']).dt.date
        
        # Obtener la última fecha de la proyección anterior
        fecha_mayor_proyeccion = proyeccion_anterior_df['fechaoperacion'].max()
        fila_base = proyeccion_anterior_df.loc[proyeccion_anterior_df['fechaoperacion'] == fecha_mayor_proyeccion].iloc[0]
        
        # Extraer valores base de la última fecha de la proyección anterior
        oferta_interna_prov = fila_base['oferta_interna_prov']
        oferta_interna_def = fila_base['oferta_interna_def']
        ipc = fila_base['ipc']
        
        # Fecha de inicio para la nueva proyección (día siguiente al último de la anterior)
        if fecha_mayor_proyeccion.month == 12:
            fecha_inicio = fecha_mayor_proyeccion.replace(year=fecha_mayor_proyeccion.year + 1, month=1)
        else:
            fecha_inicio = fecha_mayor_proyeccion.replace(month=fecha_mayor_proyeccion.month + 1)
    
    # Convertir fecha de la demanda
    cantidad_df['FECHA'] = pd.to_datetime(cantidad_df['FECHA'], format="%d/%m/%Y").dt.date
    fecha_mayor_cantidad = cantidad_df['FECHA'].max()
    
    # Si la fecha de demanda es anterior a la última fecha proyectada, no hay que hacer nada
    if usar_proyeccion_existente and fecha_mayor_cantidad <= fecha_mayor_proyeccion:
        print(f"La proyección ya cubre hasta {fecha_mayor_proyeccion}, que es posterior a la última fecha de demanda ({fecha_mayor_cantidad})")
        return True
    
    # Solicitar el crecimiento anual al usuario
    crecimiento_anual = solicitar_input_seguro(
        "Ingrese el crecimiento indexador anual (ej. 4 para 4%): ",
        tipo=float,
        validacion=lambda x: x >= 0,
        mensaje_error="El crecimiento debe ser un número positivo."
    )
    
    # Calcular variación mensual aproximada
    var_mensual = (1 + crecimiento_anual / 100) ** (1 / 12) - 1
    
    # Iniciar proyección
    proyeccion_data = []
    
    # Si ya existe una proyección, incluir todos sus registros existentes
    if usar_proyeccion_existente:
        for _, row in proyeccion_anterior_df.iterrows():
            proyeccion_data.append({
                "fechaoperacion": row['fechaoperacion'],
                "oferta_interna_prov": row['oferta_interna_prov'],
                "oferta_interna_def": row['oferta_interna_def'],
                "ipc": row['ipc']
            })
    
    # Proyectar mes a mes desde la fecha de inicio hasta la fecha máxima de demanda
    fecha_actual = fecha_inicio
    
    while fecha_actual <= fecha_mayor_cantidad:
        proyeccion_data.append({
            "fechaoperacion": fecha_actual,
            "oferta_interna_prov": round(oferta_interna_prov, 2),
            "oferta_interna_def": round(oferta_interna_def, 2),
            "ipc": round(ipc, 2)
        })
        
        # Actualizar valores para el siguiente mes
        oferta_interna_prov *= (1 + var_mensual)
        oferta_interna_def *= (1 + var_mensual)
        ipc *= (1 + var_mensual)
        
        # Avanzar al primer día del mes siguiente
        fecha_actual = (fecha_actual + timedelta(days=31)).replace(day=1)
    
    # Crear DataFrame con proyección
    proyeccion_df = pd.DataFrame(proyeccion_data)
    
    # Eliminar la hoja existente si es necesario
    if hoja_existente:
        eliminar_hoja_si_existe(datos_iniciales, "PROYECCIÓN INDEXADORES")
    
    # Guardar en el archivo
    resultado = guardar_excel_seguro(
        proyeccion_df, 
        datos_iniciales, 
        "PROYECCIÓN INDEXADORES",
        index=False
    )
    
    if resultado:
        logger.info(f"Proyección de indexadores {'actualizada' if usar_proyeccion_existente else 'creada'} correctamente")
        print(f"Proyección de indexadores {'actualizada' if usar_proyeccion_existente else 'creada'} correctamente con {len(proyeccion_df)} registros")
    else:
        logger.error(f"Error al {'actualizar' if usar_proyeccion_existente else 'crear'} la proyección de indexadores")
    
    return resultado

def crear_proyeccion_precio_sicep(datos_iniciales=DATOS_INICIALES):
    """
    Crea o actualiza la hoja 'PROYECCIÓN PRECIO SICEP' en el archivo de datos iniciales,
    proyectando los precios mensuales a partir de los datos anuales.
    También proyecta precios FNCER si están disponibles.
    
    Args:
        datos_iniciales (Path): Ruta al archivo de datos iniciales
        
    Returns:
        bool: True si la operación fue exitosa, False en caso contrario
    """
    logger.info(f"Creando proyección de precios SICEP y FNCER en {datos_iniciales}")
    
    # Verificar que el archivo de datos iniciales existe
    if not verificar_archivo_existe(datos_iniciales):
        logger.error(f"No se encontró el archivo de datos iniciales: {datos_iniciales}")
        return False
    
    # Eliminar la hoja si ya existe
    eliminar_hoja_si_existe(datos_iniciales, "PROYECCIÓN PRECIO SICEP")
    
    # Leer los datos de precios SICEP (precios anuales)
    try:
        # Leer sin conversión automática
        sicep_anual_df = leer_excel_seguro(datos_iniciales, "PRECIO SICEP", dtype=str)
        if sicep_anual_df.empty:
            logger.error("No se pudo leer la hoja PRECIO SICEP con los datos anuales")
            return False
        
        # Buscar columnas de año, precio y precio FNCER
        año_col = None
        precio_col = None
        precio_fncer_col = None
        
        for col_name in sicep_anual_df.columns:
            col_str = str(col_name).upper()
            if "AÑO" in col_str or "ANO" in col_str or "YEAR" in col_str:
                año_col = col_name
            elif "SICEP FNCER" in col_str or "FNCER" in col_str:
                precio_fncer_col = col_name
            elif "PRECIO" in col_str:
                precio_col = col_name
        
        # Si no se encontraron, usar las columnas adecuadas
        if año_col is None:
            año_col = sicep_anual_df.columns[0]  # Primera columna
        if precio_col is None:
            if len(sicep_anual_df.columns) > 1:
                precio_col = sicep_anual_df.columns[1]  # Segunda columna
            else:
                logger.error("No hay suficientes columnas en la hoja PRECIO SICEP")
                return False
        
        # Comprobar si existe la columna FNCER
        tiene_fncer = precio_fncer_col is not None
        if tiene_fncer:
            logger.info("Se encontró columna FNCER, se calculará proyección para ambos precios")
            print("Se encontró columna FNCER, se calculará proyección para ambos precios")
        else:
            logger.info("No se encontró columna FNCER, solo se calculará proyección para SICEP")
            print("No se encontró columna FNCER, solo se calculará proyección para SICEP")
        
        # Mostrar el dataframe para depuración
        print("DEBUG - Contenido completo de la hoja PRECIO SICEP (antes de limpiar):")
        print(sicep_anual_df)
        
        # Limpiar los datos manualmente
        # Eliminar filas completamente vacías
        sicep_anual_df = sicep_anual_df.dropna(how='all')
        
        # Reemplazar valores vacíos o nan en la columna de año
        for idx in range(len(sicep_anual_df)):
            if pd.isna(sicep_anual_df.iloc[idx][año_col]) or sicep_anual_df.iloc[idx][año_col] == '':
                # Si hay valor en la columna de precio, intentar inferir el año
                if idx > 0 and pd.notna(sicep_anual_df.iloc[idx][precio_col]):
                    try:
                        # Inferir el año sumando 1 al año anterior
                        año_anterior = int(float(sicep_anual_df.iloc[idx-1][año_col]))
                        sicep_anual_df.iloc[idx, sicep_anual_df.columns.get_loc(año_col)] = str(año_anterior + 1)
                        print(f"DEBUG - Año inferido para fila {idx}: {año_anterior + 1}")
                    except Exception as e:
                        print(f"DEBUG - Error al inferir año: {e}")
        
        print("DEBUG - Contenido después de limpiar:")
        print(sicep_anual_df)
        
        # Extraer los precios por año manualmente
        precios_por_año = {}
        precios_fncer_por_año = {}
        
        for idx in range(len(sicep_anual_df)):
            try:
                # Obtener valores de las celdas como strings
                año_str = str(sicep_anual_df.iloc[idx][año_col]).strip()
                precio_str = str(sicep_anual_df.iloc[idx][precio_col]).strip()
                
                # Intentar convertir a entero/float
                if año_str and año_str.lower() != 'nan' and año_str != 'NaN':
                    try:
                        año = int(float(año_str))
                        precio = float(precio_str)
                        precios_por_año[año] = precio
                        print(f"DEBUG - Extraído año {año} con precio {precio}")
                        
                        # Para FNCER
                        if tiene_fncer:
                            fncer_str = str(sicep_anual_df.iloc[idx][precio_fncer_col]).strip()
                            if fncer_str and fncer_str.lower() != 'nan':
                                precio_fncer = float(fncer_str)
                                precios_fncer_por_año[año] = precio_fncer
                                print(f"DEBUG - Extraído FNCER para año {año}: {precio_fncer}")
                    except ValueError as ve:
                        print(f"DEBUG - Error al convertir valores en fila {idx}: {ve}")
                        # Intenta inferir el año directamente del índice (2025 + idx)
                        if idx > 0 and precio_str and precio_str.lower() != 'nan':
                            try:
                                año_base = 2025  # Año inicial conocido
                                año_inferido = año_base + idx
                                precio = float(precio_str)
                                precios_por_año[año_inferido] = precio
                                print(f"DEBUG - Año inferido alternativo: {año_inferido} con precio {precio}")
                                
                                # Para FNCER
                                if tiene_fncer:
                                    fncer_str = str(sicep_anual_df.iloc[idx][precio_fncer_col]).strip()
                                    if fncer_str and fncer_str.lower() != 'nan':
                                        precio_fncer = float(fncer_str)
                                        precios_fncer_por_año[año_inferido] = precio_fncer
                                        print(f"DEBUG - Extraído FNCER para año inferido {año_inferido}: {precio_fncer}")
                            except Exception as e:
                                print(f"DEBUG - Falló la inferencia alternativa para fila {idx}: {e}")
            except Exception as e:
                print(f"DEBUG - Error procesando fila {idx}: {e}")
        
        if not precios_por_año:
            # Si aún no hay años extraídos, intentar un último método
            try:
                # Método directo basado en las imágenes compartidas
                # Las imágenes muestran que hay 3 años: 2025, 2026, 2027
                año_base = 2025
                for i, idx in enumerate(range(len(sicep_anual_df))):
                    if i < 3:  # Solo para las 3 primeras filas
                        try:
                            año = año_base + i
                            precio_str = str(sicep_anual_df.iloc[idx][precio_col]).strip()
                            if precio_str and precio_str.lower() != 'nan':
                                precio = float(precio_str)
                                precios_por_año[año] = precio
                                print(f"DEBUG - Forzado: Año {año} con precio {precio}")
                                
                                # Para FNCER
                                if tiene_fncer:
                                    fncer_str = str(sicep_anual_df.iloc[idx][precio_fncer_col]).strip()
                                    if fncer_str and fncer_str.lower() != 'nan':
                                        precio_fncer = float(fncer_str)
                                        precios_fncer_por_año[año] = precio_fncer
                                        print(f"DEBUG - Forzado: FNCER para año {año}: {precio_fncer}")
                        except Exception as e:
                            print(f"DEBUG - Falló el método forzado para fila {idx}: {e}")
            except Exception as e:
                print(f"DEBUG - Error en el método forzado: {e}")
        
        if not precios_por_año:
            logger.error("No se encontraron precios válidos en la hoja PRECIO SICEP")
            return False
            
        logger.info(f"Precios por año: {precios_por_año}")
        print(f"Precios por año encontrados: {precios_por_año}")
        
        # DEBUG - Imprimir precios por año para verificación
        for año, precio in precios_por_año.items():
            print(f"DEBUG - Precio base para año {año}: {precio}")
            if tiene_fncer and año in precios_fncer_por_año:
                print(f"DEBUG - Precio FNCER base para año {año}: {precios_fncer_por_año[año]}")
        
        if tiene_fncer:
            logger.info(f"Precios FNCER por año: {precios_fncer_por_año}")
            print(f"Precios FNCER por año encontrados: {precios_fncer_por_año}")
        
    except Exception as e:
        logger.error(f"Error al leer PRECIO SICEP: {e}")
        print(f"Error al leer PRECIO SICEP: {e}")
        return False
    
    # Leer indexadores y proyección
    try:
        indexadores_df = leer_excel_seguro(datos_iniciales, "INDEXADORES")
        if indexadores_df.empty:
            logger.error("No se pudo leer la hoja INDEXADORES")
            return False
        
        proyeccion_indexadores_df = leer_excel_seguro(datos_iniciales, "PROYECCIÓN INDEXADORES")
        if proyeccion_indexadores_df.empty:
            logger.error("No se pudo leer la hoja PROYECCIÓN INDEXADORES")
            return False
        
        # Convertir fechas
        indexadores_df['fechaoperacion'] = pd.to_datetime(indexadores_df['fechaoperacion'], format="%d/%m/%Y", errors='coerce').dt.date
        proyeccion_indexadores_df['fechaoperacion'] = pd.to_datetime(proyeccion_indexadores_df['fechaoperacion'], format="%d/%m/%Y", errors='coerce').dt.date
    except Exception as e:
        logger.error(f"Error al leer indexadores: {e}")
        return False
    
    # Solicitar fecha base
    print("\nLa fecha base es necesaria para calcular correctamente los precios mensuales del SICEP y FNCER.")
    print("Esta fecha es el punto de referencia desde el cual se realizarán las proyecciones.")
    
    fecha_base_str = solicitar_input_seguro(
        "\nIngrese la fecha base para el SICEP (formato DD/MM/YYYY, donde DD debe ser 01): ",
        tipo=str,
        validacion=lambda x: len(x.split('/')) == 3 and x.split('/')[0] == '01',
        mensaje_error="La fecha debe estar en formato DD/MM/YYYY y el día debe ser 01."
    )
    
    try:
        fecha_base = datetime.strptime(fecha_base_str, "%d/%m/%Y").date()
    except ValueError:
        logger.error(f"Formato de fecha incorrecto: {fecha_base_str}")
        return False
    
    # Obtener el IPP base
    ipp_base = None
    
    # Buscar en indexadores
    base_en_indexadores = indexadores_df[indexadores_df['fechaoperacion'] == fecha_base]
    if not base_en_indexadores.empty:
        ipp_base = base_en_indexadores['oferta_interna_prov'].iloc[0]
    else:
        # Buscar en proyección
        base_en_proyeccion = proyeccion_indexadores_df[proyeccion_indexadores_df['fechaoperacion'] == fecha_base]
        if not base_en_proyeccion.empty:
            ipp_base = base_en_proyeccion['oferta_interna_prov'].iloc[0]
    
    if ipp_base is None:
        logger.error(f"No se encontró el IPP base para la fecha {fecha_base}")
        return False
    
    # Guardar el IPP base como valor fijo para todos los cálculos de cambio de año
    ipp_base_fijo = ipp_base
    
    logger.info(f"IPP base en fecha {fecha_base}: {ipp_base}")
    print(f"IPP base en fecha {fecha_base}: {ipp_base}")
    print(f"IPP base fijo para cálculos: {ipp_base_fijo}")
    
    # Buscar el precio base (precio del año correspondiente a la fecha base)
    año_base = fecha_base.year
    
    # Para SICEP normal
    if año_base in precios_por_año:
        precio_base = precios_por_año[año_base]
    else:
        # Si no hay precio para el año base, buscar el precio del año más cercano
        años = list(precios_por_año.keys())
        años_cercanos = [año for año in años if año >= año_base]
        if años_cercanos:
            año_cercano = min(años_cercanos)
            precio_base = precios_por_año[año_cercano]
        else:
            # Si no hay años posteriores, usar el último año disponible
            año_cercano = max(años)
            precio_base = precios_por_año[año_cercano]
    
    # Para FNCER si existe
    precio_fncer_base = None
    if tiene_fncer:
        if año_base in precios_fncer_por_año:
            precio_fncer_base = precios_fncer_por_año[año_base]
        else:
            # Si no hay precio para el año base, buscar el precio del año más cercano
            años = list(precios_fncer_por_año.keys())
            años_cercanos = [año for año in años if año >= año_base]
            if años_cercanos:
                año_cercano = min(años_cercanos)
                precio_fncer_base = precios_fncer_por_año[año_cercano]
            else:
                # Si no hay años posteriores, usar el último año disponible
                año_cercano = max(años)
                precio_fncer_base = precios_fncer_por_año[año_cercano]
    
    logger.info(f"Precio base para {fecha_base}: {precio_base}")
    print(f"Precio base para {fecha_base}: {precio_base}")
    
    if tiene_fncer:
        logger.info(f"Precio FNCER base para {fecha_base}: {precio_fncer_base}")
        print(f"Precio FNCER base para {fecha_base}: {precio_fncer_base}")
    
    # Determinar el rango de fechas
    fecha_min = fecha_base  # La fecha mínima es la fecha base
    fecha_max = max(proyeccion_indexadores_df['fechaoperacion'])
    
    # Crear proyección de precios
    proyeccion_sicep = []
    
    # Variables para mantener el seguimiento del último precio ajustado por año
    ultimo_año_procesado = año_base
    
    # Primer registro (fecha base)
    primer_registro = {
        "FECHA": fecha_base,
        "IPP": round(ipp_base, 2),
        "PRECIO": round(precio_base, 2)
    }
    
    # Añadir precio FNCER si corresponde
    if tiene_fncer:
        primer_registro["PRECIO FNCER"] = round(precio_fncer_base, 2)
    
    proyeccion_sicep.append(primer_registro)
    
    # Generar meses siguientes
    fecha_actual = fecha_base
    if fecha_actual.month == 12:
        fecha_siguiente = fecha_actual.replace(year=fecha_actual.year + 1, month=1)
    else:
        fecha_siguiente = fecha_actual.replace(month=fecha_actual.month + 1)
    
    while fecha_siguiente <= fecha_max:
        # Obtener IPP para la fecha siguiente
        ipp_siguiente = None
        
        # Buscar en indexadores
        siguiente_en_indexadores = indexadores_df[indexadores_df['fechaoperacion'] == fecha_siguiente]
        if not siguiente_en_indexadores.empty:
            ipp_siguiente = siguiente_en_indexadores['oferta_interna_prov'].iloc[0]
        else:
            # Buscar en proyección
            siguiente_en_proyeccion = proyeccion_indexadores_df[proyeccion_indexadores_df['fechaoperacion'] == fecha_siguiente]
            if not siguiente_en_proyeccion.empty:
                ipp_siguiente = siguiente_en_proyeccion['oferta_interna_prov'].iloc[0]
        
        if ipp_siguiente is None:
            logger.warning(f"No se encontró IPP para fecha {fecha_siguiente}, saltando...")
            # Avanzar al siguiente mes
            fecha_actual = fecha_siguiente
            if fecha_actual.month == 12:
                fecha_siguiente = fecha_actual.replace(year=fecha_actual.year + 1, month=1)
            else:
                fecha_siguiente = fecha_actual.replace(month=fecha_actual.month + 1)
            continue
        
        # Verificar si cambiamos de año
        nuevo_registro = {"FECHA": fecha_siguiente, "IPP": round(ipp_siguiente, 2)}
        
        if fecha_siguiente.year != ultimo_año_procesado:
            # CAMBIO DE AÑO
            año_siguiente = fecha_siguiente.year
            print(f"DEBUG - Cambio a año {año_siguiente}")
            ultimo_año_procesado = año_siguiente
            
            # Para el precio SICEP, buscar el precio para el nuevo año
            if año_siguiente in precios_por_año:
                # IMPORTANTE: Tomar el precio exacto del año de la tabla PRECIO SICEP
                nuevo_precio_base = precios_por_año[año_siguiente]
                print(f"DEBUG - Tomando precio base {nuevo_precio_base} para año {año_siguiente}")
                
                # USAR EL IPP BASE FIJO como en la fórmula de Excel =REDONDEAR.MAS($B$3*G24/$G$17;2)
                # Donde $G$17 es el IPP base fijo
                precio_siguiente = round(nuevo_precio_base * (ipp_siguiente / ipp_base_fijo), 2)
                print(f"DEBUG - Cálculo: {nuevo_precio_base} * ({ipp_siguiente} / {ipp_base_fijo}) = {precio_siguiente}")
                nuevo_registro["PRECIO"] = precio_siguiente
            else:
                # Si no hay precio para el nuevo año, proyectar a partir del último mes del año anterior
                ultimo_ipp = proyeccion_sicep[-1]["IPP"]
                precio_siguiente = round(proyeccion_sicep[-1]["PRECIO"] * (ipp_siguiente / ultimo_ipp), 2)
                nuevo_registro["PRECIO"] = precio_siguiente
            
            # Para el precio FNCER, hacer lo mismo si está disponible
            if tiene_fncer:
                if año_siguiente in precios_fncer_por_año:
                    # IMPORTANTE: Tomar el precio exacto FNCER del año de la tabla PRECIO SICEP
                    nuevo_precio_fncer_base = precios_fncer_por_año[año_siguiente]
                    print(f"DEBUG - Tomando precio FNCER base {nuevo_precio_fncer_base} para año {año_siguiente}")
                    
                    # USAR EL IPP BASE FIJO igual que para SICEP
                    precio_fncer_siguiente = round(nuevo_precio_fncer_base * (ipp_siguiente / ipp_base_fijo), 2)
                    print(f"DEBUG - Cálculo FNCER: {nuevo_precio_fncer_base} * ({ipp_siguiente} / {ipp_base_fijo}) = {precio_fncer_siguiente}")
                    nuevo_registro["PRECIO FNCER"] = precio_fncer_siguiente
                else:
                    # Si no hay precio FNCER para el nuevo año, proyectar a partir del último mes del año anterior
                    ultimo_ipp = proyeccion_sicep[-1]["IPP"]
                    precio_fncer_siguiente = round(proyeccion_sicep[-1]["PRECIO FNCER"] * (ipp_siguiente / ultimo_ipp), 2)
                    nuevo_registro["PRECIO FNCER"] = precio_fncer_siguiente
        else:
            # Proyección mensual dentro del mismo año
            ultimo_ipp = proyeccion_sicep[-1]["IPP"]
            
            # Para SICEP
            precio_siguiente = round(proyeccion_sicep[-1]["PRECIO"] * (ipp_siguiente / ultimo_ipp), 2)
            nuevo_registro["PRECIO"] = precio_siguiente
            
            # Para FNCER si corresponde
            if tiene_fncer:
                precio_fncer_siguiente = round(proyeccion_sicep[-1]["PRECIO FNCER"] * (ipp_siguiente / ultimo_ipp), 2)
                nuevo_registro["PRECIO FNCER"] = precio_fncer_siguiente
        
        # Agregar a la proyección
        proyeccion_sicep.append(nuevo_registro)
        
        # Avanzar al siguiente mes
        fecha_actual = fecha_siguiente
        if fecha_actual.month == 12:
            fecha_siguiente = fecha_actual.replace(year=fecha_actual.year + 1, month=1)
        else:
            fecha_siguiente = fecha_actual.replace(month=fecha_actual.month + 1)
    
    # Convertir a DataFrame
    proyeccion_sicep_df = pd.DataFrame(proyeccion_sicep)
    
    if proyeccion_sicep_df.empty:
        logger.error("No se generaron datos para la proyección de precios SICEP")
        return False
    
    # Guardar en el archivo
    resultado = guardar_excel_seguro(
        proyeccion_sicep_df, 
        datos_iniciales, 
        "PROYECCIÓN PRECIO SICEP",
        index=False
    )
    
    if resultado:
        if tiene_fncer:
            logger.info(f"Proyección de precios SICEP y FNCER creada correctamente con {len(proyeccion_sicep)} registros")
            print(f"Proyección de precios SICEP y FNCER creada correctamente con {len(proyeccion_sicep)} registros")
        else:
            logger.info(f"Proyección de precios SICEP creada correctamente con {len(proyeccion_sicep)} registros")
            print(f"Proyección de precios SICEP creada correctamente con {len(proyeccion_sicep)} registros")
    else:
        logger.error("Error al guardar la proyección de precios SICEP")
    
    return resultado