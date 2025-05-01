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
    
    # Verificar si la hoja de proyección ya existe y eliminarla si es así
    eliminar_hoja_si_existe(datos_iniciales, "PROYECCIÓN INDEXADORES")
    
    # Leer los indexadores
    indexadores_df = leer_excel_seguro(datos_iniciales, sheet_name="INDEXADORES")
    if indexadores_df.empty:
        logger.error("No se pudo leer la hoja INDEXADORES del archivo de datos iniciales")
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
    
    # Convertir columnas de fechas
    indexadores_df['fechaoperacion'] = pd.to_datetime(indexadores_df['fechaoperacion'], format="%d/%m/%Y").dt.date
    cantidad_df['FECHA'] = pd.to_datetime(cantidad_df['FECHA'], format="%d/%m/%Y").dt.date
    
    # Obtener la última fecha de indexadores y la última fecha de cantidad
    fecha_mayor_indexadores = indexadores_df['fechaoperacion'].max()
    fila_base = indexadores_df.loc[indexadores_df['fechaoperacion'] == fecha_mayor_indexadores].iloc[0]
    fecha_mayor_cantidad = cantidad_df['FECHA'].max()
    
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
    fecha_actual = fecha_mayor_indexadores
    
    oferta_interna_prov = fila_base['oferta_interna_prov']
    oferta_interna_def = fila_base['oferta_interna_def']
    ipc = fila_base['ipc']
    
    # Proyectar mes a mes
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
    
    # Guardar en el archivo
    resultado = guardar_excel_seguro(
        proyeccion_df, 
        datos_iniciales, 
        "PROYECCIÓN INDEXADORES",
        index=False
    )
    
    if resultado:
        logger.info("Proyección de indexadores creada correctamente")
    else:
        logger.error("Error al guardar la proyección de indexadores")
    
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
        sicep_anual_df = leer_excel_seguro(datos_iniciales, "PRECIO SICEP")
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
        
        # Asegurarse de que los años sean numéricos
        sicep_anual_df[año_col] = pd.to_numeric(sicep_anual_df[año_col], errors='coerce')
        sicep_anual_df = sicep_anual_df.dropna(subset=[año_col])
        
        # Extraer los precios por año
        precios_por_año = {}
        precios_fncer_por_año = {}
        
        for _, row in sicep_anual_df.iterrows():
            año = int(row[año_col])  # Convertir a entero para usar como clave
            precio = float(row[precio_col])  # Convertir a float para cálculos
            precios_por_año[año] = precio
            
            # Registrar precio FNCER si existe
            if tiene_fncer:
                precio_fncer = float(row[precio_fncer_col])
                precios_fncer_por_año[año] = precio_fncer
            
        if not precios_por_año:
            logger.error("No se encontraron precios válidos en la hoja PRECIO SICEP")
            return False
            
        logger.info(f"Precios por año: {precios_por_año}")
        print(f"Precios por año encontrados: {precios_por_año}")
        
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
    
    logger.info(f"IPP base en fecha {fecha_base}: {ipp_base}")
    print(f"IPP base en fecha {fecha_base}: {ipp_base}")
    
    # Buscar el precio base (precio del año correspondiente a la fecha base o precio mínimo)
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
        
        if fecha_siguiente.day == 1 and fecha_siguiente.month == 1:
            # SICEP: Usar el precio del nuevo año si está disponible
            año_siguiente = fecha_siguiente.year
            
            if año_siguiente in precios_por_año:
                # Usar el precio de la tabla para este nuevo año
                nuevo_precio_base = precios_por_año[año_siguiente]
                
                # Calcular el nuevo precio usando el nuevo precio y la relación de índices
                precio_siguiente = round(nuevo_precio_base * (ipp_siguiente / ipp_base), 2)
                
                # Actualizar el registro
                nuevo_registro["PRECIO"] = precio_siguiente
            else:
                # Si no hay precio para el nuevo año, continuar con la proyección normal
                precio_anterior = proyeccion_sicep[-1]["PRECIO"]
                ipp_anterior = proyeccion_sicep[-1]["IPP"]
                
                precio_siguiente = round(precio_anterior * (ipp_siguiente / ipp_anterior), 2)
                nuevo_registro["PRECIO"] = precio_siguiente
            
            # FNCER: Usar el precio del nuevo año si está disponible
            if tiene_fncer:
                if año_siguiente in precios_fncer_por_año:
                    # Usar el precio FNCER de la tabla para este nuevo año
                    nuevo_precio_fncer_base = precios_fncer_por_año[año_siguiente]
                    
                    # Calcular el nuevo precio FNCER
                    precio_fncer_siguiente = round(nuevo_precio_fncer_base * (ipp_siguiente / ipp_base), 2)
                    
                    # Actualizar el registro
                    nuevo_registro["PRECIO FNCER"] = precio_fncer_siguiente
                else:
                    # Si no hay precio FNCER para el nuevo año, continuar con la proyección normal
                    precio_fncer_anterior = proyeccion_sicep[-1]["PRECIO FNCER"]
                    ipp_anterior = proyeccion_sicep[-1]["IPP"]
                    
                    precio_fncer_siguiente = round(precio_fncer_anterior * (ipp_siguiente / ipp_anterior), 2)
                    nuevo_registro["PRECIO FNCER"] = precio_fncer_siguiente
        else:
            # Proyección mensual normal para SICEP
            precio_anterior = proyeccion_sicep[-1]["PRECIO"]
            ipp_anterior = proyeccion_sicep[-1]["IPP"]
            
            precio_siguiente = round(precio_anterior * (ipp_siguiente / ipp_anterior), 2)
            nuevo_registro["PRECIO"] = precio_siguiente
            
            # Proyección mensual normal para FNCER si corresponde
            if tiene_fncer:
                precio_fncer_anterior = proyeccion_sicep[-1]["PRECIO FNCER"]
                precio_fncer_siguiente = round(precio_fncer_anterior * (ipp_siguiente / ipp_anterior), 2)
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