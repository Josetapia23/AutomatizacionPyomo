"""
Módulo para el manejo de indexadores y sus proyecciones.
Incluye funciones para calcular, proyectar y gestionar datos de indexadores.
"""

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