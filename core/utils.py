"""
Funciones auxiliares para el procesamiento de datos y operaciones comunes.
"""

import os
import pandas as pd
import logging
from pathlib import Path
import openpyxl
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def verificar_archivo_existe(ruta):
    """
    Verifica si un archivo existe en la ruta especificada.
    
    Args:
        ruta (str o Path): Ruta al archivo a verificar
        
    Returns:
        bool: True si el archivo existe, False en caso contrario
    """
    ruta = Path(ruta)
    existe = ruta.exists()
    if not existe:
        logger.warning(f"El archivo {ruta} no existe.")
    return existe

def verificar_hoja_existe(archivo_excel, nombre_hoja):
    """
    Verifica si una hoja específica existe en un archivo Excel.
    
    Args:
        archivo_excel (str o Path): Ruta al archivo Excel
        nombre_hoja (str): Nombre de la hoja a verificar
        
    Returns:
        bool: True si la hoja existe, False en caso contrario
    """
    try:
        xls = pd.ExcelFile(archivo_excel)
        existe = nombre_hoja in xls.sheet_names
        if not existe:
            logger.warning(f"La hoja '{nombre_hoja}' no existe en el archivo {archivo_excel}")
        return existe
    except Exception as e:
        logger.error(f"Error al verificar la hoja '{nombre_hoja}' en {archivo_excel}: {e}")
        return False

def eliminar_hoja_si_existe(archivo_excel, nombre_hoja):
    """
    Elimina una hoja de un archivo Excel si existe.
    
    Args:
        archivo_excel (str o Path): Ruta al archivo Excel
        nombre_hoja (str): Nombre de la hoja a eliminar
    
    Returns:
        bool: True si la hoja fue eliminada, False si no existía o hubo un error
    """
    try:
        if not verificar_archivo_existe(archivo_excel):
            return False
            
        libro = openpyxl.load_workbook(archivo_excel)
        if nombre_hoja in libro.sheetnames:
            hoja = libro[nombre_hoja]
            libro.remove(hoja)
            libro.save(archivo_excel)
            logger.info(f"Hoja '{nombre_hoja}' eliminada de {archivo_excel}")
            return True
        else:
            logger.info(f"La hoja '{nombre_hoja}' no existe en {archivo_excel}, no es necesario eliminarla")
            return False
    except Exception as e:
        logger.error(f"Error al eliminar la hoja '{nombre_hoja}' de {archivo_excel}: {e}")
        return False

def leer_excel_seguro(archivo, hoja=0, **kwargs):
    """
    Lee un archivo Excel de manera segura, manejando errores comunes.
    
    Args:
        archivo (str o Path): Ruta al archivo Excel
        hoja (str o int): Nombre o índice de la hoja a leer (por defecto: 0)
        **kwargs: Argumentos adicionales para pd.read_excel
        
    Returns:
        DataFrame: DataFrame con los datos leídos, o DataFrame vacío en caso de error
    """
    try:
        # Asegurarse de que sheet_name no esté duplicado en kwargs
        if 'sheet_name' in kwargs:
            logger.warning("Se está sobreescribiendo el parámetro 'sheet_name' en leer_excel_seguro")
            # Usar el sheet_name proporcionado en kwargs, no el parámetro hoja
            return pd.read_excel(archivo, **kwargs)
        else:
            # Usar el parámetro hoja como sheet_name
            return pd.read_excel(archivo, sheet_name=hoja, **kwargs)
    except Exception as e:
        logger.error(f"Error al leer {archivo} (hoja: {hoja}): {e}")
        return pd.DataFrame()

def guardar_excel_seguro(df, archivo, hoja, index=False, **kwargs):
    """
    Guarda un DataFrame en un archivo Excel de manera segura.
    
    Args:
        df (DataFrame): DataFrame a guardar
        archivo (str o Path): Ruta al archivo Excel
        hoja (str): Nombre de la hoja donde guardar
        index (bool): Si se debe incluir el índice del DataFrame
        **kwargs: Argumentos adicionales para df.to_excel
        
    Returns:
        bool: True si se guardó correctamente, False en caso contrario
    """
    try:
        archivo = Path(archivo)
        archivo.parent.mkdir(parents=True, exist_ok=True)
        
        # Determinar si el archivo existe para elegir el modo
        if archivo.exists():
            # Si el archivo existe, usamos el modo de anexar
            with pd.ExcelWriter(archivo, engine='openpyxl', mode='a', 
                              if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name=hoja, index=index, **kwargs)
        else:
            # Si el archivo no existe, lo creamos
            with pd.ExcelWriter(archivo, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=hoja, index=index, **kwargs)
                
        logger.info(f"DataFrame guardado correctamente en {archivo} (hoja: {hoja})")
        return True
    except Exception as e:
        logger.error(f"Error al guardar DataFrame en {archivo} (hoja: {hoja}): {e}")
        return False

def solicitar_input_seguro(mensaje, tipo=str, validacion=None, mensaje_error=None):
    """
    Solicita input al usuario y lo convierte al tipo especificado, con validación opcional.
    
    Args:
        mensaje (str): Mensaje a mostrar al usuario
        tipo (type): Tipo de dato esperado (str, int, float, etc.)
        validacion (callable, opcional): Función de validación que recibe el valor y retorna bool
        mensaje_error (str, opcional): Mensaje a mostrar si la validación falla
        
    Returns:
        Valor del tipo especificado ingresado por el usuario
    """
    while True:
        try:
            valor = tipo(input(mensaje))
            if validacion is None or validacion(valor):
                return valor
            else:
                print(mensaje_error or "Valor inválido. Intente nuevamente.")
        except ValueError:
            print(f"Por favor, ingrese un valor de tipo {tipo.__name__}")
        except Exception as e:
            print(f"Error: {e}")

def fecha_a_texto(fecha, formato="%Y-%m"):
    """
    Convierte una fecha a texto en el formato especificado.
    
    Args:
        fecha (date): Fecha a convertir
        formato (str): Formato de salida (por defecto: "%Y-%m")
        
    Returns:
        str: Fecha formateada como texto
    """
    return fecha.strftime(formato)

def texto_a_fecha(texto, formato="%Y-%m"):
    """
    Convierte un texto a fecha en el formato especificado.
    
    Args:
        texto (str): Texto a convertir
        formato (str): Formato de entrada (por defecto: "%Y-%m")
        
    Returns:
        date: Fecha resultante
    """
    return datetime.strptime(texto, formato).date()

def ultimo_dia_mes(fecha):
    """
    Obtiene el último día del mes para una fecha dada.
    
    Args:
        fecha (date): Fecha de referencia
        
    Returns:
        date: Último día del mes
    """
    # Ir al primer día del mes siguiente y restar un día
    siguiente_mes = datetime(fecha.year + (fecha.month // 12), 
                           ((fecha.month % 12) + 1), 1).date()
    return siguiente_mes - timedelta(days=1)