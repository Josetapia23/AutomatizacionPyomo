"""
Funciones auxiliares para la generación de visualizaciones.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
import os
from pathlib import Path

# Configurar logging
logger = logging.getLogger(__name__)

def format_number(value, decimals=2):
    """
    Formatea un número con separadores de miles y decimales específicos.
    
    Args:
        value (float): Valor a formatear
        decimals (int): Número de decimales
        
    Returns:
        str: Valor formateado
    """
    if pd.isna(value):
        return "N/A"
    
    try:
        return f"{value:,.{decimals}f}"
    except (ValueError, TypeError):
        return str(value)

def convert_to_gwh(value_kwh):
    """
    Convierte un valor de kWh a GWh.
    
    Args:
        value_kwh (float): Valor en kWh
        
    Returns:
        float: Valor en GWh
    """
    if pd.isna(value_kwh):
        return 0
    
    return value_kwh / 1_000_000

def extract_dates_from_results(resultados_dict):
    """
    Extrae fechas únicas ordenadas desde los resultados.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        list: Lista de fechas ordenadas
    """
    fechas = set()
    
    # Extraer fechas de todas las hojas que contienen fechas
    for key, df in resultados_dict.items():
        if isinstance(df, pd.DataFrame) and not df.empty and "FECHA" in df.columns:
            fechas.update(df["FECHA"].unique())
    
    # Ordenar fechas
    return sorted(list(fechas))

def extract_hours_from_results(resultados_dict):
    """
    Extrae horas únicas ordenadas desde los resultados.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        list: Lista de horas ordenadas
    """
    horas = set()
    
    # Extraer horas de todas las hojas
    for key, df in resultados_dict.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            for col in df.columns:
                if isinstance(col, int) and 1 <= col <= 24:
                    horas.add(col)
    
    # Si no se encontraron horas, usar el rango completo 1-24
    if not horas:
        horas = set(range(1, 25))
    
    # Ordenar horas
    return sorted(list(horas))

def extract_offers_from_results(resultados_dict):
    """
    Extrae ofertas únicas desde los resultados.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        list: Lista de ofertas
    """
    ofertas = set()
    
    # Extraer ofertas de todas las hojas que contienen "DEMANDA ASIGNADA"
    for key in resultados_dict.keys():
        if "DEMANDA ASIGNADA" in key:
            # Extraer nombre de la oferta
            partes = key.split("DEMANDA ASIGNADA ")
            if len(partes) > 1:
                oferta = partes[1].split(" IT")[0]
                ofertas.add(oferta)
    
    # Si no se encontraron ofertas, buscar en el resumen ejecutivo
    if not ofertas and "RESUMEN EJECUTIVO" in resultados_dict:
        df_resumen = resultados_dict["RESUMEN EJECUTIVO"]
        for col in df_resumen.columns:
            if "CANTIDAD" in col:
                oferta = col.split(" CANTIDAD")[0]
                ofertas.add(oferta)
    
    return sorted(list(ofertas))

def extract_years_months_from_dates(fechas):
    """
    Extrae años y meses únicos de una lista de fechas.
    
    Args:
        fechas (list): Lista de fechas
        
    Returns:
        tuple: (años, meses)
    """
    años = set()
    meses = set()
    
    for fecha in fechas:
        if hasattr(fecha, 'year') and hasattr(fecha, 'month'):
            años.add(fecha.year)
            meses.add(fecha.month)
        elif isinstance(fecha, str) and '/' in fecha:
            # Intentar extraer año y mes de un string formato "MM/YYYY"
            partes = fecha.split('/')
            if len(partes) == 2:
                try:
                    mes = int(partes[0])
                    año = int(partes[1])
                    meses.add(mes)
                    años.add(año)
                except ValueError:
                    pass
    
    return sorted(list(años)), sorted(list(meses))

def format_date_for_display(fecha):
    """
    Formatea una fecha para mostrar en gráficas.
    
    Args:
        fecha: Fecha a formatear (datetime.date o string)
        
    Returns:
        str: Fecha formateada
    """
    if hasattr(fecha, 'strftime'):
        return fecha.strftime('%d/%m/%Y')
    elif isinstance(fecha, str):
        # Si ya es string, verificar si tiene el formato deseado
        if '/' in fecha and len(fecha.split('/')) >= 2:
            return fecha
    
    # En caso de que no se pueda formatear
    return str(fecha)

def generate_color_scale(num_colors, start_color="#1A5276", end_color="#D4AC0D"):
    """
    Genera una escala de colores interpolando entre dos colores.
    
    Args:
        num_colors (int): Número de colores a generar
        start_color (str): Color inicial en formato hex
        end_color (str): Color final en formato hex
        
    Returns:
        list: Lista de colores en formato hex
    """
    import matplotlib.colors as mcolors
    
    # Convertir colores a RGB
    start_rgb = mcolors.hex2color(start_color)
    end_rgb = mcolors.hex2color(end_color)
    
    # Crear escala
    colors = []
    for i in range(num_colors):
        r = start_rgb[0] + (end_rgb[0] - start_rgb[0]) * i / (num_colors - 1)
        g = start_rgb[1] + (end_rgb[1] - start_rgb[1]) * i / (num_colors - 1)
        b = start_rgb[2] + (end_rgb[2] - start_rgb[2]) * i / (num_colors - 1)
        colors.append(mcolors.rgb2hex((r, g, b)))
    
    return colors

def ensure_directory_exists(directory_path):
    """
    Asegura que un directorio exista, creándolo si es necesario.
    
    Args:
        directory_path (str o Path): Ruta del directorio
    """
    path = Path(directory_path)
    path.mkdir(parents=True, exist_ok=True)
    return path