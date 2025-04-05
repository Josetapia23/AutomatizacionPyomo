"""
Módulo para el procesamiento de ofertas.
Incluye funciones para leer, procesar y evaluar ofertas.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
from pathlib import Path

from config import DATOS_INICIALES, OFERTAS_DIR, RESULTADO_OFERTAS
from core.utils import (
    verificar_archivo_existe,
    verificar_hoja_existe,
    leer_excel_seguro,
    guardar_excel_seguro,
    solicitar_input_seguro,
    fecha_a_texto
)
from core.indexadores import calcular_numerador, calcular_denominador

logger = logging.getLogger(__name__)

def procesar_precio_sicep(datos_iniciales=DATOS_INICIALES):
    """
    Procesa la hoja PRECIO SICEP. Si no existe, la crea con el formato correcto.
    
    Args:
        datos_iniciales (Path): Ruta al archivo de datos iniciales
        
    Returns:
        DataFrame: DataFrame con el precio SICEP, o None en caso de error
    """
    logger.info("Procesando PRECIO SICEP...")
    
    # Verificar que el archivo existe
    if not verificar_archivo_existe(datos_iniciales):
        logger.error(f"No se encontró el archivo {datos_iniciales}")
        return None
    
    # Si la hoja PRECIO SICEP ya existe, simplemente leerla
    if verificar_hoja_existe(datos_iniciales, "PRECIO SICEP"):
        sicep_df = leer_excel_seguro(datos_iniciales, "PRECIO SICEP")
        if not sicep_df.empty:
            logger.info("Hoja PRECIO SICEP existente leída correctamente")
            return sicep_df
    
    # Si no existe o está vacía, crearla
    logger.info("Creando hoja PRECIO SICEP...")
    
    # Solicitar la fecha base al usuario
    fecha_base_str = solicitar_input_seguro(
        "Ingrese la fecha base del SICEP (formato DD/MM/YYYY): ",
        tipo=str,
        validacion=lambda x: len(x.split('/')) == 3,
        mensaje_error="Formato de fecha incorrecto. Use DD/MM/YYYY."
    )
    
    # Convertir la fecha base a objeto datetime
    try:
        fecha_base = datetime.strptime(fecha_base_str, "%d/%m/%Y").date()
    except ValueError:
        logger.error(f"Formato de fecha incorrecto: {fecha_base_str}")
        return None
    
    # Crear fechas manualmente para evitar el error
    hoy = datetime.now().date()
    fechas = []
    
    # Intenta obtener fechas de DEMANDA
    try:
        demanda_df = leer_excel_seguro(datos_iniciales, "DEMANDA")
        if not demanda_df.empty and 'FECHA' in demanda_df.columns:
            # Convertir fechas explícitamente
            fechas_temp = pd.to_datetime(demanda_df['FECHA']).dt.date
            # Convertir a lista para evitar problemas con arrays
            fechas = list(fechas_temp.unique())
    except Exception as e:
        logger.warning(f"Error al leer fechas de DEMANDA: {e}")
    
    # Si no se obtuvieron fechas, crear fechas genéricas
    if not fechas:
        fechas = [datetime(hoy.year, m, 1).date() for m in range(1, 13)]
    
    # Crear DataFrame para PRECIO SICEP
    sicep_data = []
    precio_base = 100.0
    
    for fecha in fechas:
        # Calcular precio basado en diferencia de meses
        try:
            meses_diff = (fecha.year - fecha_base.year) * 12 + fecha.month - fecha_base.month
            factor = 1 + (meses_diff * 0.01)  # 1% mensual
            precio = precio_base * max(factor, 0.8)
            
            sicep_data.append({
                "FECHA": fecha,
                "PRECIO": round(precio, 2)
            })
        except Exception as e:
            logger.error(f"Error al calcular precio para fecha {fecha}: {e}")
    
    # Crear DataFrame
    sicep_df = pd.DataFrame(sicep_data)
    
    # Guardar en archivo
    resultado = guardar_excel_seguro(
        sicep_df,
        datos_iniciales,
        "PRECIO SICEP",
        index=False
    )
    
    if resultado:
        logger.info("Hoja PRECIO SICEP creada y guardada correctamente")
        return sicep_df
    else:
        logger.error("Error al guardar la hoja PRECIO SICEP")
        return None

def evaluar_oferta(precio_indexado, precio_sicep, precio_bolsa, k_factor):
    """
    Evalúa si una oferta cumple con los criterios establecidos.
    
    Args:
        precio_indexado (float): Precio indexado de la oferta
        precio_sicep (float): Precio SICEP
        precio_bolsa (float): Precio BOLSA
        k_factor (float): Factor k para la evaluación
        
    Returns:
        int: 1 si cumple, 0 si no cumple
    """
    if (precio_indexado is None or 
        precio_sicep is None or 
        precio_bolsa is None):
        return 0  # No cumple si falta algún dato
    
    # Aplicar la nueva condición: PRECIO_INDEXADO ≤ MIN(k· PRECIO_SICEP, PRECIO_BOLSA)
    limite = min(k_factor * precio_sicep, precio_bolsa)
    
    if precio_indexado <= limite:
        return 1  # Cumple
    else:
        return 0  # No cumple

def procesar_ofertas(carpeta_ofertas=OFERTAS_DIR, datos_iniciales=DATOS_INICIALES, 
                    archivo_salida=RESULTADO_OFERTAS):
    """
    Lee todos los archivos de ofertas en la carpeta especificada,
    construye la TABLA MAESTRA OFERTAS y la hoja CANTIDADES Y PRECIOS.
    
    Args:
        carpeta_ofertas (Path): Carpeta donde se encuentran las ofertas
        datos_iniciales (Path): Ruta al archivo de datos iniciales
        archivo_salida (Path): Ruta al archivo de resultados
        
    Returns:
        bool: True si el procesamiento fue exitoso, False en caso contrario
    """
    logger.info(f"Procesando ofertas en {carpeta_ofertas}")
    
    # Verificar que los archivos existan
    if not verificar_archivo_existe(datos_iniciales):
        return False
    
    # Solicitar el factor k al usuario
    k_factor = solicitar_input_seguro(
        "Ingrese el factor k para la evaluación de ofertas: ",
        tipo=float,
        validacion=lambda x: x > 0,
        mensaje_error="El factor k debe ser un número positivo."
    )
    
    # Buscar archivos de ofertas
    archivos = [f for f in os.listdir(carpeta_ofertas) if f.endswith('.xlsx')]
    if not archivos:
        logger.error(f"No se encontraron archivos en {carpeta_ofertas}")
        return False
    
    # Leer los indexadores y proyecciones
    indexadores_df = leer_excel_seguro(datos_iniciales, "INDEXADORES")
    if indexadores_df.empty:
        return False
    
    proyeccion_df = leer_excel_seguro(datos_iniciales, "PROYECCIÓN INDEXADORES")
    if proyeccion_df.empty:
        logger.warning("No se encontró la hoja PROYECCIÓN INDEXADORES, se creará automáticamente")
        
        # Intentar crear la proyección
        from core.indexadores import crear_proyeccion_indexadores
        if not crear_proyeccion_indexadores(datos_iniciales, carpeta_ofertas):
            return False
        
        # Leer la proyección recién creada
        proyeccion_df = leer_excel_seguro(datos_iniciales, "PROYECCIÓN INDEXADORES")
        if proyeccion_df.empty:
            return False
    
    # Leer las hojas PRECIO SICEP y P BOLSA
    sicep_df = leer_excel_seguro(datos_iniciales, "PRECIO SICEP")
    if sicep_df.empty:
        logger.warning("No se encontró la hoja PRECIO SICEP, se procesará automáticamente")
        sicep_df = procesar_precio_sicep(datos_iniciales)
        if sicep_df is None:
            return False
    
    bolsa_df = leer_excel_seguro(datos_iniciales, "P BOLSA")
    if bolsa_df.empty:
        logger.error(f"No se encontró la hoja P BOLSA en {datos_iniciales}")
        return False
    
    # Convertir columnas de fechas
    indexadores_df['fechaoperacion'] = pd.to_datetime(indexadores_df['fechaoperacion'], format="%d/%m/%Y").dt.date
    proyeccion_df['fechaoperacion'] = pd.to_datetime(proyeccion_df['fechaoperacion'], format="%d/%m/%Y").dt.date
    
    # Preparar sicep_df y bolsa_df para búsqueda rápida
    try:
        sicep_df['FECHA'] = pd.to_datetime(sicep_df['FECHA'], format="%d/%m/%Y").dt.date
        sicep_df['AUX'] = sicep_df['FECHA'].apply(lambda d: f"{d.year}-{str(d.month).zfill(2)}")
        sicep_dict = sicep_df.groupby('AUX')['PRECIO'].sum().to_dict()
    except Exception as e:
        logger.error(f"Error al procesar PRECIO SICEP: {e}")
        return False
    
    try:
        bolsa_df['FECHA'] = pd.to_datetime(bolsa_df['FECHA'], format="%d/%m/%Y").dt.date
        bolsa_df['AUX'] = bolsa_df['FECHA'].apply(lambda d: f"{d.year}-{str(d.month).zfill(2)}")
        bolsa_dict = bolsa_df.groupby('AUX')['PBNA'].sum().to_dict()
    except Exception as e:
        logger.error(f"Error al procesar P BOLSA: {e}")
        return False
    
    # Inicializar listas para resultados
    tabla_maestra = []
    cantidades_precios = []
    
    # Procesar cada archivo de oferta
    for archivo in archivos:
        codigo_oferta = os.path.splitext(archivo)[0]
        ruta_archivo = os.path.join(carpeta_ofertas, archivo)
        
        logger.info(f"Procesando oferta: {codigo_oferta}")
        
        # Leer las hojas necesarias
        try:
            indexador_df = leer_excel_seguro(ruta_archivo, "INDEXADOR")
            cantidad_df = leer_excel_seguro(ruta_archivo, "cantidad")
            precios_df = leer_excel_seguro(ruta_archivo, "precios")
            
            if indexador_df.empty or cantidad_df.empty or precios_df.empty:
                logger.error(f"Error al leer las hojas de {ruta_archivo}")
                continue
            
            # Limpiar nombres de columnas en precios_df
            precios_df.columns = precios_df.columns.str.replace(r"\$/KWh-", "", regex=True)
            
            # Convertir fechas
            cantidad_df['FECHA'] = pd.to_datetime(cantidad_df['FECHA'], format="%d/%m/%Y").dt.date
            precios_df['FECHA'] = pd.to_datetime(precios_df['FECHA'], format="%d/%m/%Y").dt.date
            
            # Extraer datos del indexador
            indexador_data = {
                "CÓDIGO OFERTA": codigo_oferta,
                "INDEXADOR": indexador_df.loc[indexador_df["CONCEPTO"] == "INDEXADOR", "VALOR"].values[0],
                "NUMERADOR": indexador_df.loc[indexador_df["CONCEPTO"] == "NUMERADOR", "VALOR"].values[0],
                "DENOMINADOR": indexador_df.loc[indexador_df["CONCEPTO"] == "DENOMINADOR", "VALOR"].values[0],
                "FECHA BASE": pd.to_datetime(indexador_df.loc[indexador_df["CONCEPTO"] == "FECHA BASE", "VALOR"].values[0]).date()
            }
            tabla_maestra.append(indexador_data)
        except Exception as e:
            logger.error(f"Error al procesar metadatos de la oferta {codigo_oferta}: {e}")
            continue
        
        # Procesar cada fila de cantidad_df y cada hora
        for _, row in cantidad_df.iterrows():
            try:
                fecha = row['FECHA']
                
                # Verificar que la fecha sea válida
                if pd.isna(fecha):
                    logger.warning(f"Se encontró una fecha nula en la oferta {codigo_oferta}, omitiendo esta entrada")
                    continue
                
                # Construimos la clave año-mes para buscar en sicep_dict y bolsa_dict
                fecha_aux = f"{fecha.year}-{str(fecha.month).zfill(2)}"
                
                for hora in range(1, 25):
                    # Obtener precio para esta hora y fecha
                    precio_hora = precios_df.loc[precios_df['FECHA'] == fecha, f"H{hora}"].values
                    precio_hora = precio_hora[0] if len(precio_hora) > 0 else None
                    
                    # Calcular numerador y denominador
                    try:
                        numerador_valor = calcular_numerador(
                            fecha,
                            indexador_data["INDEXADOR"],
                            indexador_data["NUMERADOR"],
                            indexadores_df,
                            proyeccion_df
                        )
                        
                        denominador_valor = calcular_denominador(
                            indexador_data["FECHA BASE"],
                            indexador_data["INDEXADOR"],
                            indexador_data["DENOMINADOR"],
                            indexadores_df,
                            proyeccion_df
                        )
                        
                        # Calcular precio indexado
                        if (
                            precio_hora is not None
                            and numerador_valor is not None
                            and denominador_valor is not None
                            and denominador_valor != 0
                        ):
                            precio_indexado = (precio_hora + 0) * ((numerador_valor + 0) / (denominador_valor + 0))
                        else:
                            precio_indexado = None
                        
                        # Obtener PRECIO SICEP para ese año-mes
                        precio_sicep_val = sicep_dict.get(fecha_aux, 0)
                        
                        # Obtener PRECIO BOLSA para ese año-mes
                        precio_bolsa_val = bolsa_dict.get(fecha_aux, 0)
                        
                        # Evaluacion:
                        evaluacion = evaluar_oferta(
                            precio_indexado,
                            precio_sicep_val,
                            precio_bolsa_val,
                            k_factor
                        )
                        
                        # Construimos el diccionario en el orden que necesitamos
                        fila_resultado = {
                            "CÓDIGO OFERTA": codigo_oferta,
                            "FECHA": fecha,
                            "Atributo": hora,
                            "CANTIDAD": row.get(f"KWH-H{hora}", 0),
                            "PRECIO": precio_hora,
                            "INDEXADOR": indexador_data["INDEXADOR"],
                            "NUMERADOR": indexador_data["NUMERADOR"],
                            "DENOMINADOR": indexador_data["DENOMINADOR"],
                            "FECHA BASE": indexador_data["FECHA BASE"],
                            "NUMERADOR #": numerador_valor,
                            "DENOMINADOR #": denominador_valor,
                            "PRECIO INDEXADO": precio_indexado,
                            "PRECIO SICEP": precio_sicep_val,
                            "PRECIO BOLSA": precio_bolsa_val,
                            "EVALUACIÓN": evaluacion
                        }
                        
                        cantidades_precios.append(fila_resultado)
                    except Exception as e:
                        logger.error(f"Error al procesar hora {hora} fecha {fecha} oferta {codigo_oferta}: {e}")
                        continue
            except Exception as e:
                logger.error(f"Error al procesar fila en oferta {codigo_oferta}: {e}")
                continue
    
    # Convertir a DataFrames
    tabla_maestra_df = pd.DataFrame(tabla_maestra)
    cantidades_precios_df = pd.DataFrame(cantidades_precios)
    
    # Verificar que tengamos datos para guardar
    if tabla_maestra_df.empty or cantidades_precios_df.empty:
        logger.error("No se generaron datos para guardar")
        return False
    
    # Guardar en archivo de salida
    try:
        # Crear directorios si no existen
        Path(archivo_salida).parent.mkdir(parents=True, exist_ok=True)
        
        with pd.ExcelWriter(archivo_salida, engine="openpyxl") as writer:
            tabla_maestra_df.to_excel(writer, sheet_name="TABLA MAESTRA OFERTAS", index=False)
            cantidades_precios_df.to_excel(writer, sheet_name="CANTIDADES Y PRECIOS", index=False)
        
        logger.info(f"Resultados guardados en {archivo_salida}")
        return True
    except Exception as e:
        logger.error(f"Error al guardar resultados: {e}")
        return False