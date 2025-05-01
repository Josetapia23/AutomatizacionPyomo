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
from core.indexadores import calcular_numerador, calcular_denominador, crear_proyeccion_precio_sicep

logger = logging.getLogger(__name__)

def procesar_precio_sicep(datos_iniciales=DATOS_INICIALES):
    """
    Procesa los precios SICEP y FNCER y crea un diccionario para su uso en la evaluación de ofertas.
    Ahora usa la hoja 'PROYECCIÓN PRECIO SICEP' si existe, o la crea si no existe.
    
    Args:
        datos_iniciales (Path): Ruta al archivo de datos iniciales
        
    Returns:
        dict: Diccionario con los valores de PRECIO y PRECIO FNCER por año-mes, o None en caso de error
    """
    logger.info(f"Procesando PRECIO SICEP y FNCER desde {datos_iniciales}")
    
    # Verificar que el archivo existe
    if not verificar_archivo_existe(datos_iniciales):
        logger.error(f"No se encontró el archivo de datos iniciales: {datos_iniciales}")
        return None
    
    # Verificar si la proyección ya existe
    if not verificar_hoja_existe(datos_iniciales, "PROYECCIÓN PRECIO SICEP"):
        logger.info("No se encontró la hoja PROYECCIÓN PRECIO SICEP, se creará...")
        
        # Verificar si existe la hoja con precios anuales
        if not verificar_hoja_existe(datos_iniciales, "PRECIO SICEP"):
            logger.error("No se encontró la hoja PRECIO SICEP con los precios anuales")
            return None
        
        # Llamar a la función para crear la proyección
        if not crear_proyeccion_precio_sicep(datos_iniciales):
            logger.error("No se pudo crear la proyección de precios SICEP")
            return None
    
    # Leer la proyección de precios SICEP
    try:
        sicep_df = leer_excel_seguro(datos_iniciales, "PROYECCIÓN PRECIO SICEP")
        if sicep_df.empty:
            logger.error("La hoja PROYECCIÓN PRECIO SICEP está vacía")
            return None
        
        # Convertir la columna FECHA a datetime.date
        sicep_df['FECHA'] = pd.to_datetime(sicep_df['FECHA'], errors='coerce').dt.date
        
        # Verificar si hay fechas inválidas
        if sicep_df['FECHA'].isna().any():
            logger.warning("Hay fechas en formato incorrecto en la hoja PROYECCIÓN PRECIO SICEP")
            sicep_df = sicep_df.dropna(subset=['FECHA'])
        
        # Verificar la estructura de columnas
        if 'PRECIO' not in sicep_df.columns:
            logger.error("No se encontró la columna 'PRECIO' en la hoja PROYECCIÓN PRECIO SICEP")
            return None
            
        # Crear columna auxiliar para agrupar por año-mes
        sicep_df['AUX'] = sicep_df['FECHA'].apply(lambda d: f"{d.year}-{d.month}")
        
        # Crear diccionario simple con los valores de PRECIO
        sicep_dict = dict(zip(sicep_df['AUX'], sicep_df['PRECIO']))
        
        # Crear diccionario para valores FNCER si existe la columna
        fncer_dict = {}
        if 'PRECIO FNCER' in sicep_df.columns:
            fncer_dict = dict(zip(sicep_df['AUX'], sicep_df['PRECIO FNCER']))
            logger.info(f"PRECIO SICEP y FNCER procesados correctamente: {len(sicep_dict)} períodos")
            print(f"PRECIO SICEP y FNCER procesados correctamente: {len(sicep_dict)} períodos")
        else:
            logger.info(f"PRECIO SICEP procesado correctamente: {len(sicep_dict)} períodos (no se encontró PRECIO FNCER)")
            print(f"PRECIO SICEP procesado correctamente: {len(sicep_dict)} períodos (no se encontró PRECIO FNCER)")
        
        return {'SICEP': sicep_dict, 'FNCER': fncer_dict}
    
    except Exception as e:
        logger.exception(f"Error al procesar PROYECCIÓN PRECIO SICEP: {e}")
        print(f"Error al procesar PROYECCIÓN PRECIO SICEP: {e}")
        return None

def procesar_precio_bolsa(datos_iniciales=DATOS_INICIALES):
    """
    Procesa la hoja P BOLSA del archivo de datos iniciales.
    
    Args:
        datos_iniciales (Path): Ruta al archivo de datos iniciales
        
    Returns:
        dict: Diccionario con los valores de PBNA por año-mes, o None en caso de error
    """
    logger.info(f"Procesando PRECIO BOLSA desde {datos_iniciales}")
    
    # Verificar que el archivo existe
    if not verificar_archivo_existe(datos_iniciales):
        logger.error(f"No se encontró el archivo de datos iniciales: {datos_iniciales}")
        return None
    
    # Verificar que la hoja P BOLSA existe
    if not verificar_hoja_existe(datos_iniciales, "P BOLSA"):
        logger.error(f"No se encontró la hoja 'P BOLSA' en el archivo: {datos_iniciales}")
        return None
    
    try:
        # Leer la hoja P BOLSA
        bolsa_df = leer_excel_seguro(datos_iniciales, "P BOLSA")
        if bolsa_df.empty:
            logger.error("La hoja 'P BOLSA' está vacía")
            return None
        
        # Convertir la columna FECHA a datetime.date
        bolsa_df['FECHA'] = pd.to_datetime(bolsa_df['FECHA'], format="%d/%m/%Y", errors='coerce').dt.date
        
        # Verificar si hay fechas inválidas
        if bolsa_df['FECHA'].isna().any():
            logger.warning("Hay fechas en formato incorrecto en la hoja 'P BOLSA'")
            # Filtrar solo las fechas válidas
            bolsa_df = bolsa_df.dropna(subset=['FECHA'])
        
        # Crear columna auxiliar para agrupar por año-mes
        bolsa_df['AUX'] = bolsa_df['FECHA'].apply(lambda d: f"{d.year}-{d.month}")
        
        # Crear diccionario con los valores de PBNA
        bolsa_dict = dict(zip(bolsa_df['AUX'], bolsa_df['PBNA']))
        
        logger.info(f"PRECIO BOLSA procesado correctamente: {len(bolsa_dict)} períodos")
        print(f"PRECIO BOLSA procesado correctamente: {len(bolsa_dict)} períodos")
        
        return bolsa_dict
    
    except Exception as e:
        logger.exception(f"Error al procesar PRECIO BOLSA: {e}")
        return None
    
def evaluar_oferta(precio_indexado, precio_sicep, precio_bolsa, constante_sicep=None, precio_fncer=None, es_oferta_fncer=False):
    """
    Evalúa si una oferta cumple con los criterios establecidos.
    Para ofertas normales, se compara con min(constante_sicep * PRECIO_SICEP, PRECIO_BOLSA).
    Para ofertas FNCER, se compara con PRECIO_FNCER.
    
    Args:
        precio_indexado (float): Precio indexado de la oferta
        precio_sicep (float): Precio SICEP
        precio_bolsa (float): Precio BOLSA
        constante_sicep (float, opcional): Constante para multiplicar el precio SICEP
        precio_fncer (float, opcional): Precio FNCER para evaluar ofertas FNCER
        es_oferta_fncer (bool): Indica si la oferta es de tipo FNCER
        
    Returns:
        int: 1 si cumple, 0 si no cumple
    """
    # Inicializar evaluación como 0 (incumple)
    evaluacion = 0
    
    # Verificar que precio indexado sea válido
    if precio_indexado is None or pd.isna(precio_indexado):
        # Solo registrar si el precio indexado no es válido
        logger.debug(f"Precio indexado no válido: {precio_indexado}")
        return evaluacion  # Si no hay precio indexado, no cumple
    
    # Evaluación para ofertas FNCER
    if es_oferta_fncer:
        if precio_fncer is not None and precio_fncer > 0:
            # Para ofertas FNCER, se compara directamente con el precio FNCER
            if precio_indexado <= precio_fncer:
                evaluacion = 1  # Cumple criterio FNCER
            # Registrar solo si es importante para depuración o si cambia el resultado
            if logger.getEffectiveLevel() <= logging.DEBUG:
                logger.debug(f"Evaluación FNCER: Precio indexado {precio_indexado} <= Precio FNCER {precio_fncer}: {evaluacion}")
        else:
            logger.warning(f"Oferta marcada como FNCER pero precio FNCER no disponible o es 0. Se usará evaluación normal.")
            # Caer en evaluación normal si no hay precio FNCER disponible
            if precio_sicep is not None and precio_sicep > 0 and precio_bolsa is not None and precio_bolsa > 0:
                if constante_sicep is None:
                    constante_sicep = 1.0
                
                precio_sicep_ajustado = precio_sicep * constante_sicep
                limite = min(precio_sicep_ajustado, precio_bolsa)
                
                if precio_indexado <= limite:
                    evaluacion = 1
                
                if logger.getEffectiveLevel() <= logging.DEBUG:
                    logger.debug(f"Evaluación FNCER fallback: Precio indexado {precio_indexado} <= min({precio_sicep_ajustado}, {precio_bolsa}): {evaluacion}")
    else:
        # Evaluación para ofertas normales (no FNCER)
        if precio_sicep is not None and precio_bolsa is not None:
            # Solo advertir si ambos son cero
            if precio_sicep == 0 and precio_bolsa == 0:
                logger.warning(f"Ambos precios SICEP y BOLSA son 0")
                return evaluacion
            
            # Si alguno de los dos es mayor que cero, continuar con la evaluación
            if precio_sicep > 0 or precio_bolsa > 0:
                # Si no se proporcionó constante, usar valor predeterminado
                if constante_sicep is None:
                    constante_sicep = 1.0
                
                # Aplicar la constante al precio SICEP
                precio_sicep_ajustado = precio_sicep * constante_sicep
                
                # Usar precio que no sea cero si uno es cero
                if precio_sicep_ajustado == 0 and precio_bolsa > 0:
                    limite = precio_bolsa
                elif precio_bolsa == 0 and precio_sicep_ajustado > 0:
                    limite = precio_sicep_ajustado
                else:
                    # Evaluación: PRECIO_INDEXADO <= min(constante_sicep * PRECIO_SICEP, PRECIO_BOLSA)
                    limite = min(precio_sicep_ajustado, precio_bolsa)
                
                if precio_indexado <= limite:
                    evaluacion = 1  # Cumple
                
                # Solo registrar en nivel de depuración
                if logger.getEffectiveLevel() <= logging.DEBUG:
                    logger.debug(f"Evaluación normal: Precio indexado {precio_indexado} <= límite {limite}: {evaluacion}")
            else:
                logger.warning(f"Faltan datos para evaluación normal: Precio SICEP: {precio_sicep}, Precio BOLSA: {precio_bolsa}")
        else:
            logger.warning(f"Faltan datos para evaluación normal: Precio SICEP: {precio_sicep}, Precio BOLSA: {precio_bolsa}")
    
    return evaluacion

def procesar_ofertas(carpeta_ofertas=OFERTAS_DIR, datos_iniciales=DATOS_INICIALES, 
                    archivo_salida=RESULTADO_OFERTAS):
    """
    Lee todos los archivos de ofertas en la carpeta especificada,
    construye la TABLA MAESTRA OFERTAS y la hoja CANTIDADES Y PRECIOS.
    Ahora incluye procesamiento de ofertas FNCER.
    
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
        logger.error(f"No se encontró el archivo de datos iniciales: {datos_iniciales}")
        return False
    
    if not os.path.exists(carpeta_ofertas):
        logger.error(f"No se encontró la carpeta de ofertas: {carpeta_ofertas}")
        return False
    
    # Solicitar la constante SICEP al usuario
    try:
        constante_sicep = solicitar_input_seguro(
            "Ingrese la constante para el cálculo del precio SICEP: ",
            tipo=float,
            validacion=lambda x: x > 0,
            mensaje_error="La constante debe ser un número positivo."
        )
        print(f"Usando constante SICEP: {constante_sicep}")
    except Exception as e:
        logger.warning(f"Error al solicitar constante SICEP: {e}. Se usará el valor predeterminado de 1.0")
        constante_sicep = 1.0
        print(f"Usando constante SICEP predeterminada: {constante_sicep}")
    
    # Buscar archivos de ofertas
    archivos = [f for f in os.listdir(carpeta_ofertas) if f.endswith('.xlsx')]
    if not archivos:
        logger.error(f"No se encontraron archivos en {carpeta_ofertas}")
        return False
    
    # Leer los indexadores y proyecciones
    indexadores_df = leer_excel_seguro(datos_iniciales, "INDEXADORES")
    if indexadores_df.empty:
        logger.error(f"No se encontró o está vacía la hoja INDEXADORES en {datos_iniciales}")
        return False
    
    proyeccion_df = leer_excel_seguro(datos_iniciales, "PROYECCIÓN INDEXADORES")
    if proyeccion_df.empty:
        logger.warning("No se encontró la hoja PROYECCIÓN INDEXADORES, se creará automáticamente")
        
        # Intentar crear la proyección
        from core.indexadores import crear_proyeccion_indexadores
        if not crear_proyeccion_indexadores(datos_iniciales, carpeta_ofertas):
            logger.error("No se pudo crear la proyección de indexadores")
            return False
        
        # Leer la proyección recién creada
        proyeccion_df = leer_excel_seguro(datos_iniciales, "PROYECCIÓN INDEXADORES")
        if proyeccion_df.empty:
            logger.error("La proyección de indexadores está vacía")
            return False
    
    # Procesar PRECIO SICEP
    sicep_dict = procesar_precio_sicep(datos_iniciales)
    if sicep_dict is None:
        logger.error("No se pudo procesar PRECIO SICEP")
        return False
    
    # Procesar PRECIO BOLSA
    bolsa_dict = procesar_precio_bolsa(datos_iniciales)
    if bolsa_dict is None:
        logger.error("No se pudo procesar PRECIO BOLSA")
        return False
    
    # Convertir columnas de fechas a tipo datetime.date
    indexadores_df['fechaoperacion'] = pd.to_datetime(indexadores_df['fechaoperacion'], format="%d/%m/%Y").dt.date
    proyeccion_df['fechaoperacion'] = pd.to_datetime(proyeccion_df['fechaoperacion'], format="%d/%m/%Y").dt.date
    
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
            
            # Verificar si existe el campo FNCER en el indexador y obtener su valor
            fncer_rows = indexador_df[indexador_df["CONCEPTO"] == "FNCER"]
            if not fncer_rows.empty:
                es_fncer = fncer_rows["VALOR"].values[0].upper() == "SI"
                indexador_data["FNCER"] = "SI" if es_fncer else "NO"
                logger.info(f"Oferta {codigo_oferta} FNCER: {'SI' if es_fncer else 'NO'}")
            else:
                indexador_data["FNCER"] = "NO"
                logger.info(f"Oferta {codigo_oferta} no tiene campo FNCER, asignando NO por defecto")
            
            tabla_maestra.append(indexador_data)
        except Exception as e:
            logger.error(f"Error al procesar metadatos de la oferta {codigo_oferta}: {e}")
            continue
        
        # Determinar si esta oferta es de tipo FNCER
        es_fncer = indexador_data.get("FNCER", "NO") == "SI"
        
        # Procesar cada fila de cantidad_df y cada hora
        for _, row in cantidad_df.iterrows():
            try:
                fecha = row['FECHA']
                
                # Verificar que la fecha sea válida
                if pd.isna(fecha):
                    logger.warning(f"Se encontró una fecha nula en la oferta {codigo_oferta}, omitiendo esta entrada")
                    continue
                
                # Construimos la clave año-mes para buscar en sicep_dict y bolsa_dict
                fecha_aux = f"{fecha.year}-{fecha.month}"
                
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
                        precio_sicep_val = sicep_dict.get('SICEP', {}).get(fecha_aux, 0)
                        
                        # Si es oferta FNCER, obtener precio FNCER
                        precio_fncer_val = None
                        if es_fncer:
                            precio_fncer_val = sicep_dict.get('FNCER', {}).get(fecha_aux, 0)
                            # Si no hay precio FNCER, usar un valor predeterminado o registrar mensaje
                            if precio_fncer_val == 0:
                                logger.warning(f"No se encontró precio FNCER para {fecha_aux} en oferta {codigo_oferta}")
                        
                        # Obtener PRECIO BOLSA para ese año-mes
                        precio_bolsa_val = bolsa_dict.get(fecha_aux, 0)
                        
                        # Evaluación usando la función evaluar_oferta con la constante SICEP
                        evaluacion = evaluar_oferta(
                            precio_indexado,
                            precio_sicep_val,
                            precio_bolsa_val,
                            constante_sicep,
                            precio_fncer=precio_fncer_val,
                            es_oferta_fncer=es_fncer
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
                            "FNCER": indexador_data.get("FNCER", "NO"),
                            "PRECIO SICEP": precio_sicep_val if not es_fncer else precio_fncer_val,
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
        print(f"Se procesaron {len(tabla_maestra)} ofertas con {len(cantidades_precios)} registros")
        return True
    except Exception as e:
        logger.error(f"Error al guardar resultados: {e}")
        return False