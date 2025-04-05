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
    Procesa la hoja PRECIO SICEP del archivo de datos iniciales.
    Si la hoja no existe, la crea con valores calculados siguiendo 
    el formato del archivo de ejemplo.
    
    Args:
        datos_iniciales (Path): Ruta al archivo de datos iniciales
        
    Returns:
        dict: Diccionario con los valores de PRECIO por año-mes, o None en caso de error
    """
    logger.info(f"Procesando PRECIO SICEP desde {datos_iniciales}")
    
    # Verificar que el archivo existe
    if not verificar_archivo_existe(datos_iniciales):
        logger.error(f"No se encontró el archivo de datos iniciales: {datos_iniciales}")
        return None
    
    # Verificar si la hoja PRECIO SICEP existe
    if verificar_hoja_existe(datos_iniciales, "PRECIO SICEP"):
        try:
            # Leer la hoja PRECIO SICEP
            sicep_df = leer_excel_seguro(datos_iniciales, "PRECIO SICEP")
            if sicep_df.empty:
                logger.warning("La hoja PRECIO SICEP está vacía, se creará con valores calculados")
            else:
                # Convertir la columna FECHA a datetime.date
                sicep_df['FECHA'] = pd.to_datetime(sicep_df['FECHA'], format="%d/%m/%Y", errors='coerce').dt.date
                
                # Verificar si hay fechas inválidas
                if sicep_df['FECHA'].isna().any():
                    logger.warning("Hay fechas en formato incorrecto en la hoja PRECIO SICEP")
                    sicep_df = sicep_df.dropna(subset=['FECHA'])
                
                # Crear columna auxiliar para agrupar por año-mes si no existe
                if 'AUX' not in sicep_df.columns:
                    sicep_df['AUX'] = sicep_df['FECHA'].apply(lambda d: f"{d.year}-{d.month}")
                
                # Crear diccionario con los valores de PRECIO
                sicep_dict = dict(zip(sicep_df['AUX'], sicep_df['PRECIO']))
                
                logger.info(f"PRECIO SICEP procesado correctamente: {len(sicep_dict)} períodos")
                print(f"PRECIO SICEP procesado correctamente: {len(sicep_dict)} períodos")
                
                return sicep_dict
        except Exception as e:
            logger.error(f"Error al leer la hoja PRECIO SICEP: {e}")
            # Continuamos para crear la hoja
    
    # Si llegamos aquí, necesitamos crear la hoja PRECIO SICEP
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
    
    # Solicitar el valor IPP base
    ipp_base = solicitar_input_seguro(
        "Ingrese el valor IPP base (ej. 178.5): ",
        tipo=float,
        validacion=lambda x: x > 0,
        mensaje_error="El valor IPP debe ser un número positivo."
    )
    
    # Solicitar el valor PRECIO base
    precio_base = solicitar_input_seguro(
        "Ingrese el valor PRECIO base (ej. 330): ",
        tipo=float,
        validacion=lambda x: x > 0,
        mensaje_error="El valor PRECIO debe ser un número positivo."
    )
    
    # Crear fechas para la proyección
    fechas = []
    
    # Intenta obtener fechas de DEMANDA
    try:
        demanda_df = leer_excel_seguro(datos_iniciales, "DEMANDA")
        if not demanda_df.empty and 'FECHA' in demanda_df.columns:
            # Convertir fechas explícitamente
            fechas_temp = pd.to_datetime(demanda_df['FECHA']).dt.date
            # Obtener el primer día de cada mes único
            fechas_mes = set()
            for fecha in fechas_temp:
                primer_dia = datetime(fecha.year, fecha.month, 1).date()
                fechas_mes.add(primer_dia)
            fechas = sorted(list(fechas_mes))
    except Exception as e:
        logger.warning(f"Error al leer fechas de DEMANDA: {e}")
    
    # Si no se obtuvieron fechas, crear fechas genéricas para 24 meses desde la fecha base
    if not fechas:
        fechas = []
        fecha_actual = fecha_base
        for _ in range(24):  # Crear 24 meses
            fechas.append(fecha_actual)
            # Avanzar al primer día del mes siguiente
            if fecha_actual.month == 12:
                fecha_actual = datetime(fecha_actual.year + 1, 1, 1).date()
            else:
                fecha_actual = datetime(fecha_actual.year, fecha_actual.month + 1, 1).date()
    
    # Crear DataFrame para PRECIO SICEP
    sicep_data = []
    
    # Valor inicial de IPP y PRECIO
    ipp_actual = ipp_base
    precio_actual = precio_base
    
    # Incremento mensual aproximado (0.5% para IPP, 0.33% para PRECIO)
    incremento_ipp = 0.005  # 0.5% mensual
    incremento_precio = 0.0033  # 0.33% mensual
    
    for fecha in fechas:
        # Calcular IPP y PRECIO basado en incrementos mensuales
        meses_diff = (fecha.year - fecha_base.year) * 12 + fecha.month - fecha_base.month
        
        if meses_diff == 0:
            # Primer mes (valores base)
            ipp = ipp_base
            precio = precio_base
        else:
            # Para meses posteriores, aplicar incremento
            ipp = ipp_base * (1 + incremento_ipp * meses_diff)
            precio = precio_base * (1 + incremento_precio * meses_diff)
        
        # Crear entrada para este mes
        sicep_data.append({
            "FECHA": fecha,
            "IPP": round(ipp, 2),
            "PRECIO": round(precio, 2),
            "AUX": f"{fecha.year}-{fecha.month}"
        })
    
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
        
        # Crear el diccionario de retorno con valores de PRECIO
        sicep_dict = dict(zip(sicep_df['AUX'], sicep_df['PRECIO']))
        
        return sicep_dict
    else:
        logger.error("Error al guardar la hoja PRECIO SICEP")
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
    
def evaluar_oferta(precio_indexado, precio_sicep, precio_bolsa, k_factor=1.5):
    """
    Evalúa si una oferta cumple con los criterios establecidos.
    
    Args:
        precio_indexado (float): Precio indexado de la oferta
        precio_sicep (float): Precio SICEP
        precio_bolsa (float): Precio BOLSA
        k_factor (float, opcional): Factor k para la evaluación
        
    Returns:
        int: 1 si cumple, 0 si no cumple
    """
    if (precio_indexado is None or 
        precio_sicep is None or 
        precio_bolsa is None):
        return 0  # No cumple si falta algún dato
    
    # Se implementan dos criterios diferentes:
    
    # 1. Criterio original (comportamiento por defecto):
    # Condition A: PRECIO_INDEXADO >= PRECIO_SICEP and PRECIO_INDEXADO <= PRECIO_BOLSA
    cond_a = (precio_indexado >= precio_sicep) and (precio_indexado <= precio_bolsa)
    # Condition B: PRECIO_INDEXADO <= PRECIO_SICEP
    cond_b = (precio_indexado <= precio_sicep)
    
    if cond_a or cond_b:
        return 1  # Cumple
    
    # 2. Criterio alternativo con factor k (si se especifica):
    if k_factor != 1.5:  # Si se usa un factor k personalizado
        # PRECIO_INDEXADO ≤ MIN(k· PRECIO_SICEP, PRECIO_BOLSA)
        limite = min(k_factor * precio_sicep, precio_bolsa)
        
        if precio_indexado <= limite:
            return 1  # Cumple
    
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
        logger.error(f"No se encontró el archivo de datos iniciales: {datos_iniciales}")
        return False
    
    if not os.path.exists(carpeta_ofertas):
        logger.error(f"No se encontró la carpeta de ofertas: {carpeta_ofertas}")
        return False
    
    # Solicitar el factor k al usuario
    try:
        k_factor = solicitar_input_seguro(
            "Ingrese el factor k para la evaluación de ofertas (1.5 recomendado): ",
            tipo=float,
            validacion=lambda x: x > 0,
            mensaje_error="El factor k debe ser un número positivo."
        )
    except Exception as e:
        logger.warning(f"Error al solicitar factor k: {e}. Se usará el valor predeterminado de 1.5")
        k_factor = 1.5
        print(f"Usando factor k predeterminado: {k_factor}")
    
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
                        precio_sicep_val = sicep_dict.get(fecha_aux, 0)
                        
                        # Obtener PRECIO BOLSA para ese año-mes
                        precio_bolsa_val = bolsa_dict.get(fecha_aux, 0)
                        
                        # Evaluación usando la función evaluar_oferta
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
        print(f"Se procesaron {len(tabla_maestra)} ofertas con {len(cantidades_precios)} registros")
        return True
    except Exception as e:
        logger.error(f"Error al guardar resultados: {e}")
        return False