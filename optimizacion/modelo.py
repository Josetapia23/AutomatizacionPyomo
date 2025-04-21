"""
Módulo para la construcción y extracción de resultados del modelo de optimización con Pyomo.
"""

import pyomo.environ as pyo
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime


logger = logging.getLogger(__name__)

def construir_modelo(demanda_df, ofertas_df):
    """
    Construye el modelo de optimización utilizando Pyomo.
    
    Args:
        demanda_df (DataFrame): DataFrame con los datos de demanda
        ofertas_df (DataFrame): DataFrame con los datos de ofertas
        
    Returns:
        ConcreteModel: Modelo de Pyomo construido
    """
    logger.info("Construyendo modelo de optimización...")
    print("Construyendo modelo de optimización...")
    
    # Crear modelo concreto
    model = pyo.ConcreteModel(name="AsignacionOfertas")
    
    # Preprocesar los datos
    # Convertir a tipo fecha si es necesario
    if isinstance(demanda_df['FECHA'].iloc[0], str):
        demanda_df['FECHA'] = pd.to_datetime(demanda_df['FECHA']).dt.date
    
    if isinstance(ofertas_df['FECHA'].iloc[0], str):
        ofertas_df['FECHA'] = pd.to_datetime(ofertas_df['FECHA']).dt.date
    
    # Obtener índices únicos
    ofertas = sorted(ofertas_df['CÓDIGO OFERTA'].unique())
    fechas = sorted(demanda_df['FECHA'].unique())
    horas = sorted(demanda_df['HORA'].unique())
    
    # Imprimir ofertas para verificar
    print("Ofertas disponibles para optimización:", ofertas)
    
    # Crear diccionarios para acceder rápidamente a los datos
    demanda_dict = {}
    for _, row in demanda_df.iterrows():
        fecha = row['FECHA']
        hora = row['HORA']
        demanda_dict[(fecha, hora)] = row['DEMANDA']
    
    # Filtrar ofertas que cumplen con la evaluación (EVALUACIÓN = 1)
    ofertas_validas_df = ofertas_df[ofertas_df['EVALUACIÓN'] == 1].copy()
    
    # Crear diccionarios para ofertas válidas
    precio_dict = {}
    cantidad_dict = {}
    oferta_valida_dict = {}
    
    for _, row in ofertas_validas_df.iterrows():
        oferta = row['CÓDIGO OFERTA']
        fecha = row['FECHA']
        hora = row['Atributo']
        
        # Solo ofertas con precio indexado y cantidad válidos
        if pd.notna(row['PRECIO INDEXADO']) and pd.notna(row['CANTIDAD']) and row['CANTIDAD'] > 0:
            precio_dict[(oferta, fecha, hora)] = row['PRECIO INDEXADO']
            cantidad_dict[(oferta, fecha, hora)] = row['CANTIDAD']
            oferta_valida_dict[(oferta, fecha, hora)] = 1
    
    # Definir conjuntos
    model.I = pyo.Set(initialize=ofertas, doc='Índice de ofertas')
    model.A = pyo.Set(initialize=fechas, doc='Índice de fechas')
    model.H = pyo.Set(initialize=horas, doc='Índice de horas')
    
    # Definir combinaciones válidas de ofertas, fechas y horas
    def ofertas_fechas_horas_filter(model, i, a, h):
        return (i, a, h) in oferta_valida_dict
    
    model.OFH = pyo.Set(
        initialize=model.I * model.A * model.H,
        dimen=3,
        filter=ofertas_fechas_horas_filter,
        doc='Combinaciones válidas de oferta-fecha-hora'
    )
    
    # Definir parámetros
    # Demanda en cada período
    model.D = pyo.Param(
        model.A, model.H,
        initialize=lambda model, a, h: demanda_dict.get((a, h), 0),
        default=0,
        doc='Demanda para cada fecha y hora'
    )
    
    # Precio de oferta
    model.PO = pyo.Param(
        model.OFH,
        initialize=lambda model, i, a, h: precio_dict.get((i, a, h), 0),
        default=0,
        doc='Precio de oferta'
    )
    
    # Cantidad de oferta
    model.CO = pyo.Param(
        model.OFH,
        initialize=lambda model, i, a, h: cantidad_dict.get((i, a, h), 0),
        default=0,
        doc='Cantidad de oferta'
    )
    
    # Constante grande para restricciones big-M
    model.M = pyo.Param(initialize=1e10, doc='Constante para restricciones big-M')
    
    # Asignar prioridades a las ofertas de manera dinámica
    prioridad_dict = {}
    print("Asignando prioridades a ofertas de manera dinámica:")

    # Ordenar ofertas alfabéticamente para tener un orden consistente
    ofertas_ordenadas = sorted(ofertas)

    # Asignación automática: simplemente numerarlas por orden alfabético
    for i, oferta in enumerate(ofertas_ordenadas, 1):
        prioridad_dict[oferta] = i
        print(f"  Oferta: {oferta} - Prioridad: {i}")

    # Imprimir resumen de prioridades asignadas
    print("\nPrioridades finales asignadas a ofertas:")
    for oferta, prioridad in sorted(prioridad_dict.items(), key=lambda x: x[1]):
        print(f"  {oferta}: Prioridad {prioridad}")
    
    # Añadir parámetro de prioridad al modelo
    model.prioridad = pyo.Param(
        model.I,
        initialize=lambda model, i: prioridad_dict.get(i, 999),
        doc='Prioridad de asignación para cada oferta'
    )
    
    # Definir variables
    # Variable de energía asignada
    model.EA = pyo.Var(
        model.OFH,
        domain=pyo.NonNegativeReals,
        doc='Energía asignada para cada oferta, fecha y hora'
    )
    
    # Variable binaria para determinar si una oferta es aceptada o no
    model.Y = pyo.Var(
        model.OFH,
        domain=pyo.Binary,
        doc='Variable binaria: 1 si oferta es aceptada, 0 si no'
    )
    
    # Variable para déficit de energía
    model.ENA = pyo.Var(
        model.A, model.H,
        domain=pyo.NonNegativeReals,
        doc='Energía no asignada (déficit) para cada fecha y hora'
    )
    
    # Función objetivo: Minimizar el costo total considerando prioridades
    def objetivo_rule(model):
        # Costo base por energía
        costo_energia = sum(
            model.PO[i, a, h] * model.EA[i, a, h]
            for (i, a, h) in model.OFH
        )
        
        # Penalización por déficit (muy alto para forzar uso de ofertas disponibles)
        penalizacion_deficit = sum(
            model.M * model.ENA[a, h]
            for a in model.A
            for h in model.H
        )
        
        # Factor de prioridad (multiplicamos por un valor pequeño para no distorsionar el costo)
        factor_prioridad = sum(
            (model.prioridad[i] * 0.001) * model.EA[i, a, h]
            for (i, a, h) in model.OFH
        )
        
        return costo_energia + penalizacion_deficit + factor_prioridad
    
    model.Objetivo = pyo.Objective(rule=objetivo_rule, sense=pyo.minimize)
    
    # Restricción de balance de demanda
    def balance_demanda_rule(model, a, h):
        # Suma de energía asignada para todas las ofertas en este período
        energia_asignada = sum(
            model.EA[i, a, h]
            for i in model.I
            if (i, a, h) in model.OFH
        )
        
        # Debe ser igual a la demanda menos el déficit
        return energia_asignada + model.ENA[a, h] == model.D[a, h]
    
    model.RestriccionDemanda = pyo.Constraint(
        model.A, model.H,
        rule=balance_demanda_rule,
        doc='Restricción de balance de demanda'
    )
    
    # Restricción de límite de asignación
    def limite_asignacion_rule(model, i, a, h):
        if (i, a, h) in model.OFH:
            # La energía asignada no puede superar la cantidad ofertada
            return model.EA[i, a, h] <= model.CO[i, a, h]
        else:
            return pyo.Constraint.Skip
    
    model.RestriccionAsignacion = pyo.Constraint(
        model.I, model.A, model.H,
        rule=limite_asignacion_rule,
        doc='Restricción de límite de asignación'
    )
    
    # Restricción de variable binaria para asignación
    def binaria_asignacion_rule(model, i, a, h):
        if (i, a, h) in model.OFH:
            # Si Y = 0, entonces EA = 0
            # Si Y = 1, entonces EA <= CO
            return model.EA[i, a, h] <= model.CO[i, a, h] * model.Y[i, a, h]
        else:
            return pyo.Constraint.Skip
    
    model.RestriccionBinariaAsignacion = pyo.Constraint(
        model.I, model.A, model.H,
        rule=binaria_asignacion_rule,
        doc='Restricción de variable binaria para asignación'
    )
    
    logger.info(f"Modelo construido con {len(model.OFH)} combinaciones de ofertas válidas")
    print(f"Modelo construido con {len(model.OFH)} combinaciones de ofertas válidas")
    
    return model

def extraer_resultados(model):
    """
    Extrae los resultados del modelo resuelto y los organiza en múltiples DataFrames
    siguiendo la lógica original de Excel.
    """
    logger.info("Extrayendo resultados del modelo...")
    print("Extrayendo resultados del modelo...")
    
    # Obtener todas las ofertas disponibles
    ofertas_disponibles = list(model.I)
    print(f"Ofertas disponibles en el modelo: {ofertas_disponibles}")
    
    # Obtener las ofertas (suponiendo que BTG y AES siempre están presentes)
    btg_oferta = None
    aes_oferta = None
    
    for oferta in ofertas_disponibles:
        oferta_str = str(oferta).upper()
        if "BTG" in oferta_str or "BTK" in oferta_str:
            btg_oferta = oferta
        elif "AES" in oferta_str:
            aes_oferta = oferta
    
    if not btg_oferta or not aes_oferta:
        print("ADVERTENCIA: No se encontraron ofertas BTG o AES en nombres exactos.")
        print("Buscando ofertas con nombres similares...")
        
        for oferta in ofertas_disponibles:
            oferta_str = str(oferta).upper()
            if not btg_oferta and ("BT" in oferta_str or "WIDE" in oferta_str):
                btg_oferta = oferta
                print(f"  Usando {oferta} como equivalente a BTG")
            elif not aes_oferta and "A" in oferta_str:
                aes_oferta = oferta
                print(f"  Usando {oferta} como equivalente a AES")
    
    if not btg_oferta or not aes_oferta:
        print("ADVERTENCIA: No se encontraron ofertas similares a BTG o AES. Usando las primeras dos ofertas disponibles.")
        if len(ofertas_disponibles) >= 2:
            btg_oferta = ofertas_disponibles[0]
            aes_oferta = ofertas_disponibles[1]
        else:
            print("ERROR: No hay suficientes ofertas para continuar.")
            return {}
    
    print(f"Usando oferta BTG: {btg_oferta}")
    print(f"Usando oferta AES: {aes_oferta}")
    
    # Obtener todas las fechas y horas
    todas_fechas = sorted(list(model.A))
    todas_horas = sorted(list(model.H))
    
    print(f"Procesando {len(todas_fechas)} fechas y {len(todas_horas)} horas")
    
    # Diccionarios para almacenar los resultados
    resultados = {}
    
    try:
        # Primer paso: BTG IT1
        btg_it1_comprar = []
        btg_it1_no_comprada = []
        
        # Para cada fecha y hora, aplicar la lógica de BTG IT1
        for fecha in todas_fechas:
            comprar_row = {"FECHA": fecha, "X": fecha}
            no_comprada_row = {"FECHA": fecha, "X": fecha}
            
            for hora in range(1, 25):
                # Usar try-except para manejar posibles errores al acceder a datos
                try:
                    demanda = pyo.value(model.D[fecha, hora]) if (fecha, hora) in model.D else 0
                    
                    # Lógica para BTG IT1 (primera iteración para BTG)
                    energia_asignada = 0
                    cantidad_btg = 0
                    
                    if (btg_oferta, fecha, hora) in model.OFH:
                        precio_btg = pyo.value(model.PO[btg_oferta, fecha, hora])
                        cantidad_btg = pyo.value(model.CO[btg_oferta, fecha, hora])
                        
                        if (aes_oferta, fecha, hora) in model.OFH:
                            precio_aes = pyo.value(model.PO[aes_oferta, fecha, hora])
                            # Comparar precios
                            if precio_btg <= precio_aes:
                                energia_asignada = min(demanda, cantidad_btg)
                    
                    comprar_row[hora] = energia_asignada
                    no_comprada_row[hora] = max(0, cantidad_btg - energia_asignada)
                    
                except Exception as e:
                    print(f"Error al procesar BTG IT1 - fecha: {fecha}, hora: {hora}: {e}")
                    comprar_row[hora] = 0
                    no_comprada_row[hora] = 0
            
            btg_it1_comprar.append(comprar_row)
            btg_it1_no_comprada.append(no_comprada_row)
        
        # Guardar BTG IT1
        resultados["DEMANDA ASIGNADA BTG IT1_COMPRAR"] = pd.DataFrame(btg_it1_comprar)
        resultados["DEMANDA ASIGNADA BTG IT1_NO_COMPRADA"] = pd.DataFrame(btg_it1_no_comprada)
        
        print("BTG IT1 procesado correctamente")
        
        # Segundo paso: AES IT1
        aes_it1_comprar = []
        aes_it1_no_comprada = []
        
        for fecha in todas_fechas:
            comprar_row = {"FECHA": fecha, "X": fecha}
            no_comprada_row = {"FECHA": fecha, "X": fecha}
            
            for hora in range(1, 25):
                try:
                    demanda = pyo.value(model.D[fecha, hora]) if (fecha, hora) in model.D else 0
                    
                    # Obtener BTG asignado
                    btg_asignado = 0
                    btg_df = resultados["DEMANDA ASIGNADA BTG IT1_COMPRAR"]
                    fecha_rows = btg_df[btg_df["FECHA"] == fecha]
                    if not fecha_rows.empty:
                        btg_asignado = fecha_rows.iloc[0].get(hora, 0)
                    
                    # Demanda restante después de BTG IT1
                    demanda_restante = demanda - btg_asignado
                    
                    # Lógica para AES IT1
                    energia_asignada = 0
                    cantidad_aes = 0
                    
                    if (aes_oferta, fecha, hora) in model.OFH and demanda_restante > 0:
                        precio_aes = pyo.value(model.PO[aes_oferta, fecha, hora])
                        cantidad_aes = pyo.value(model.CO[aes_oferta, fecha, hora])
                        
                        if (btg_oferta, fecha, hora) in model.OFH:
                            precio_btg = pyo.value(model.PO[btg_oferta, fecha, hora])
                            # Comparar precios (inverso a BTG IT1)
                            if precio_aes <= precio_btg:
                                energia_asignada = min(demanda_restante, cantidad_aes)
                    
                    comprar_row[hora] = energia_asignada
                    no_comprada_row[hora] = max(0, cantidad_aes - energia_asignada)
                
                except Exception as e:
                    print(f"Error al procesar AES IT1 - fecha: {fecha}, hora: {hora}: {e}")
                    comprar_row[hora] = 0
                    no_comprada_row[hora] = 0
            
            aes_it1_comprar.append(comprar_row)
            aes_it1_no_comprada.append(no_comprada_row)
        
        # Guardar AES IT1
        resultados["DEMANDA ASIGNADA AES IT1_COMPRAR"] = pd.DataFrame(aes_it1_comprar)
        resultados["DEMANDA ASIGNADA AES IT1_NO_COMPRADA"] = pd.DataFrame(aes_it1_no_comprada)
        
        print("AES IT1 procesado correctamente")
        
        # Tercer paso: BTG IT2
        btg_it2_comprar = []
        btg_it2_no_comprada = []
        
        for fecha in todas_fechas:
            comprar_row = {"FECHA": fecha, "X": fecha}
            no_comprada_row = {"FECHA": fecha, "X": fecha}
            
            for hora in range(1, 25):
                try:
                    demanda = pyo.value(model.D[fecha, hora]) if (fecha, hora) in model.D else 0
                    
                    # Obtener asignaciones previas
                    btg_it1_asignado = 0
                    aes_it1_asignado = 0
                    
                    btg_df = resultados["DEMANDA ASIGNADA BTG IT1_COMPRAR"]
                    btg_fecha_rows = btg_df[btg_df["FECHA"] == fecha]
                    if not btg_fecha_rows.empty:
                        btg_it1_asignado = btg_fecha_rows.iloc[0].get(hora, 0)
                    
                    aes_df = resultados["DEMANDA ASIGNADA AES IT1_COMPRAR"]
                    aes_fecha_rows = aes_df[aes_df["FECHA"] == fecha]
                    if not aes_fecha_rows.empty:
                        aes_it1_asignado = aes_fecha_rows.iloc[0].get(hora, 0)
                    
                    # Demanda restante después de BTG IT1 y AES IT1
                    demanda_restante = demanda - btg_it1_asignado - aes_it1_asignado
                    
                    # Lógica para BTG IT2 (segunda iteración para BTG)
                    energia_asignada = 0
                    
                    if (btg_oferta, fecha, hora) in model.OFH and demanda_restante > 0:
                        # Energía no comprada de BTG en IT1
                        btg_it1_no_comprada_val = 0
                        btg_no_comprada_df = resultados["DEMANDA ASIGNADA BTG IT1_NO_COMPRADA"]
                        btg_no_comprada_rows = btg_no_comprada_df[btg_no_comprada_df["FECHA"] == fecha]
                        if not btg_no_comprada_rows.empty:
                            btg_it1_no_comprada_val = btg_no_comprada_rows.iloc[0].get(hora, 0)
                        
                        # Asignar el mínimo entre la demanda restante y lo no comprado en IT1
                        energia_asignada = min(demanda_restante, btg_it1_no_comprada_val)
                    
                    comprar_row[hora] = energia_asignada
                    no_comprada_row[hora] = max(0, pyo.value(model.CO[btg_oferta, fecha, hora]) - btg_it1_asignado - energia_asignada) if (btg_oferta, fecha, hora) in model.OFH else 0
                
                except Exception as e:
                    print(f"Error al procesar BTG IT2 - fecha: {fecha}, hora: {hora}: {e}")
                    comprar_row[hora] = 0
                    no_comprada_row[hora] = 0
            
            btg_it2_comprar.append(comprar_row)
            btg_it2_no_comprada.append(no_comprada_row)
        
        # Guardar BTG IT2
        resultados["DEMANDA ASIGNADA BTG IT2_COMPRAR"] = pd.DataFrame(btg_it2_comprar)
        resultados["DEMANDA ASIGNADA BTG IT2_NO_COMPRADA"] = pd.DataFrame(btg_it2_no_comprada)
        
        print("BTG IT2 procesado correctamente")
        
        # Cuarto paso: Demanda Faltante
        demanda_faltante = []
        
        for fecha in todas_fechas:
            row = {"FECHA": fecha, "X": fecha}
            
            for hora in range(1, 25):
                try:
                    demanda = pyo.value(model.D[fecha, hora]) if (fecha, hora) in model.D else 0
                    
                    # Sumar todas las asignaciones
                    btg_it1_asignado = 0
                    aes_it1_asignado = 0
                    btg_it2_asignado = 0
                    
                    btg_df = resultados["DEMANDA ASIGNADA BTG IT1_COMPRAR"]
                    btg_fecha_rows = btg_df[btg_df["FECHA"] == fecha]
                    if not btg_fecha_rows.empty:
                        btg_it1_asignado = btg_fecha_rows.iloc[0].get(hora, 0)
                    
                    aes_df = resultados["DEMANDA ASIGNADA AES IT1_COMPRAR"]
                    aes_fecha_rows = aes_df[aes_df["FECHA"] == fecha]
                    if not aes_fecha_rows.empty:
                        aes_it1_asignado = aes_fecha_rows.iloc[0].get(hora, 0)
                    
                    btg2_df = resultados["DEMANDA ASIGNADA BTG IT2_COMPRAR"]
                    btg2_fecha_rows = btg2_df[btg2_df["FECHA"] == fecha]
                    if not btg2_fecha_rows.empty:
                        btg_it2_asignado = btg2_fecha_rows.iloc[0].get(hora, 0)
                    
                    # Calcular demanda faltante
                    faltante = demanda - btg_it1_asignado - aes_it1_asignado - btg_it2_asignado
                    
                    # Si es un valor muy pequeño, considerarlo como 0
                    if abs(faltante) < 1e-6:
                        faltante = 0
                        
                    row[hora] = faltante
                
                except Exception as e:
                    print(f"Error al calcular demanda faltante - fecha: {fecha}, hora: {hora}: {e}")
                    row[hora] = 0
            
            demanda_faltante.append(row)
        
        resultados["DEMANDA_FALTANTE"] = pd.DataFrame(demanda_faltante)
        
        print("Demanda faltante procesada correctamente")
        
        # Quinto paso: RESUMEN (mensuales) - VERSIÓN MEJORADA CON ORDEN CRONOLÓGICO
        resumen_rows = []

        # Agrupar fechas por mes
        fechas_por_mes = {}
        for fecha in todas_fechas:
            mes = fecha.month
            año = fecha.year
            # Guardamos la fecha en formato numérico para poder ordenar correctamente después
            key = (año, mes)  # Tupla (año, mes) para ordenar cronológicamente
            display_key = f"{mes:02d}/{año}"  # Formato MM/YYYY para mostrar
            
            if key not in fechas_por_mes:
                fechas_por_mes[key] = {
                    "display": display_key,
                    "fechas": []
                }
            
            fechas_por_mes[key]["fechas"].append(fecha)

        # Para cada mes, calcular totales - ordenamos por la clave numérica (año, mes)
        for key in sorted(fechas_por_mes.keys()):
            datos_mes = fechas_por_mes[key]
            display_key = datos_mes["display"]
            fechas = datos_mes["fechas"]
            
            # Crear una fila para el mes actual
            row = {"FECHA": display_key}
            
            # Calcular totales para BTG y AES
            total_btg = 0
            total_aes = 0
            
            for fecha in fechas:
                # BTG IT1
                btg_df = resultados["DEMANDA ASIGNADA BTG IT1_COMPRAR"]
                btg_fecha_rows = btg_df[btg_df["FECHA"] == fecha]
                if not btg_fecha_rows.empty:
                    for hora in range(1, 25):
                        total_btg += btg_fecha_rows.iloc[0].get(hora, 0)
                
                # BTG IT2
                btg2_df = resultados["DEMANDA ASIGNADA BTG IT2_COMPRAR"]
                btg2_fecha_rows = btg2_df[btg2_df["FECHA"] == fecha]
                if not btg2_fecha_rows.empty:
                    for hora in range(1, 25):
                        total_btg += btg2_fecha_rows.iloc[0].get(hora, 0)
                
                # AES IT1
                aes_df = resultados["DEMANDA ASIGNADA AES IT1_COMPRAR"]
                aes_fecha_rows = aes_df[aes_df["FECHA"] == fecha]
                if not aes_fecha_rows.empty:
                    for hora in range(1, 25):
                        total_aes += aes_fecha_rows.iloc[0].get(hora, 0)
            
            row[btg_oferta] = total_btg
            row[aes_oferta] = total_aes
            
            resumen_rows.append(row)

        resultados["RESUMEN"] = pd.DataFrame(resumen_rows)
        
        print("Resumen procesado correctamente")
        
        # Imprimir estadísticas
        total_demanda = sum(pyo.value(model.D[a, h]) for a in model.A for h in model.H)
        
        # Calcular totales de asignación
        total_asignado_btg_it1 = sum(row.get(hora, 0) for row in btg_it1_comprar for hora in range(1, 25))
        total_asignado_aes_it1 = sum(row.get(hora, 0) for row in aes_it1_comprar for hora in range(1, 25))
        total_asignado_btg_it2 = sum(row.get(hora, 0) for row in btg_it2_comprar for hora in range(1, 25))
        total_asignado = total_asignado_btg_it1 + total_asignado_aes_it1 + total_asignado_btg_it2
        
        # Calcular déficit total
        total_deficit = sum(row.get(hora, 0) for row in demanda_faltante for hora in range(1, 25))
        
        logger.info(f"Demanda total: {total_demanda:.2f} kWh")
        logger.info(f"Energía asignada total: {total_asignado:.2f} kWh")
        logger.info(f"  - BTG IT1: {total_asignado_btg_it1:.2f} kWh")
        logger.info(f"  - AES IT1: {total_asignado_aes_it1:.2f} kWh")
        logger.info(f"  - BTG IT2: {total_asignado_btg_it2:.2f} kWh")
        logger.info(f"Déficit total: {total_deficit:.2f} kWh")
        
        if total_demanda > 0:
            porcentaje_cubierto = (total_asignado / total_demanda) * 100
            porcentaje_deficit = (total_deficit / total_demanda) * 100
            logger.info(f"Porcentaje cubierto: {porcentaje_cubierto:.2f}%")
            logger.info(f"Porcentaje déficit: {porcentaje_deficit:.2f}%")
        
        print(f"Resultados extraídos: {sum(len(df) for df in resultados.values())} filas en total")
    
    except Exception as e:
        print(f"ERROR GENERAL en extraer_resultados: {e}")
        import traceback
        traceback.print_exc()
    
    return resultados