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
    Construye el modelo de optimización utilizando Pyomo, 
    con prioridades específicas para replicar la lógica del Excel.
    
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
    
    # Asignar prioridades a las ofertas según su nombre
    prioridad_dict = {}
    for oferta in ofertas:
        if "BTG" in oferta:
            if "IT1" in oferta or "-" in oferta:  # Asumir que la forma base es IT1
                prioridad_dict[oferta] = 1  # Primera prioridad: BTG IT1
            elif "IT2" in oferta:
                prioridad_dict[oferta] = 3  # Tercera prioridad: BTG IT2
            else:
                prioridad_dict[oferta] = 1  # Por defecto BTG es prioridad 1
        elif "AES" in oferta:
            prioridad_dict[oferta] = 2  # Segunda prioridad: AES
        else:
            # Verificar por patrón específico en el nombre de la oferta
            if oferta.startswith("OP1_Wide-BTG"):
                prioridad_dict[oferta] = 1
            elif oferta.startswith("OP1_Wide-AES"):
                prioridad_dict[oferta] = 2
            else:
                prioridad_dict[oferta] = 10  # Baja prioridad para otras ofertas
    
    # Imprimir las prioridades asignadas
    print("Prioridades asignadas a ofertas:")
    for oferta, prioridad in prioridad_dict.items():
        print(f"  {oferta}: {prioridad}")
    
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
    
    # Restricciones para implementar la lógica de asignación secuencial como en Excel
    
    # Identificar ofertas por tipo
    ofertas_btg_it1 = [o for o in ofertas if model.prioridad[o] == 1]
    ofertas_aes_it1 = [o for o in ofertas if model.prioridad[o] == 2]
    ofertas_btg_it2 = [o for o in ofertas if model.prioridad[o] == 3]
    
    # No intentar agregar restricciones si no tenemos ofertas de un tipo
    logger.info(f"Modelo construido con {len(model.OFH)} combinaciones de ofertas válidas")
    print(f"Modelo construido con {len(model.OFH)} combinaciones de ofertas válidas")
    
    return model

def extraer_resultados(model):
    """
    Extrae los resultados del modelo resuelto y los organiza en múltiples DataFrames
    según el formato requerido en el Excel.
    
    Args:
        model (ConcreteModel): Modelo de Pyomo resuelto
        
    Returns:
        dict: Diccionario con los diferentes DataFrames de resultados
    """
    logger.info("Extrayendo resultados del modelo...")
    print("Extrayendo resultados del modelo...")
    
    # Nombres específicos para las hojas
    hojas_necesarias = [
        "DEMANDA ASIGNADA BTG IT1",
        "DEMANDA ASIGNADA AES IT1",
        "DEMANDA ASIGNADA BTG IT2"
    ]
    
    # Determinar qué ofertas corresponden a cada hoja
    ofertas = sorted(list(model.I))
    mapeo_ofertas = {}
    
    # Intentar asignar ofertas basándose en su nombre
    for oferta in ofertas:
        if "BTG" in oferta or "OP1_Wide-BTG" in oferta:
            mapeo_ofertas["DEMANDA ASIGNADA BTG IT1"] = oferta
        elif "AES" in oferta or "OP1_Wide-AES" in oferta:
            mapeo_ofertas["DEMANDA ASIGNADA AES IT1"] = oferta
    
    # Asignación por defecto si no se encontraron ofertas específicas
    if not mapeo_ofertas:
        if len(ofertas) >= 1:
            mapeo_ofertas["DEMANDA ASIGNADA BTG IT1"] = ofertas[0]
        if len(ofertas) >= 2:
            mapeo_ofertas["DEMANDA ASIGNADA AES IT1"] = ofertas[1]
        if len(ofertas) >= 3:
            mapeo_ofertas["DEMANDA ASIGNADA BTG IT2"] = ofertas[2]
    
    print("Mapeo de ofertas a hojas:")
    for hoja, oferta in mapeo_ofertas.items():
        print(f"  {hoja}: {oferta}")
    
    # Crear diccionarios para almacenar las asignaciones por oferta
    asignaciones_por_oferta = {}
    for oferta in model.I:
        asignaciones_por_oferta[oferta] = {}
    
    # Extraer asignaciones para cada combinación de fecha y hora
    for a in model.A:
        for h in model.H:
            demanda_total = pyo.value(model.D[a, h])
            deficit = pyo.value(model.ENA[a, h])
            
            # Para cada oferta en esta fecha y hora
            for i in model.I:
                if (i, a, h) in model.OFH:
                    energia_asignada = pyo.value(model.EA[i, a, h])
                    
                    # Redondear valores muy pequeños a cero
                    if abs(energia_asignada) < 1e-6:
                        energia_asignada = 0
                    
                    # Almacenar la asignación (incluso si es cero)
                    if a not in asignaciones_por_oferta[i]:
                        asignaciones_por_oferta[i][a] = {}
                    
                    asignaciones_por_oferta[i][a][h] = {
                        "ENERGÍA ASIGNADA": energia_asignada,
                        "CANTIDAD OFERTADA": pyo.value(model.CO[i, a, h]),
                        "PRECIO": pyo.value(model.PO[i, a, h]),
                        "DEMANDA TOTAL": demanda_total
                    }
    
    # Asegurar que todas las fechas y horas estén en todas las ofertas
    todas_fechas = sorted(list(model.A))
    todas_horas = sorted(list(model.H))
    
    for oferta in model.I:
        for fecha in todas_fechas:
            if fecha not in asignaciones_por_oferta[oferta]:
                asignaciones_por_oferta[oferta][fecha] = {}
            
            for hora in todas_horas:
                if hora not in asignaciones_por_oferta[oferta][fecha]:
                    # Buscar la demanda total para esta fecha/hora
                    demanda_total = pyo.value(model.D[fecha, hora])
                    
                    asignaciones_por_oferta[oferta][fecha][hora] = {
                        "ENERGÍA ASIGNADA": 0,
                        "CANTIDAD OFERTADA": 0,
                        "PRECIO": 0,
                        "DEMANDA TOTAL": demanda_total
                    }
    
    # Crear DataFrames para cada formato requerido
    resultados = {}
    
    # 1. Crear DataFrames para "ENERGÍA A COMPRAR AL VENDEDOR"
    for nombre_hoja, oferta in mapeo_ofertas.items():
        energia_comprar_rows = []
        energia_no_comprada_rows = []
        
        # Preparar estructura para ambas secciones
        for fecha in todas_fechas:
            comprar_row = {"FECHA": fecha, "X": fecha}
            no_comprada_row = {"FECHA": fecha, "X": fecha}
            
            for hora in range(1, 25):  # Asegurarse de incluir todas las horas de 1 a 24
                # ENERGÍA A COMPRAR AL VENDEDOR
                if hora in asignaciones_por_oferta[oferta][fecha]:
                    comprar_row[hora] = asignaciones_por_oferta[oferta][fecha][hora]["ENERGÍA ASIGNADA"]
                else:
                    comprar_row[hora] = 0
                
                # ENERGÍA NO COMPRADA AL VENDEDOR
                cantidad_ofertada = 0
                if (oferta, fecha, hora) in model.OFH:
                    cantidad_ofertada = pyo.value(model.CO[oferta, fecha, hora])
                
                energia_asignada = comprar_row[hora]  # Ya calculada arriba
                energia_no_comprada = max(0, cantidad_ofertada - energia_asignada)
                no_comprada_row[hora] = energia_no_comprada
            
            energia_comprar_rows.append(comprar_row)
            energia_no_comprada_rows.append(no_comprada_row)
        
        # Crear DataFrames
        resultados[f"{nombre_hoja}_COMPRAR"] = pd.DataFrame(energia_comprar_rows)
        resultados[f"{nombre_hoja}_NO_COMPRADA"] = pd.DataFrame(energia_no_comprada_rows)
    
    # 2. Crear DataFrame para "DEMANDA FALTANTE"
    demanda_faltante_rows = []
    
    # Calcular la demanda faltante para cada fecha y hora
    for fecha in todas_fechas:
        row = {"FECHA": fecha, "X": fecha}
        
        for hora in todas_horas:
            # Obtener la demanda total
            demanda_total = pyo.value(model.D[fecha, hora])
            
            # Calcular la suma de energía asignada
            energia_asignada_total = 0
            for nombre_hoja, oferta in mapeo_ofertas.items():
                if fecha in asignaciones_por_oferta[oferta] and hora in asignaciones_por_oferta[oferta][fecha]:
                    energia_asignada_total += asignaciones_por_oferta[oferta][fecha][hora]["ENERGÍA ASIGNADA"]
            
            # Calcular el déficit
            deficit = max(0, demanda_total - energia_asignada_total)
            
            # Si es un valor muy pequeño, considerarlo como 0
            if deficit < 1e-6:
                deficit = 0
                
            row[hora] = deficit
        
        demanda_faltante_rows.append(row)
    
    resultados["DEMANDA_FALTANTE"] = pd.DataFrame(demanda_faltante_rows)
    
    # 3. Crear DataFrame para RESUMEN (mensuales)
    resumen_rows = []
    
    # Agrupar fechas por mes
    fechas_por_mes = {}
    for fecha in todas_fechas:
        mes = fecha.month
        año = fecha.year
        key = f"{año}-{mes:02d}"
        
        if key not in fechas_por_mes:
            fechas_por_mes[key] = []
        
        fechas_por_mes[key].append(fecha)
    
    # Para cada mes, calcular totales
    for key, fechas in fechas_por_mes.items():
        año, mes = key.split('-')
        fecha_mostrar = datetime(int(año), int(mes), 1).date()
        
        row = {"FECHA": fecha_mostrar}
        
        # Para cada oferta, calcular total asignado en el mes
        for nombre_hoja, oferta in mapeo_ofertas.items():
            total_asignado = 0
            
            for fecha in fechas:
                for hora in todas_horas:
                    if fecha in asignaciones_por_oferta[oferta] and hora in asignaciones_por_oferta[oferta][fecha]:
                        total_asignado += asignaciones_por_oferta[oferta][fecha][hora]["ENERGÍA ASIGNADA"]
            
            row[nombre_hoja] = total_asignado
        
        resumen_rows.append(row)
    
    resultados["RESUMEN"] = pd.DataFrame(resumen_rows)
    
    # Imprimir estadísticas de los resultados
    total_demanda = sum(pyo.value(model.D[a, h]) for a in model.A for h in model.H)
    total_asignado = 0
    for oferta in model.I:
        for fecha in asignaciones_por_oferta[oferta]:
            for hora in asignaciones_por_oferta[oferta][fecha]:
                total_asignado += asignaciones_por_oferta[oferta][fecha][hora]["ENERGÍA ASIGNADA"]
    
    total_deficit = sum(pyo.value(model.ENA[a, h]) for a in model.A for h in model.H)
    
    logger.info(f"Demanda total: {total_demanda:.2f} kWh")
    logger.info(f"Energía asignada total: {total_asignado:.2f} kWh")
    logger.info(f"Déficit total: {total_deficit:.2f} kWh")
    
    if total_demanda > 0:
        porcentaje_cubierto = (total_asignado / total_demanda) * 100
        porcentaje_deficit = (total_deficit / total_demanda) * 100
        logger.info(f"Porcentaje cubierto: {porcentaje_cubierto:.2f}%")
        logger.info(f"Porcentaje déficit: {porcentaje_deficit:.2f}%")
    
    print(f"Resultados extraídos: {sum(len(df) for df in resultados.values())} filas en total")
    
    return resultados
