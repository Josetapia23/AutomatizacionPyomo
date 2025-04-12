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
    model.M = pyo.Param(initialize=100000000, doc='Constante para restricciones big-M')
    
    # Margen SICEP (MS) - Factor k para evaluación (Solicitar al usuario)
    model.MS = pyo.Param(initialize=1.5, doc='Margen SICEP (factor k)')
    
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
    
    # Función objetivo: Minimizar el costo total de energía asignada
    def objetivo_rule(model):
        # Costo de la energía asignada
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
        
        return costo_energia + penalizacion_deficit
    
    model.Objetivo = pyo.Objective(rule=objetivo_rule, sense=pyo.minimize)
    
    # Restricción de balance de demanda (Ecuación 2)
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
    
    # Restricción de límite de asignación (Ecuación 3)
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
    
    # Restricción de variable binaria para asignación (Ecuación 6, primera parte)
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
    según el formato requerido.
    
    Args:
        model (ConcreteModel): Modelo de Pyomo resuelto
        
    Returns:
        dict: Diccionario con los diferentes DataFrames de resultados
    """
    logger.info("Extrayendo resultados del modelo...")
    print("Extrayendo resultados del modelo...")
    
    # Obtener ofertas únicas
    ofertas = sorted(list(model.I))
    
    # Crear diccionarios para almacenar las asignaciones por oferta
    asignaciones_por_oferta = {}
    for oferta in ofertas:
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
                    
                    # Almacenar la asignación
                    if energia_asignada > 0:
                        if a not in asignaciones_por_oferta[i]:
                            asignaciones_por_oferta[i][a] = {}
                        
                        asignaciones_por_oferta[i][a][h] = {
                            "ENERGÍA ASIGNADA": energia_asignada,
                            "CANTIDAD OFERTADA": pyo.value(model.CO[i, a, h]),
                            "PRECIO": pyo.value(model.PO[i, a, h]),
                            "DEMANDA TOTAL": demanda_total
                        }
    
    # Crear DataFrames para cada formato requerido
    resultados = {}
    
    # 1. Crear DataFrame para "DEMANDA ASIGNADA [OFERTA] IT1"
    for idx, oferta in enumerate(ofertas):
        rows = []
        # Preparar estructura de datos para la hoja "DEMANDA ASIGNADA [OFERTA] IT1"
        for a in model.A:
            row = {"FECHA": a, "X": a}
            
            for h in model.H:
                if a in asignaciones_por_oferta[oferta] and h in asignaciones_por_oferta[oferta][a]:
                    row[h] = asignaciones_por_oferta[oferta][a][h]["ENERGÍA ASIGNADA"]
                else:
                    row[h] = 0
            
            rows.append(row)
        
        # Crear DataFrame
        nombre_hoja = f"DEMANDA_ASIGNADA_{oferta}_IT{idx+1}"
        resultados[nombre_hoja] = pd.DataFrame(rows)
    
    # 2. Crear DataFrame para "ENERGÍA NO COMPRADA AL VENDEDOR"
    energia_no_comprada_rows = []
    for a in model.A:
        row = {"FECHA": a, "X": a}
        
        for h in model.H:
            # Para cada oferta, calcular la energía no asignada
            energia_no_asignada = 0
            
            for i in model.I:
                if (i, a, h) in model.OFH:
                    cantidad_ofertada = pyo.value(model.CO[i, a, h])
                    energia_asignada = 0
                    
                    if a in asignaciones_por_oferta[i] and h in asignaciones_por_oferta[i][a]:
                        energia_asignada = asignaciones_por_oferta[i][a][h]["ENERGÍA ASIGNADA"]
                    
                    energia_no_asignada += (cantidad_ofertada - energia_asignada)
            
            row[h] = energia_no_asignada
        
        energia_no_comprada_rows.append(row)
    
    resultados["ENERGIA_NO_COMPRADA"] = pd.DataFrame(energia_no_comprada_rows)
    
    # 3. Crear DataFrame para "DEMANDA FALTANTE"
    demanda_faltante_rows = []
    for a in model.A:
        row = {"FECHA": a, "X": a}
        
        for h in model.H:
            deficit = pyo.value(model.ENA[a, h])
            row[h] = deficit
        
        demanda_faltante_rows.append(row)
    
    resultados["DEMANDA_FALTANTE"] = pd.DataFrame(demanda_faltante_rows)
    
    # 4. Crear DataFrame para RESUMEN (mensuales)
    resumen_rows = []
    
    # Agrupar fechas por mes
    fechas_por_mes = {}
    for a in model.A:
        mes = a.month  # Asumiendo que a es un objeto date o datetime
        año = a.year
        key = f"{año}-{mes:02d}"
        
        if key not in fechas_por_mes:
            fechas_por_mes[key] = []
        
        fechas_por_mes[key].append(a)
    
    # Para cada mes, calcular totales
    for key, fechas in fechas_por_mes.items():
        año, mes = key.split('-')
        fecha_mostrar = datetime(int(año), int(mes), 1).date()
        
        row = {"FECHA": fecha_mostrar}
        
        # Para cada oferta, calcular total asignado en el mes
        for idx, oferta in enumerate(ofertas):
            total_asignado = 0
            
            for a in fechas:
                for h in model.H:
                    if a in asignaciones_por_oferta[oferta] and h in asignaciones_por_oferta[oferta][a]:
                        total_asignado += asignaciones_por_oferta[oferta][a][h]["ENERGÍA ASIGNADA"]
            
            row[f"{oferta}_IT{idx+1}"] = total_asignado
        
        resumen_rows.append(row)
    
    resultados["RESUMEN"] = pd.DataFrame(resumen_rows)
    
    # Imprimir estadísticas de los resultados
    total_demanda = sum(pyo.value(model.D[a, h]) for a in model.A for h in model.H)
    total_asignado = sum(
        asignaciones_por_oferta[i][a][h]["ENERGÍA ASIGNADA"] 
        for i in ofertas 
        for a in asignaciones_por_oferta[i] 
        for h in asignaciones_por_oferta[i][a]
    )
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