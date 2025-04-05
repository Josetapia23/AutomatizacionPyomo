"""
Módulo para la construcción y extracción de resultados del modelo de optimización con Pyomo.
"""

import pyomo.environ as pyo
import pandas as pd
import numpy as np
import logging
from pathlib import Path

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
    Extrae los resultados del modelo resuelto y los organiza en un DataFrame.
    
    Args:
        model (ConcreteModel): Modelo de Pyomo resuelto
        
    Returns:
        DataFrame: DataFrame con los resultados de la optimización
    """
    logger.info("Extrayendo resultados del modelo...")
    print("Extrayendo resultados del modelo...")
    
    resultados = []
    
    # Para cada combinación de fecha y hora
    for a in model.A:
        for h in model.H:
            demanda_total = pyo.value(model.D[a, h])
            deficit = pyo.value(model.ENA[a, h])
            
            # Para cada oferta en esta fecha y hora
            oferta_asignada = False
            for i in model.I:
                if (i, a, h) in model.OFH:
                    energia_asignada = pyo.value(model.EA[i, a, h])
                    
                    # Redondear valores muy pequeños a cero
                    if abs(energia_asignada) < 1e-6:
                        energia_asignada = 0
                    
                    # Solo incluir ofertas con asignación positiva
                    if energia_asignada > 0:
                        oferta_asignada = True
                        resultados.append({
                            "CÓDIGO OFERTA": i,
                            "FECHA": a,
                            "HORA": h,
                            "ENERGÍA ASIGNADA": energia_asignada,
                            "CANTIDAD OFERTADA": pyo.value(model.CO[i, a, h]),
                            "PRECIO": pyo.value(model.PO[i, a, h]),
                            "DEMANDA TOTAL": demanda_total,
                            "DÉFICIT": deficit,
                            "OFERTA ACEPTADA": 1
                        })
            
            # Si no se asignó ninguna oferta pero hay demanda, registrar déficit
            if not oferta_asignada and demanda_total > 0:
                resultados.append({
                    "CÓDIGO OFERTA": "SIN ASIGNACIÓN",
                    "FECHA": a,
                    "HORA": h,
                    "ENERGÍA ASIGNADA": 0,
                    "CANTIDAD OFERTADA": 0,
                    "PRECIO": 0,
                    "DEMANDA TOTAL": demanda_total,
                    "DÉFICIT": deficit,
                    "OFERTA ACEPTADA": 0
                })
    
    # Crear DataFrame con los resultados
    df = pd.DataFrame(resultados)
    
    # Agregar columnas calculadas
    if not df.empty:
        # Porcentaje de asignación respecto a la demanda
        df["PORCENTAJE ASIGNACIÓN"] = df.apply(
            lambda row: (row["ENERGÍA ASIGNADA"] / row["DEMANDA TOTAL"] * 100) if row["DEMANDA TOTAL"] > 0 else 0,
            axis=1
        )
        
        # Costo de la energía asignada
        df["COSTO"] = df["ENERGÍA ASIGNADA"] * df["PRECIO"]
    
    # Ordenar por fecha, hora y oferta
    if not df.empty and "FECHA" in df.columns and "HORA" in df.columns:
        df = df.sort_values(["FECHA", "HORA", "CÓDIGO OFERTA"])
    
    # Calcular métricas
    if not df.empty:
        demanda_total = df["DEMANDA TOTAL"].sum()
        energia_asignada_total = df["ENERGÍA ASIGNADA"].sum()
        deficit_total = df["DÉFICIT"].sum()
        
        logger.info(f"Demanda total: {demanda_total:.2f} kWh")
        logger.info(f"Energía asignada total: {energia_asignada_total:.2f} kWh")
        logger.info(f"Déficit total: {deficit_total:.2f} kWh")
        
        if demanda_total > 0:
            porcentaje_cubierto = (energia_asignada_total / demanda_total) * 100
            porcentaje_deficit = (deficit_total / demanda_total) * 100
            logger.info(f"Porcentaje cubierto: {porcentaje_cubierto:.2f}%")
            logger.info(f"Porcentaje déficit: {porcentaje_deficit:.2f}%")
        
        print(f"Resultados extraídos: {len(df)} registros, {df['CÓDIGO OFERTA'].nunique()} ofertas distintas")
    else:
        logger.warning("No se obtuvieron resultados de la optimización")
        print("No se obtuvieron resultados de la optimización")
    
    return df