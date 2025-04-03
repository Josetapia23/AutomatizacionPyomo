"""
Definición del modelo de optimización en Pyomo para asignar ofertas a la demanda.
"""

import pandas as pd
import numpy as np
import logging
from pyomo.environ import (
    ConcreteModel, Var, NonNegativeReals, Objective,
    SolverFactory, Param, Set, minimize, value, Constraint
)

from config import DATOS_INICIALES, RESULTADO_OFERTAS

logger = logging.getLogger(__name__)

def construir_modelo(demanda_df, ofertas_df):
    """
    Construye el modelo de optimización en Pyomo para asignar ofertas a la demanda.
    
    Args:
        demanda_df (DataFrame): DataFrame con la demanda a cubrir
        ofertas_df (DataFrame): DataFrame con las ofertas disponibles
        
    Returns:
        ConcreteModel: Modelo de Pyomo construido
    """
    logger.info("Construyendo modelo de optimización en Pyomo...")
    
    # Preparar conjuntos (sets)
    fechas_horas = set((r['FECHA'], r['HORA']) for _, r in demanda_df.iterrows())
    ofertas_list = set((r['CÓDIGO OFERTA'], r['FECHA'], r['Atributo']) for _, r in ofertas_df.iterrows())
    
    # Crear diccionarios para acceso rápido
    demanda_dict = {}
    for _, row in demanda_df.iterrows():
        f, h = row['FECHA'], row['HORA']
        demanda_dict[(f, h)] = row['DEMANDA']
    
    cap_dict = {}
    price_dict = {}
    for _, row in ofertas_df.iterrows():
        key = (row['CÓDIGO OFERTA'], row['FECHA'], row['Atributo'])
        cap = row['CANTIDAD']
        p = row['PRECIO INDEXADO']
        cap_dict[key] = cap
        price_dict[key] = p
    
    # Crear modelo
    model = ConcreteModel()
    
    # Definir conjuntos
    model.OFERTAS = Set(initialize=list(ofertas_list), dimen=3)
    model.FECHAS_HORAS = Set(initialize=list(fechas_horas), dimen=2)
    
    # Definir parámetros
    model.capacity = Param(
        model.OFERTAS,
        initialize=lambda m, o, f, h: cap_dict.get((o, f, h), 0.0),
        within=NonNegativeReals
    )
    model.price = Param(
        model.OFERTAS,
        initialize=lambda m, o, f, h: price_dict.get((o, f, h), 99999),
        within=NonNegativeReals
    )
    model.demand = Param(
        model.FECHAS_HORAS,
        initialize=lambda m, f, h: demanda_dict.get((f, h), 0.0),
        within=NonNegativeReals
    )
    
    # Definir variables de decisión
    model.x = Var(model.OFERTAS, domain=NonNegativeReals)  # Cantidad asignada a cada oferta
    model.deficit = Var(model.FECHAS_HORAS, domain=NonNegativeReals)  # Déficit de demanda
    
    # Restricción de capacidad: x[i,t] <= capacity[i,t]
    def cap_rule(m, oferta, f, h):
        return m.x[oferta, f, h] <= m.capacity[oferta, f, h]
    model.cap_constraint = Constraint(model.OFERTAS, rule=cap_rule)
    
    # Restricción de demanda con déficit: sum(x[i,t]) + deficit[t] == demand[t]
    def dem_rule_with_deficit(m, f, h):
        relevant_offers = [(o1, o2, o3) for (o1, o2, o3) in m.OFERTAS if (o2 == f and o3 == h)]
        expr = sum(m.x[o1, o2, o3] for (o1, o2, o3) in relevant_offers) + m.deficit[f, h]
        return expr == m.demand[f, h]
    model.dem_constraint = Constraint(model.FECHAS_HORAS, rule=dem_rule_with_deficit)
    
    # Función objetivo: Minimizar el costo total (incluyendo penalización por déficit)
    def obj_rule_with_deficit(m):
        return sum(m.price[o] * m.x[o] for o in m.OFERTAS) + sum(999999 * m.deficit[f, h] for (f, h) in m.FECHAS_HORAS)
    model.obj = Objective(rule=obj_rule_with_deficit, sense=minimize)
    
    logger.info("Modelo de optimización construido correctamente")
    return model

def extraer_resultados(model):
    """
    Extrae los resultados del modelo de optimización.
    
    Args:
        model (ConcreteModel): Modelo de Pyomo resuelto
        
    Returns:
        DataFrame: DataFrame con los resultados de la asignación
    """
    logger.info("Extrayendo resultados del modelo de optimización...")
    
    rows = []
    for (f, h) in model.FECHAS_HORAS:
        demanda_total = value(model.demand[f, h])
        deficit = value(model.deficit[f, h])
        
        asignaciones = []
        for (o, of, oh) in model.OFERTAS:
            if of == f and oh == h:
                val = value(model.x[o, of, oh])
                if val > 0:
                    p = value(model.price[o, of, oh])
                    asignaciones.append({
                        "CÓDIGO OFERTA": o,
                        "FECHA": f,
                        "HORA": h,
                        "ASIGNADO": val,
                        "PRECIO INDEXADO": p,
                        "DEMANDA TOTAL": demanda_total,
                        "DÉFICIT": deficit,
                        "PORCENTAJE CUBIERTO": ((val / demanda_total) * 100) if demanda_total > 0 else 0
                    })
        
        # Si no hay asignaciones para esta fecha/hora pero hay demanda
        if not asignaciones and demanda_total > 0:
            rows.append({
                "CÓDIGO OFERTA": "SIN ASIGNACIÓN",
                "FECHA": f, 
                "HORA": h,
                "ASIGNADO": 0,
                "PRECIO INDEXADO": None,
                "DEMANDA TOTAL": demanda_total,
                "DÉFICIT": deficit,
                "PORCENTAJE CUBIERTO": 0
            })
        else:
            rows.extend(asignaciones)
    
    df_resultados = pd.DataFrame(rows)
    logger.info(f"Se extrajeron {len(df_resultados)} resultados del modelo")
    
    return df_resultados