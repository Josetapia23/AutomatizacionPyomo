"""
Módulo para la resolución de modelos de optimización con el solver CBC.
"""

import pyomo.environ as pyo
import logging
import os
from pathlib import Path

from config import get_solver_path

logger = logging.getLogger(__name__)

def resolver_modelo(model, tiempo_limite=600):
    """
    Resuelve el modelo de optimización utilizando el solver CBC.
    
    Args:
        model (ConcreteModel): Modelo de Pyomo a resolver
        tiempo_limite (int): Tiempo límite en segundos para la resolución
        
    Returns:
        SolverResults: Resultado de la resolución del modelo
    """
    logger.info(f"Resolviendo modelo de optimización (tiempo límite: {tiempo_limite} segundos)...")
    print(f"Resolviendo modelo de optimización (tiempo límite: {tiempo_limite} segundos)...")
    
    # Intentar obtener la ruta del solver
    try:
        solver_path = get_solver_path()
        logger.info(f"Ruta del solver CBC: {solver_path}")
    except Exception as e:
        logger.warning(f"No se pudo determinar la ruta del solver: {e}. Intentando usar el solver disponible en el sistema.")
        solver_path = None
    
    # Configurar el solver
    solver = pyo.SolverFactory('cbc', executable=solver_path)
    
    # Establecer opciones del solver
    solver.options['seconds'] = tiempo_limite
    solver.options['ratioGap'] = 0.01  # Gap relativo (1%)
    if 'threads' in solver.options:
        solver.options['threads'] = max(1, os.cpu_count() - 1)  # Usar todos los hilos disponibles menos uno
    
    # Resolver el modelo
    try:
        results = solver.solve(model, tee=True)
        
        # Verificar el estado de la solución
        if results.solver.termination_condition == pyo.TerminationCondition.optimal:
            logger.info("Se encontró una solución óptima")
            print("Se encontró una solución óptima")
        elif results.solver.termination_condition == pyo.TerminationCondition.feasible:
            logger.info("Se encontró una solución factible (no necesariamente óptima)")
            print("Se encontró una solución factible (no necesariamente óptima)")
        else:
            logger.warning(f"No se encontró una solución óptima. Estado: {results.solver.termination_condition}")
            print(f"No se encontró una solución óptima. Estado: {results.solver.termination_condition}")
        
        # Calcular valor de la función objetivo
        objective_value = pyo.value(model.Objetivo)
        logger.info(f"Valor de la función objetivo: {objective_value}")
        print(f"Valor de la función objetivo: {objective_value}")
        
        return results
    
    except Exception as e:
        logger.exception(f"Error al resolver el modelo: {e}")
        print(f"Error al resolver el modelo: {e}")
        raise