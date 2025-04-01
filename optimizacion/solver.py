"""
Configuración y ejecución del solver para el modelo de optimización.
"""

import logging
from pyomo.environ import SolverFactory
from pathlib import Path

from ..config import get_solver_path

logger = logging.getLogger(__name__)

def resolver_modelo(model, tee=True):
    """
    Resuelve el modelo de optimización utilizando el solver CBC.
    
    Args:
        model: Modelo de Pyomo a resolver
        tee (bool): Si se deben mostrar los logs del solver
        
    Returns:
        SolverResults: Resultados del solver
    """
    logger.info("Resolviendo modelo de optimización...")
    
    try:
        # Obtener la ruta al solver CBC
        solver_path = get_solver_path()
        logger.info(f"Usando solver CBC en {solver_path}")
        
        # Crear el objeto solver
        solver = SolverFactory("cbc", executable=solver_path)
        
        # Resolver el modelo
        result = solver.solve(model, tee=tee)
        
        # Verificar el resultado
        if result.solver.termination_condition == 'optimal':
            logger.info("Modelo resuelto de manera óptima")
        else:
            logger.warning(f"El solver terminó con condición: {result.solver.termination_condition}")
            logger.warning("Es posible que no se haya encontrado una solución óptima")
        
        return result
    
    except Exception as e:
        logger.error(f"Error al resolver el modelo: {e}")
        raise