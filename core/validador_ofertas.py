"""
Validador de nombres de ofertas con formato estandarizado.
Formato esperado: Agente-OFERTA-# (ejemplo: EPM-OFERTA-001.xlsx)

CREAR ESTE ARCHIVO COMO: core/validador_ofertas.py
"""

import re
import os
import logging
from dataclasses import dataclass
from typing import Dict, List

logger = logging.getLogger(__name__)

@dataclass
class OfertaInfo:
    """Información estructurada de una oferta."""
    agente: str
    numero: str
    nombre_archivo: str
    nombre_estandarizado: str
    es_valido: bool
    error_mensaje: str = ""

class ValidadorNombresOfertas:
    """
    Validador de nombres de ofertas con formato Agente-OFERTA-#
    - Agente: Cualquier nombre (EPM, AES, EMPRESA123, etc.)
    - OFERTA: Palabra fija 
    - #: Número de la oferta
    """
    
    # Patrón regex flexible para cualquier agente
    PATRON_ESTANDAR = re.compile(r'^([A-Za-z0-9_\-\.]+)-OFERTA-(\d+)$', re.IGNORECASE)
    
    def __init__(self):
        self.ofertas_procesadas: Dict[str, OfertaInfo] = {}
        self.agentes_encontrados = set()
        self.errores_encontrados = []
        
    def validar_nombre_archivo(self, nombre_archivo: str) -> OfertaInfo:
        """
        Valida si un nombre de archivo cumple con el formato estandarizado.
        
        Args:
            nombre_archivo (str): Nombre del archivo (con o sin extensión)
            
        Returns:
            OfertaInfo: Información estructurada de la oferta
        """
        # Remover extensión si existe
        nombre_base = os.path.splitext(nombre_archivo)[0]
        
        # Aplicar patrón regex
        match = self.PATRON_ESTANDAR.match(nombre_base)
        
        if match:
            agente = match.group(1).upper()  # Normalizar a mayúsculas
            numero = match.group(2)
            
            # Crear nombre estandarizado
            nombre_estandarizado = f"{agente}-OFERTA-{numero.zfill(3)}"  # Pad con ceros
            
            # Registrar agente encontrado
            self.agentes_encontrados.add(agente)
            
            oferta_info = OfertaInfo(
                agente=agente,
                numero=numero.zfill(3),
                nombre_archivo=nombre_archivo,
                nombre_estandarizado=nombre_estandarizado,
                es_valido=True
            )
            
            logger.info(f"✅ Oferta válida: {nombre_archivo} → {nombre_estandarizado}")
            
        else:
            # Nombre no cumple el formato
            oferta_info = OfertaInfo(
                agente="",
                numero="",
                nombre_archivo=nombre_archivo,
                nombre_estandarizado="",
                es_valido=False,
                error_mensaje=f"No cumple formato Agente-OFERTA-# (ej: EPM-OFERTA-001)"
            )
            
            logger.warning(f"❌ Oferta inválida: {nombre_archivo}")
            self.errores_encontrados.append(oferta_info)
        
        self.ofertas_procesadas[nombre_archivo] = oferta_info
        return oferta_info
    
    def obtener_resumen_validacion(self) -> Dict:
        """Retorna un resumen de la validación realizada."""
        total = len(self.ofertas_procesadas)
        validas = sum(1 for info in self.ofertas_procesadas.values() if info.es_valido)
        invalidas = total - validas
        
        return {
            'total_archivos': total,
            'ofertas_validas': validas,
            'ofertas_invalidas': invalidas,
            'agentes_encontrados': sorted(list(self.agentes_encontrados)),
            'errores': [info.error_mensaje for info in self.errores_encontrados]
        }

def validar_carpeta_ofertas(carpeta_ofertas):
    """
    Valida todos los archivos de una carpeta y retorna un reporte.
    
    Args:
        carpeta_ofertas (str): Ruta a la carpeta de ofertas
        
    Returns:
        dict: Reporte de validación completo
    """
    validador = ValidadorNombresOfertas()
    
    if not os.path.exists(carpeta_ofertas):
        return {
            'error': f"La carpeta {carpeta_ofertas} no existe",
            'validas': [],
            'invalidas': [],
            'agentes': []
        }
    
    archivos = [f for f in os.listdir(carpeta_ofertas) if f.endswith('.xlsx')]
    
    for archivo in archivos:
        validador.validar_nombre_archivo(archivo)
    
    resumen = validador.obtener_resumen_validacion()
    
    return {
        'total': resumen['total_archivos'],
        'validas': resumen['ofertas_validas'],
        'invalidas': resumen['ofertas_invalidas'],
        'agentes': resumen['agentes_encontrados'],
        'errores': resumen['errores'],
        'archivos_validos': [info.nombre_archivo for info in validador.ofertas_procesadas.values() if info.es_valido],
        'archivos_invalidos': [info.nombre_archivo for info in validador.ofertas_procesadas.values() if not info.es_valido]
    }