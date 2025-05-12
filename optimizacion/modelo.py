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
    
    # Crear el modelo vacío de Pyomo
    model = pyo.ConcreteModel(name="AsignacionOfertas")
    
    # Preprocesamiento: convertir fechas a formato de fecha si vienen como texto
    if isinstance(demanda_df['FECHA'].iloc[0], str):
        demanda_df['FECHA'] = pd.to_datetime(demanda_df['FECHA']).dt.date
    
    if isinstance(ofertas_df['FECHA'].iloc[0], str):
        ofertas_df['FECHA'] = pd.to_datetime(ofertas_df['FECHA']).dt.date
    
    # Obtener listas únicas y ordenadas de ofertas, fechas y horas
    ofertas = sorted(ofertas_df['CÓDIGO OFERTA'].unique())
    fechas = sorted(demanda_df['FECHA'].unique())
    horas = sorted(demanda_df['HORA'].unique())
    
    # Mostrar las ofertas disponibles para verificación
    print("Ofertas disponibles para optimización:", ofertas)
    
    # Crear diccionario de demanda para acceso rápido por fecha y hora
    demanda_dict = {}
    for _, row in demanda_df.iterrows():
        fecha = row['FECHA']
        hora = row['HORA']
        demanda_dict[(fecha, hora)] = row['DEMANDA']
    
    # Filtrar solo las ofertas que tienen EVALUACIÓN = 1
    ofertas_validas_df = ofertas_df[ofertas_df['EVALUACIÓN'] == 1].copy()
    
    # Crear diccionarios para almacenar precios, cantidades y combinaciones válidas
    precio_dict = {}
    cantidad_dict = {}
    oferta_valida_dict = {}
    
    # Llenar los diccionarios solo con valores válidos (no nulos y cantidades positivas)
    for _, row in ofertas_validas_df.iterrows():
        oferta = row['CÓDIGO OFERTA']
        fecha = row['FECHA']
        hora = row['Atributo']
        
        # Verificar que tiene precio y cantidad válidos
        if pd.notna(row['PRECIO INDEXADO']) and pd.notna(row['CANTIDAD']) and row['CANTIDAD'] > 0:
            precio_dict[(oferta, fecha, hora)] = row['PRECIO INDEXADO']
            cantidad_dict[(oferta, fecha, hora)] = row['CANTIDAD']
            oferta_valida_dict[(oferta, fecha, hora)] = 1
    
    # Definir los conjuntos básicos del modelo
    model.I = pyo.Set(initialize=ofertas, doc='Índice de ofertas')
    model.A = pyo.Set(initialize=fechas, doc='Índice de fechas')
    model.H = pyo.Set(initialize=horas, doc='Índice de horas')
    
    # Función para filtrar solo las combinaciones válidas de oferta-fecha-hora
    def ofertas_fechas_horas_filter(model, i, a, h):
        return (i, a, h) in oferta_valida_dict
    
    # Definir el conjunto de combinaciones válidas oferta-fecha-hora
    model.OFH = pyo.Set(
        initialize=model.I * model.A * model.H,
        dimen=3,
        filter=ofertas_fechas_horas_filter,
        doc='Combinaciones válidas de oferta-fecha-hora'
    )
    
    # Definir parámetro de demanda para cada fecha y hora
    model.D = pyo.Param(
        model.A, model.H,
        initialize=lambda model, a, h: demanda_dict.get((a, h), 0),
        default=0,
        doc='Demanda para cada fecha y hora'
    )
    
    # Definir parámetro de precio para cada combinación válida oferta-fecha-hora
    model.PO = pyo.Param(
        model.OFH,
        initialize=lambda model, i, a, h: precio_dict.get((i, a, h), 0),
        default=0,
        doc='Precio de oferta'
    )
    
    # Definir parámetro de cantidad disponible para cada combinación válida
    model.CO = pyo.Param(
        model.OFH,
        initialize=lambda model, i, a, h: cantidad_dict.get((i, a, h), 0),
        default=0,
        doc='Cantidad de oferta'
    )
    
    # Definir constante grande para penalizaciones
    model.M = pyo.Param(initialize=1e10, doc='Constante para restricciones big-M')
    
    # Asignar prioridades a las ofertas basadas en precio promedio
    prioridad_dict = {}
    print("Asignando prioridades a ofertas basadas en precio promedio:")

    # Primero, calcular precio promedio para cada oferta
    precios_promedio = {}
    for oferta in ofertas:
        # Filtrar registros de esta oferta que cumplan con el filtro de evaluación = 1
        ofertas_validas = ofertas_df[(ofertas_df['CÓDIGO OFERTA'] == oferta) & (ofertas_df['EVALUACIÓN'] == 1)]
        
        if not ofertas_validas.empty:
            # Calcular precio promedio para esta oferta (usando PRECIO INDEXADO)
            precio_promedio = ofertas_validas['PRECIO INDEXADO'].mean()
            precios_promedio[oferta] = precio_promedio
            print(f"  Oferta: {oferta} - Precio promedio: {precio_promedio:.4f}")
        else:
            # Si no hay ofertas válidas, asignar un precio muy alto
            precios_promedio[oferta] = float('inf')
            print(f"  Oferta: {oferta} - Sin ofertas válidas con EVALUACIÓN = 1")

    # Ordenar ofertas por precio promedio (de menor a mayor)
    ofertas_ordenadas_por_precio = [oferta for oferta, _ in sorted(precios_promedio.items(), key=lambda x: x[1])]

    # Asignar prioridad según precio promedio (1 es la más alta - precio más bajo)
    for i, oferta in enumerate(ofertas_ordenadas_por_precio, 1):
        prioridad_dict[oferta] = i
        print(f"  Oferta: {oferta} - Prioridad: {i} - Precio promedio: {precios_promedio[oferta]:.4f}")

    # Mostrar resumen de prioridades
    print("\nPrioridades finales asignadas a ofertas (basadas en precio):")
    for oferta, prioridad in sorted(prioridad_dict.items(), key=lambda x: x[1]):
        print(f"  {oferta}: Prioridad {prioridad} - Precio: {precios_promedio[oferta]:.4f}")
    
    # Añadir prioridades como parámetro del modelo
    model.prioridad = pyo.Param(
        model.I,
        initialize=lambda model, i: prioridad_dict.get(i, 999),
        doc='Prioridad de asignación para cada oferta'
    )
    
    # DEFINIR VARIABLES DE DECISIÓN
    
    # Variable principal: cuánta energía asignar de cada oferta en cada fecha y hora
    model.EA = pyo.Var(
        model.OFH,
        domain=pyo.NonNegativeReals,
        doc='Energía asignada para cada oferta, fecha y hora'
    )
    
    # Variable binaria que indica si se usa una oferta (1) o no (0)
    model.Y = pyo.Var(
        model.OFH,
        domain=pyo.Binary,
        doc='Variable binaria: 1 si oferta es aceptada, 0 si no'
    )
    
    # Variable para la demanda que no puede ser cubierta (déficit)
    model.ENA = pyo.Var(
        model.A, model.H,
        domain=pyo.NonNegativeReals,
        doc='Energía no asignada (déficit) para cada fecha y hora'
    )
    
    # DEFINIR FUNCIÓN OBJETIVO
    
    # Función que determina qué minimizar (costo total)
    def objetivo_rule(model):
        # Componente 1: Costo básico de la energía (precio × cantidad)
        costo_energia = sum(
            model.PO[i, a, h] * model.EA[i, a, h]
            for (i, a, h) in model.OFH
        )
        
        # Componente 2: Penalización muy alta por no cubrir demanda
        penalizacion_deficit = sum(
            model.M * model.ENA[a, h]
            for a in model.A
            for h in model.H
        )
        
        # Componente 3: Pequeño ajuste para preferir ofertas con mayor prioridad
        factor_prioridad = sum(
            (model.prioridad[i] * 0.001) * model.EA[i, a, h]
            for (i, a, h) in model.OFH
        )
        
        # Combinar los tres componentes
        return costo_energia + penalizacion_deficit + factor_prioridad
    
    # Establecer el objetivo como minimizar esta función
    model.Objetivo = pyo.Objective(rule=objetivo_rule, sense=pyo.minimize)
    
    # DEFINIR RESTRICCIONES
    
    # Restricción 1: Equilibrio de demanda
    def balance_demanda_rule(model, a, h):
        # Sumar toda la energía asignada para esta fecha y hora
        energia_asignada = sum(
            model.EA[i, a, h]
            for i in model.I
            if (i, a, h) in model.OFH
        )
        
        # Energía asignada + déficit debe ser igual a la demanda total
        return energia_asignada + model.ENA[a, h] == model.D[a, h]
    
    # Aplicar esta restricción para cada fecha y hora
    model.RestriccionDemanda = pyo.Constraint(
        model.A, model.H,
        rule=balance_demanda_rule,
        doc='Restricción de balance de demanda'
    )
    
    # Restricción 2: No asignar más energía de la disponible
    def limite_asignacion_rule(model, i, a, h):
        if (i, a, h) in model.OFH:
            # La energía asignada no puede superar la cantidad ofertada
            return model.EA[i, a, h] <= model.CO[i, a, h]
        else:
            # Ignorar combinaciones que no son válidas
            return pyo.Constraint.Skip
    
    # Aplicar esta restricción para todas las ofertas, fechas y horas
    model.RestriccionAsignacion = pyo.Constraint(
        model.I, model.A, model.H,
        rule=limite_asignacion_rule,
        doc='Restricción de límite de asignación'
    )
    
    # Restricción 3: Conectar variables binarias con asignación
    def binaria_asignacion_rule(model, i, a, h):
        if (i, a, h) in model.OFH:
            # Si Y = 0, entonces EA = 0 (no se usa esta oferta)
            # Si Y = 1, entonces EA puede ser hasta CO (se usa esta oferta)
            return model.EA[i, a, h] <= model.CO[i, a, h] * model.Y[i, a, h]
        else:
            # Ignorar combinaciones que no son válidas
            return pyo.Constraint.Skip
    
    # Aplicar esta restricción para todas las ofertas, fechas y horas
    model.RestriccionBinariaAsignacion = pyo.Constraint(
        model.I, model.A, model.H,
        rule=binaria_asignacion_rule,
        doc='Restricción de variable binaria para asignación'
    )
    
    # Registrar y mostrar estadísticas del modelo
    logger.info(f"Modelo construido con {len(model.OFH)} combinaciones de ofertas válidas")
    print(f"Modelo construido con {len(model.OFH)} combinaciones de ofertas válidas")
    
    # Retornar el modelo completo listo para resolver
    return model

def extraer_resultados(model, ofertas_df=None, log_detallado=False):
    """
    Extrae los resultados del modelo optimizado y los organiza en DataFrames.
    Realiza iteraciones hasta agotar la demanda o la capacidad disponible.
    Prioriza la asignación por precio específico en cada hora y día.
    
    Args:
        model (ConcreteModel): Modelo Pyomo resuelto
        ofertas_df (DataFrame, opcional): DataFrame con las ofertas originales
        log_detallado (bool, opcional): Si es True, muestra detalles de asignación por hora
        
    Returns:
        dict: Diccionario con los DataFrames de resultados
    """
    logger.info("Extrayendo resultados del modelo...")
    print("Extrayendo resultados del modelo...")
    
    # Identificar todas las ofertas que tienen combinaciones válidas en el modelo
    ofertas_validas = []
    for i in model.I:
        # Una oferta es válida si tiene al menos una combinación en model.OFH
        if any((i, a, h) in model.OFH for a in model.A for h in model.H):
            ofertas_validas.append(i)
    
    print(f"Ofertas válidas identificadas: {ofertas_validas}")
    
    # Verificar que hay ofertas válidas
    if not ofertas_validas:
        print("ADVERTENCIA: No hay ofertas válidas. No se pueden generar resultados.")
        return {}
    
    # Obtener todas las fechas y horas del modelo
    todas_fechas = sorted(list(model.A))
    todas_horas = sorted(list(model.H))
    
    print(f"Procesando {len(todas_fechas)} fechas y {len(todas_horas)} horas")
    
    # Diccionario para almacenar todos los resultados
    resultados = {}
    
    try:
        # Inicializar la demanda restante con la demanda total del modelo
        demanda_restante = {}
        for fecha in todas_fechas:
            for hora in todas_horas:
                demanda_restante[(fecha, hora)] = pyo.value(model.D[fecha, hora]) if (fecha, hora) in model.D else 0
        
        # Inicializar variables para las iteraciones
        iteracion_actual = 1
        demanda_anterior = sum(demanda_restante.values())
        ofertas_procesadas = []
        
        # Inicializar diccionarios para almacenar las asignaciones y capacidades no utilizadas
        # Estructura: {oferta: {iteración: DataFrame}}
        asignaciones_por_oferta = {oferta: {} for oferta in ofertas_validas}
        capacidad_no_usada_por_oferta = {oferta: {} for oferta in ofertas_validas}
        
        # Bucle principal: seguir iterando mientras haya demanda por cubrir y cambios
        while True:
            # Mostrar inicio de iteración actual
            print(f"\n=== COMENZANDO ITERACIÓN {iteracion_actual} ===")
            
            # Verificar si queda demanda por cubrir
            demanda_total_restante = sum(demanda_restante.values())
            if demanda_total_restante < 1e-6:
                # Si ya no hay demanda, terminar
                print(f"No queda demanda por asignar. Finalizando en iteración {iteracion_actual}")
                break
            
            # Verificar si hubo cambios en la iteración anterior
            if iteracion_actual > 1 and abs(demanda_anterior - demanda_total_restante) < 1e-6:
                # Si la demanda no cambió, terminar (no se puede asignar más)
                print(f"No se asignó más demanda en la iteración anterior. Finalizando en iteración {iteracion_actual}")
                break
            
            # Guardar demanda actual para comparar en la siguiente iteración
            demanda_anterior = demanda_total_restante
            
            # Mostrar cuánta demanda queda por asignar
            print(f"Demanda restante: {demanda_total_restante:.2f} kWh")
            
            # Inicializar asignaciones para esta iteración
            for oferta in ofertas_validas:
                # Crear DataFrames vacíos para esta oferta e iteración
                df_comprar = pd.DataFrame({"FECHA": todas_fechas})
                df_no_comprada = pd.DataFrame({"FECHA": todas_fechas})
                
                # Añadir columnas para cada hora (inicializadas a 0)
                for hora in range(1, 25):
                    df_comprar[hora] = 0.0
                    df_no_comprada[hora] = 0.0
                
                # Guardar en los diccionarios
                asignaciones_por_oferta[oferta][iteracion_actual] = df_comprar
                capacidad_no_usada_por_oferta[oferta][iteracion_actual] = df_no_comprada
            
            # Contador para el total asignado en esta iteración
            asignacion_total_iteracion = 0
            
            # NUEVA LÓGICA: Procesar por fecha y hora, asignando primero las ofertas más económicas
            for fecha in todas_fechas:
                # Para cada hora del día
                for hora in todas_horas:
                    # Verificar si queda demanda para esta hora y fecha
                    demanda = demanda_restante.get((fecha, hora), 0)
                    if demanda <= 1e-6:
                        continue  # Si no hay demanda, pasar a la siguiente hora
                    
                    # Recolectar todas las ofertas disponibles para esta hora y fecha
                    ofertas_disponibles = []
                    
                    for oferta in ofertas_validas:
                        capacidad = 0
                        precio = float('inf')
                        
                        # Determinar disponibilidad según la iteración
                        if iteracion_actual == 1:
                            # Primera iteración, usar valores originales del modelo
                            if (oferta, fecha, hora) in model.OFH:
                                capacidad = pyo.value(model.CO[oferta, fecha, hora])
                                precio = pyo.value(model.PO[oferta, fecha, hora])
                        else:
                            # Iteraciones siguientes, usar capacidad no utilizada anterior
                            df_anterior = capacidad_no_usada_por_oferta[oferta][iteracion_actual-1]
                            fila = df_anterior[df_anterior["FECHA"] == fecha]
                            if not fila.empty:
                                capacidad = fila.iloc[0].get(hora, 0)
                                
                                # Buscar el precio en el modelo
                                if (oferta, fecha, hora) in model.OFH:
                                    precio = pyo.value(model.PO[oferta, fecha, hora])
                        
                        # Si hay capacidad disponible, añadir a la lista
                        if capacidad > 0:
                            ofertas_disponibles.append((oferta, precio, capacidad))
                    
                    # Ordenar ofertas por precio (de menor a mayor)
                    ofertas_disponibles.sort(key=lambda x: x[1])
                    
                    # Mostrar ofertas disponibles para esta hora y fecha (para depuración)
                    if ofertas_disponibles and log_detallado:
                        print(f"  Fecha: {fecha}, Hora: {hora}, Demanda: {demanda:.2f}")
                        print(f"  Ofertas disponibles (ordenadas por precio):")
                        for oferta, precio, capacidad in ofertas_disponibles:
                            print(f"    - {oferta}: Precio={precio:.2f} $/KWh, Capacidad={capacidad:.2f} KWh")
                    
                    # Asignar energía a las ofertas, en orden de precio
                    for oferta, precio, capacidad in ofertas_disponibles:
                        # Asignar solo lo que se necesita
                        energia_asignada = min(demanda, capacidad)
                        
                        if energia_asignada > 0:
                            # Actualizar demanda restante
                            demanda_restante[(fecha, hora)] -= energia_asignada
                            demanda -= energia_asignada
                            
                            # Actualizar asignación para esta oferta
                            df_asignacion = asignaciones_por_oferta[oferta][iteracion_actual]
                            fila_idx = df_asignacion[df_asignacion["FECHA"] == fecha].index[0]
                            
                            # Corrección: Convertir a float explícitamente para evitar advertencias
                            df_asignacion.at[fila_idx, hora] = float(energia_asignada)
                            
                            # Actualizar capacidad no utilizada
                            df_no_usada = capacidad_no_usada_por_oferta[oferta][iteracion_actual]
                            fila_idx = df_no_usada[df_no_usada["FECHA"] == fecha].index[0]
                            
                            # Corrección: Convertir a float explícitamente
                            df_no_usada.at[fila_idx, hora] = float(capacidad - energia_asignada)
                            
                            # Acumular total asignado
                            asignacion_total_iteracion += energia_asignada
                            
                            # Registrar oferta como procesada si es la primera vez
                            if oferta not in ofertas_procesadas:
                                ofertas_procesadas.append(oferta)
                            
                            # Mostrar detalle de asignación si log_detallado es True
                            if log_detallado:
                                print(f"    → Asignado {energia_asignada:.2f} KWh a {oferta} a precio {precio:.2f} $/KWh")
                        
                        # Si ya no queda demanda, salir del bucle de ofertas
                        if demanda <= 1e-6:
                            break
            
            # Si no se asignó nada en esta iteración, terminar
            if asignacion_total_iteracion < 1e-6:
                print(f"No se asignó energía en iteración {iteracion_actual}. Finalizando.")
                break
            
            # Guardar los resultados en el diccionario de resultados final
            for oferta in ofertas_validas:
                # Verificar si hubo asignaciones para esta oferta
                df_asignacion = asignaciones_por_oferta[oferta][iteracion_actual]
                total_asignado = df_asignacion.iloc[:, 1:].sum().sum()  # Sumar todas las columnas excepto FECHA
                
                if total_asignado > 0:
                    print(f"Oferta {oferta} IT{iteracion_actual} asignada: {total_asignado:.2f} kWh")
                    
                    # Añadir columna X (copia de FECHA para el formato de salida)
                    df_asignacion["X"] = df_asignacion["FECHA"]
                    
                    # Guardar en el diccionario final
                    resultados[f"DEMANDA ASIGNADA {oferta} IT{iteracion_actual}_COMPRAR"] = df_asignacion.copy()
                
                # Guardar la capacidad no utilizada
                df_no_usada = capacidad_no_usada_por_oferta[oferta][iteracion_actual]
                df_no_usada["X"] = df_no_usada["FECHA"]
                resultados[f"DEMANDA ASIGNADA {oferta} IT{iteracion_actual}_NO_COMPRADA"] = df_no_usada.copy()
            
            # Avanzar a la siguiente iteración
            iteracion_actual += 1
        
        # CALCULAR DEMANDA FALTANTE
        demanda_faltante = []
        
        # Para cada fecha y hora, registrar cuánta demanda quedó sin cubrir
        for fecha in todas_fechas:
            row = {"FECHA": fecha, "X": fecha}
            
            for hora in range(1, 25):
                # Obtener la demanda restante
                faltante = demanda_restante.get((fecha, hora), 0)
                
                # Redondear valores muy pequeños a cero
                if abs(faltante) < 1e-6:
                    faltante = 0
                    
                row[hora] = faltante
            
            demanda_faltante.append(row)
        
        # Guardar DataFrame de demanda faltante
        resultados["DEMANDA_FALTANTE"] = pd.DataFrame(demanda_faltante)
        
        print("Demanda faltante procesada correctamente")
        
        # GENERAR RESUMEN EJECUTIVO CON TODA LA INFORMACIÓN REQUERIDA
        resumen_ejecutivo_rows = []
        
        # Agrupar fechas por mes cronológicamente
        fechas_por_mes = {}
        for fecha in todas_fechas:
            # Usar tupla (año, mes) como clave para ordenar cronológicamente
            key = (fecha.year, fecha.month)
            display_key = f"{fecha.month:02d}/{fecha.year}"
            
            if key not in fechas_por_mes:
                fechas_por_mes[key] = {
                    "display": display_key,
                    "fechas": []
                }
            
            fechas_por_mes[key]["fechas"].append(fecha)
        
        # Verificar si tenemos ofertas_df disponible para precios sin indexar
        has_ofertas_df = ofertas_df is not None
        if not has_ofertas_df:
            logger.warning("No se proporcionó DataFrame de ofertas. Solo se usarán precios indexados en el resumen.")
        
        # Preparar un diccionario para almacenar la demanda no asignada por mes
        demanda_no_asignada_por_mes = {}
        demanda_faltante_df = pd.DataFrame(demanda_faltante)
        
        for _, row in demanda_faltante_df.iterrows():
            fecha = row["FECHA"]
            key = (fecha.year, fecha.month)
            
            if key not in demanda_no_asignada_por_mes:
                demanda_no_asignada_por_mes[key] = 0
                
            # Sumar la demanda faltante para todas las horas de este día
            for hora in range(1, 25):
                if hora in row and not pd.isna(row[hora]):
                    demanda_no_asignada_por_mes[key] += row[hora]
        
        # Para cada mes, calcular totales y precios promedio para cada oferta
        for key in sorted(fechas_por_mes.keys()):
            datos_mes = fechas_por_mes[key]
            display_key = datos_mes["display"]
            fechas = datos_mes["fechas"]
            
            # Crear fila para el resumen ejecutivo
            row_resumen = {"FECHA": display_key}
            
            # Calcular totales y precios para cada oferta
            for oferta in ofertas_procesadas:
                total_energia = 0
                total_costo_indexado = 0
                total_costo_sin_indexar = 0
                
                # Sumar todas las iteraciones
                for it in range(1, iteracion_actual):
                    key_it = f"DEMANDA ASIGNADA {oferta} IT{it}_COMPRAR"
                    if key_it in resultados:
                        df_it = resultados[key_it]
                        for fecha in fechas:
                            fecha_rows = df_it[df_it["FECHA"] == fecha]
                            if not fecha_rows.empty:
                                for hora in range(1, 25):
                                    energia_asignada = fecha_rows.iloc[0].get(hora, 0)
                                    if energia_asignada > 0:
                                        # Obtener el precio para esta combinación (PRECIO INDEXADO)
                                        if (oferta, fecha, hora) in model.OFH:
                                            precio_indexado = pyo.value(model.PO[oferta, fecha, hora])
                                            
                                            # Inicializar precio sin indexar con el precio indexado por defecto
                                            precio_sin_indexar = precio_indexado
                                            
                                            # Buscar precio sin indexar en ofertas_df si está disponible
                                            if has_ofertas_df:
                                                try:
                                                    # Filtrar el DataFrame por las tres condiciones
                                                    ofertas_filtradas = ofertas_df[
                                                        (ofertas_df['CÓDIGO OFERTA'] == oferta) & 
                                                        (ofertas_df['FECHA'] == fecha) & 
                                                        (ofertas_df['Atributo'] == hora)
                                                    ]
                                                    
                                                    if not ofertas_filtradas.empty:
                                                        # Verificar si existe la columna PRECIO
                                                        if 'PRECIO' in ofertas_filtradas.columns:
                                                            # Obtener el precio original sin indexar
                                                            precio_original = ofertas_filtradas.iloc[0]['PRECIO']
                                                            if not pd.isna(precio_original):
                                                                precio_sin_indexar = precio_original
                                                except Exception as e:
                                                    logger.warning(f"Error al buscar precio sin indexar: {e}")
                                            
                                            # Acumular para cálculos de promedio ponderado
                                            total_energia += energia_asignada
                                            total_costo_indexado += energia_asignada * precio_indexado
                                            total_costo_sin_indexar += energia_asignada * precio_sin_indexar
                
                # Calcular precios promedio ponderados
                precio_promedio_indexado = total_costo_indexado / total_energia if total_energia > 0 else 0
                precio_promedio_sin_indexar = total_costo_sin_indexar / total_energia if total_energia > 0 else 0
                
                # Añadir a la fila de resumen ejecutivo con las nuevas etiquetas
                row_resumen[f"{oferta} CANTIDAD (KWh)"] = total_energia
                row_resumen[f"{oferta} PRECIO ($/KWh)"] = precio_promedio_sin_indexar
                row_resumen[f"{oferta} PRECIO INDEXADO ($/KWh)"] = precio_promedio_indexado
            
            # Agregar la demanda no asignada para este mes
            row_resumen["DEMANDA NO ASIGNADA (KWh)"] = demanda_no_asignada_por_mes.get(key, 0)
            
            resumen_ejecutivo_rows.append(row_resumen)
        
        # Guardar DataFrame de resumen ejecutivo
        resultados["RESUMEN EJECUTIVO"] = pd.DataFrame(resumen_ejecutivo_rows)
        
        print("Resumen ejecutivo procesado correctamente")
        
        # CALCULAR ESTADÍSTICAS FINALES
        
        # Calcular demanda total del modelo
        total_demanda = sum(pyo.value(model.D[a, h]) for a in model.A for h in model.H)
        
        # Diccionario para almacenar asignaciones por oferta e iteración
        total_asignado_por_oferta = {}
        
        # Calcular total asignado por oferta e iteración
        for oferta in ofertas_procesadas:
            totales_por_it = {}
            
            # Sumar todas las iteraciones
            for it in range(1, iteracion_actual):
                key_it = f"DEMANDA ASIGNADA {oferta} IT{it}_COMPRAR"
                if key_it in resultados:
                    total_it = 0
                    df_it = resultados[key_it]
                    for fecha in todas_fechas:
                        fecha_rows = df_it[df_it["FECHA"] == fecha]
                        if not fecha_rows.empty:
                            for hora in range(1, 25):
                                total_it += fecha_rows.iloc[0].get(hora, 0)
                    
                    totales_por_it[f"IT{it}"] = total_it
            
            # Calcular total general para esta oferta
            total_general = sum(totales_por_it.values())
            totales_por_it["TOTAL"] = total_general
            
            total_asignado_por_oferta[oferta] = totales_por_it
        
        # Calcular total asignado sumando todas las ofertas
        total_asignado = sum(datos["TOTAL"] for datos in total_asignado_por_oferta.values())
        
        # Calcular déficit total
        total_deficit = sum(row.get(hora, 0) for row in demanda_faltante for hora in range(1, 25))
        
        # Registrar estadísticas en el log
        logger.info(f"Demanda total: {total_demanda:.2f} kWh")
        logger.info(f"Energía asignada total: {total_asignado:.2f} kWh")
        
        # Desglose por oferta
        for oferta, datos in total_asignado_por_oferta.items():
            desglose = ", ".join(f"{it}: {valor:.2f}" for it, valor in datos.items() if it != "TOTAL")
            logger.info(f"  - {oferta}: {datos['TOTAL']:.2f} kWh ({desglose})")
        
        logger.info(f"Déficit total: {total_deficit:.2f} kWh")
        
        # Calcular porcentajes
        if total_demanda > 0:
            porcentaje_cubierto = (total_asignado / total_demanda) * 100
            porcentaje_deficit = (total_deficit / total_demanda) * 100
            logger.info(f"Porcentaje cubierto: {porcentaje_cubierto:.2f}%")
            logger.info(f"Porcentaje déficit: {porcentaje_deficit:.2f}%")
        
        print(f"Resultados extraídos: {sum(len(df) for df in resultados.values())} filas en total")
        
    except Exception as e:
        # Capturar y mostrar errores generales
        print(f"ERROR GENERAL en extraer_resultados: {e}")
        import traceback
        traceback.print_exc()
    
    # Retornar todos los resultados generados
    return resultados