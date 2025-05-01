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
    
    # Asignar prioridades a las ofertas basadas en orden alfabético
    prioridad_dict = {}
    print("Asignando prioridades a ofertas de manera dinámica:")

    # Ordenar ofertas alfabéticamente
    ofertas_ordenadas = sorted(ofertas)

    # Asignar prioridad según posición alfabética (1 es la más alta)
    for i, oferta in enumerate(ofertas_ordenadas, 1):
        prioridad_dict[oferta] = i
        print(f"  Oferta: {oferta} - Prioridad: {i}")

    # Mostrar resumen de prioridades
    print("\nPrioridades finales asignadas a ofertas:")
    for oferta, prioridad in sorted(prioridad_dict.items(), key=lambda x: x[1]):
        print(f"  {oferta}: Prioridad {prioridad}")
    
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

def extraer_resultados(model, ofertas_df=None):
    """
    Extrae los resultados del modelo optimizado y los organiza en DataFrames.
    Realiza iteraciones hasta agotar la demanda o la capacidad disponible.
    
    Args:
        model (ConcreteModel): Modelo Pyomo resuelto
        ofertas_df (DataFrame, opcional): DataFrame con las ofertas originales
        
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
    
    # Ordenar alfabéticamente para consistencia
    ofertas_validas.sort()
    print(f"Ofertas válidas para iteraciones: {ofertas_validas}")
    
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
            
            # Contador para el total asignado en esta iteración
            asignacion_total_iteracion = 0
            
            # Procesar cada oferta en esta iteración
            for oferta in ofertas_validas:
                # Determinar la capacidad disponible para esta oferta en esta iteración
                capacidad_disponible = {}
                
                # Si es primera iteración, usar la capacidad original del modelo
                if iteracion_actual == 1:
                    for (i, a, h) in model.OFH:
                        if i == oferta:
                            capacidad_disponible[(a, h)] = pyo.value(model.CO[i, a, h])
                else:
                    # Para iteraciones siguientes, usar capacidad no utilizada de la iteración anterior
                    key_anterior = f"DEMANDA ASIGNADA {oferta} IT{iteracion_actual-1}_NO_COMPRADA"
                    if key_anterior in resultados:
                        df_anterior = resultados[key_anterior]
                        for fecha in todas_fechas:
                            fecha_rows = df_anterior[df_anterior["FECHA"] == fecha]
                            if not fecha_rows.empty:
                                for hora in range(1, 25):
                                    capacidad_no_usada = fecha_rows.iloc[0].get(hora, 0)
                                    if capacidad_no_usada > 0:
                                        capacidad_disponible[(fecha, hora)] = capacidad_no_usada
                
                # Si no hay capacidad disponible, pasar a la siguiente oferta
                if not capacidad_disponible:
                    continue
                
                print(f"Procesando oferta {oferta} (IT{iteracion_actual})")
                
                # Listas para almacenar las filas de los DataFrames de resultados
                oferta_comprar = []
                oferta_no_comprada = []
                
                # Contador para el total asignado a esta oferta
                total_asignado = 0
                
                # Procesar cada fecha y hora para esta oferta
                for fecha in todas_fechas:
                    # Inicializar filas para los DataFrames
                    comprar_row = {"FECHA": fecha, "X": fecha}
                    no_comprada_row = {"FECHA": fecha, "X": fecha}
                    
                    # Procesar cada hora
                    for hora in range(1, 25):
                        try:
                            # Obtener la demanda restante para esta fecha y hora
                            demanda = demanda_restante.get((fecha, hora), 0)
                            
                            # Inicializar valores
                            energia_asignada = 0
                            cantidad_oferta = capacidad_disponible.get((fecha, hora), 0)
                            
                            # Asignar energía: el mínimo entre demanda y capacidad
                            if demanda > 0 and cantidad_oferta > 0:
                                energia_asignada = min(demanda, cantidad_oferta)
                                
                                # Actualizar la demanda restante
                                demanda_restante[(fecha, hora)] -= energia_asignada
                                total_asignado += energia_asignada
                            
                            # Guardar valores en las filas
                            comprar_row[hora] = energia_asignada
                            no_comprada_row[hora] = max(0, cantidad_oferta - energia_asignada)
                            
                        except Exception as e:
                            # Manejar errores en el procesamiento
                            print(f"Error al procesar {oferta} IT{iteracion_actual} - fecha: {fecha}, hora: {hora}: {e}")
                            comprar_row[hora] = 0
                            no_comprada_row[hora] = 0
                    
                    # Añadir filas a las listas
                    oferta_comprar.append(comprar_row)
                    oferta_no_comprada.append(no_comprada_row)
                
                # Guardar DataFrames para esta oferta e iteración
                resultados[f"DEMANDA ASIGNADA {oferta} IT{iteracion_actual}_COMPRAR"] = pd.DataFrame(oferta_comprar)
                resultados[f"DEMANDA ASIGNADA {oferta} IT{iteracion_actual}_NO_COMPRADA"] = pd.DataFrame(oferta_no_comprada)
                
                # Mostrar cuánto se asignó a esta oferta
                print(f"Oferta {oferta} IT{iteracion_actual} asignada: {total_asignado:.2f} kWh")
                asignacion_total_iteracion += total_asignado
                
                # Registrar oferta como procesada si es la primera vez
                if oferta not in ofertas_procesadas:
                    ofertas_procesadas.append(oferta)
            
            # Si no se asignó nada en esta iteración, terminar
            if asignacion_total_iteracion < 1e-6:
                print(f"No se asignó energía en iteración {iteracion_actual}. Finalizando.")
                break
                
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
        
        # GENERAR RESUMEN MENSUAL CON PRECIOS INDEXADOS Y SIN INDEXAR
        resumen_indexado_rows = []
        resumen_sin_indexar_rows = []
        
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
        
        # Para cada mes, calcular totales y precios promedio para cada oferta
        for key in sorted(fechas_por_mes.keys()):
            datos_mes = fechas_por_mes[key]
            display_key = datos_mes["display"]
            fechas = datos_mes["fechas"]
            
            # Crear filas para ambos tipos de resumen
            row_indexado = {"FECHA": display_key}
            row_sin_indexar = {"FECHA": display_key}
            
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
                
                # Añadir a las filas de resumen
                row_indexado[f"{oferta} CANTIDAD"] = total_energia
                row_indexado[f"{oferta} PRECIO PROMEDIO"] = precio_promedio_indexado
                
                row_sin_indexar[f"{oferta} CANTIDAD"] = total_energia
                row_sin_indexar[f"{oferta} PRECIO PROMEDIO"] = precio_promedio_sin_indexar
            
            resumen_indexado_rows.append(row_indexado)
            resumen_sin_indexar_rows.append(row_sin_indexar)
        
        # Guardar DataFrames de resumen
        resultados["RESUMEN"] = pd.DataFrame(resumen_indexado_rows)
        resultados["RESUMEN SIN INDEXAR"] = pd.DataFrame(resumen_sin_indexar_rows)
        
        print("Resumen procesado correctamente")
        
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