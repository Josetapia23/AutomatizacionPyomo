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
    Extrae los resultados del modelo optimizado y los organiza en DataFrames.
    Realiza iteraciones hasta agotar la demanda o la capacidad disponible.
    """
    logger.info("Extrayendo resultados del modelo...")
    print("Extrayendo resultados del modelo...")
    
    # Obtener todas las ofertas disponibles que pasaron la evaluación
    ofertas_validas = []
    for i in model.I:
        # Verificar si esta oferta tiene combinaciones válidas
        if any((i, a, h) in model.OFH for a in model.A for h in model.H):
            ofertas_validas.append(i)
    
    ofertas_validas.sort()  # Ordenar alfabéticamente
    print(f"Ofertas válidas para iteraciones: {ofertas_validas}")
    
    if not ofertas_validas:
        print("ADVERTENCIA: No hay ofertas válidas. No se pueden generar resultados.")
        return {}
    
    # Obtener todas las fechas y horas
    todas_fechas = sorted(list(model.A))
    todas_horas = sorted(list(model.H))
    
    print(f"Procesando {len(todas_fechas)} fechas y {len(todas_horas)} horas")
    
    # Diccionarios para almacenar los resultados
    resultados = {}
    
    try:
        # Inicializar la demanda restante con la demanda total
        demanda_restante = {}
        for fecha in todas_fechas:
            for hora in todas_horas:
                demanda_restante[(fecha, hora)] = pyo.value(model.D[fecha, hora]) if (fecha, hora) in model.D else 0
        
        # Para realizar múltiples iteraciones
        iteracion_actual = 1
        demanda_anterior = sum(demanda_restante.values())
        ofertas_procesadas = []
        
        # Seguir iterando mientras haya cambios y demanda por cubrir
        while True:
            print(f"\n=== COMENZANDO ITERACIÓN {iteracion_actual} ===")
            
            # Verificar si aún queda demanda por cubrir
            demanda_total_restante = sum(demanda_restante.values())
            if demanda_total_restante < 1e-6:
                print(f"No queda demanda por asignar. Finalizando en iteración {iteracion_actual}")
                break
            
            # Si la demanda no cambió en la iteración anterior, finalizar
            if iteracion_actual > 1 and abs(demanda_anterior - demanda_total_restante) < 1e-6:
                print(f"No se asignó más demanda en la iteración anterior. Finalizando en iteración {iteracion_actual}")
                break
            
            # Guardar demanda actual para comparar en la siguiente iteración
            demanda_anterior = demanda_total_restante
            
            # Indicar cuánta demanda queda por asignar
            print(f"Demanda restante: {demanda_total_restante:.2f} kWh")
            
            # Para cada oferta, crear su asignación para esta iteración
            asignacion_total_iteracion = 0
            
            for oferta in ofertas_validas:
                # Determinar la capacidad disponible para esta oferta en esta iteración
                capacidad_disponible = {}
                
                # Si es primera iteración, usar la capacidad original
                if iteracion_actual == 1:
                    for (i, a, h) in model.OFH:
                        if i == oferta:
                            capacidad_disponible[(a, h)] = pyo.value(model.CO[i, a, h])
                else:
                    # Obtener capacidad no utilizada de la iteración anterior
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
                
                # Listas para almacenar las filas de los DataFrames
                oferta_comprar = []
                oferta_no_comprada = []
                
                total_asignado = 0
                
                # Para cada fecha y hora, asignar energía
                for fecha in todas_fechas:
                    comprar_row = {"FECHA": fecha, "X": fecha}
                    no_comprada_row = {"FECHA": fecha, "X": fecha}
                    
                    for hora in range(1, 25):
                        try:
                            # Obtener la demanda restante para esta fecha y hora
                            demanda = demanda_restante.get((fecha, hora), 0)
                            
                            # Inicializar valores
                            energia_asignada = 0
                            cantidad_oferta = capacidad_disponible.get((fecha, hora), 0)
                            
                            # Asignar el mínimo entre demanda y capacidad
                            if demanda > 0 and cantidad_oferta > 0:
                                energia_asignada = min(demanda, cantidad_oferta)
                                
                                # Actualizar la demanda restante
                                demanda_restante[(fecha, hora)] -= energia_asignada
                                total_asignado += energia_asignada
                            
                            comprar_row[hora] = energia_asignada
                            no_comprada_row[hora] = max(0, cantidad_oferta - energia_asignada)
                            
                        except Exception as e:
                            print(f"Error al procesar {oferta} IT{iteracion_actual} - fecha: {fecha}, hora: {hora}: {e}")
                            comprar_row[hora] = 0
                            no_comprada_row[hora] = 0
                    
                    oferta_comprar.append(comprar_row)
                    oferta_no_comprada.append(no_comprada_row)
                
                # Guardar resultados para esta oferta en esta iteración
                resultados[f"DEMANDA ASIGNADA {oferta} IT{iteracion_actual}_COMPRAR"] = pd.DataFrame(oferta_comprar)
                resultados[f"DEMANDA ASIGNADA {oferta} IT{iteracion_actual}_NO_COMPRADA"] = pd.DataFrame(oferta_no_comprada)
                
                print(f"Oferta {oferta} IT{iteracion_actual} asignada: {total_asignado:.2f} kWh")
                asignacion_total_iteracion += total_asignado
                
                # Agregar esta oferta a la lista de ofertas procesadas si no está ya
                if oferta not in ofertas_procesadas:
                    ofertas_procesadas.append(oferta)
            
            # Si no se asignó nada en esta iteración, terminar
            if asignacion_total_iteracion < 1e-6:
                print(f"No se asignó energía en iteración {iteracion_actual}. Finalizando.")
                break
                
            # Avanzar a la siguiente iteración
            iteracion_actual += 1
        
        # Calcular demanda faltante
        demanda_faltante = []
        
        for fecha in todas_fechas:
            row = {"FECHA": fecha, "X": fecha}
            
            for hora in range(1, 25):
                # Utilizar directamente el valor de demanda restante calculado
                faltante = demanda_restante.get((fecha, hora), 0)
                
                # Si es un valor muy pequeño, considerarlo como 0
                if abs(faltante) < 1e-6:
                    faltante = 0
                    
                row[hora] = faltante
            
            demanda_faltante.append(row)
        
        resultados["DEMANDA_FALTANTE"] = pd.DataFrame(demanda_faltante)
        
        print("Demanda faltante procesada correctamente")
        
        # Generar resumen mensual con precios
        resumen_rows = []
        
        # Agrupar fechas por mes cronológicamente
        fechas_por_mes = {}
        for fecha in todas_fechas:
            key = (fecha.year, fecha.month)
            display_key = f"{fecha.month:02d}/{fecha.year}"
            
            if key not in fechas_por_mes:
                fechas_por_mes[key] = {
                    "display": display_key,
                    "fechas": []
                }
            
            fechas_por_mes[key]["fechas"].append(fecha)
        
        # Para cada mes, calcular totales y precios promedio para cada oferta
        for key in sorted(fechas_por_mes.keys()):
            datos_mes = fechas_por_mes[key]
            display_key = datos_mes["display"]
            fechas = datos_mes["fechas"]
            
            # Crear fila para este mes
            row = {"FECHA": display_key}
            
            # Calcular totales y precios para cada oferta
            for oferta in ofertas_procesadas:
                total_energia = 0
                total_costo = 0
                
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
                                        # Obtener el precio para esta combinación
                                        if (oferta, fecha, hora) in model.OFH:
                                            precio = pyo.value(model.PO[oferta, fecha, hora])
                                            total_energia += energia_asignada
                                            total_costo += energia_asignada * precio
                
                # Calcular precio promedio ponderado
                precio_promedio = total_costo / total_energia if total_energia > 0 else 0
                
                # Añadir a la fila del resumen
                row[f"{oferta} CANTIDAD"] = total_energia
                row[f"{oferta} PRECIO PROMEDIO"] = precio_promedio
            
            resumen_rows.append(row)
        
        resultados["RESUMEN"] = pd.DataFrame(resumen_rows)
        
        print("Resumen procesado correctamente")
        
        # Calcular estadísticas
        total_demanda = sum(pyo.value(model.D[a, h]) for a in model.A for h in model.H)
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
        
        # Calcular total asignado
        total_asignado = sum(datos["TOTAL"] for datos in total_asignado_por_oferta.values())
        
        # Calcular déficit total
        total_deficit = sum(row.get(hora, 0) for row in demanda_faltante for hora in range(1, 25))
        
        # Registrar estadísticas
        logger.info(f"Demanda total: {total_demanda:.2f} kWh")
        logger.info(f"Energía asignada total: {total_asignado:.2f} kWh")
        
        # Desglose por oferta
        for oferta, datos in total_asignado_por_oferta.items():
            desglose = ", ".join(f"{it}: {valor:.2f}" for it, valor in datos.items() if it != "TOTAL")
            logger.info(f"  - {oferta}: {datos['TOTAL']:.2f} kWh ({desglose})")
        
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