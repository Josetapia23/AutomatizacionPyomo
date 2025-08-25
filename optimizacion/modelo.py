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
    VERSIÓN CORREGIDA: Elimina duplicación en hojas ENA.
    
    Args:
        model (ConcreteModel): Modelo Pyomo resuelto
        ofertas_df (DataFrame, opcional): DataFrame con las ofertas originales
        log_detallado (bool, opcional): Si es True, muestra detalles de asignación por hora
        
    Returns:
        dict: Diccionario con los DataFrames de resultados
    """
    import pyomo.environ as pyo
    import pandas as pd
    import logging
    
    logger = logging.getLogger(__name__)
    
    logger.info("Extrayendo resultados del modelo...")
    print("Extrayendo resultados del modelo...")
    
    # Identificar todas las ofertas que tienen combinaciones válidas en el modelo
    ofertas_validas = []
    for i in model.I:
        # Una oferta es válida si tiene al menos una combinación en model.OFH
        if any((i, a, h) in model.OFH for a in model.A for h in model.H):
            ofertas_validas.append(i)
    
    # NUEVO: También obtener ofertas que no pasaron la evaluación desde ofertas_df
    ofertas_rechazadas = []
    todas_las_ofertas_df = []
    
    if ofertas_df is not None:
        # Obtener TODAS las ofertas únicas del DataFrame original
        todas_las_ofertas_df = ofertas_df['CÓDIGO OFERTA'].unique().tolist()
        
        print(f"DEBUG - TODAS las ofertas en DataFrame original: {todas_las_ofertas_df}")
        print(f"DEBUG - Ofertas válidas del modelo: {ofertas_validas}")
        
        # Identificar ofertas rechazadas (las que NO están en ofertas_validas)
        ofertas_rechazadas = [oferta for oferta in todas_las_ofertas_df if oferta not in ofertas_validas]
    
    # TODAS las ofertas (válidas + rechazadas)
    todas_las_ofertas = ofertas_validas + ofertas_rechazadas
    
    print(f"Ofertas válidas para optimización: {ofertas_validas}")
    print(f"Ofertas rechazadas por evaluación: {ofertas_rechazadas}")
    print(f"Total de ofertas a incluir en resumen: {todas_las_ofertas}")
    
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
        # ========================================
        # PROCESAR OFERTAS VÁLIDAS DEL OPTIMIZADOR
        # ========================================
        
        print(f"\nExtrayendo asignaciones directas del optimizador...")
        
        # Para cada oferta del modelo, extraer resultados directamente
        ofertas_procesadas = []
        
        for oferta in ofertas_validas:
            print(f"\nProcesando oferta: {oferta}")
            
            # Extraer asignaciones del optimizador
            da_rows = []
            ena_rows = []
            total_asignado = 0
            
            for fecha in todas_fechas:
                fila_da = {"FECHA": fecha, "X": fecha}
                fila_ena = {"FECHA": fecha, "X": fecha}
                
                # Inicializar todas las horas en 0
                for hora in range(1, 25):
                    fila_da[hora] = 0
                    fila_ena[hora] = 0
                
                # Llenar con valores del modelo
                for hora in todas_horas:
                    if (oferta, fecha, hora) in model.OFH:
                        # Obtener asignación del optimizador
                        asignacion = pyo.value(model.EA[oferta, fecha, hora]) if (oferta, fecha, hora) in model.EA else 0
                        capacidad = pyo.value(model.CO[oferta, fecha, hora]) if (oferta, fecha, hora) in model.CO else 0
                        
                        # DA = Lo que asignó el optimizador
                        fila_da[hora] = asignacion
                        total_asignado += asignacion
                        
                        # ENA = Capacidad disponible - asignado
                        fila_ena[hora] = max(0, capacidad - asignacion)
                
                da_rows.append(fila_da)
                ena_rows.append(fila_ena)
            
            # Determinar si la oferta fue procesada (tiene asignaciones > 0)
            if total_asignado > 0:
                ofertas_procesadas.append(oferta)
                print(f"  {oferta} - Asignado: {total_asignado:.2f} kWh")
            else:
                print(f"  {oferta} - Sin asignaciones (precio alto o sin demanda)")
            
            # Generar hojas DA y ENA para TODAS las ofertas (con o sin asignaciones)
            if da_rows:
                da_df = pd.DataFrame(da_rows)
                resultados[f"DEMANDA ASIGNADA {oferta} IT1_COMPRAR"] = da_df
                print(f"  Hoja DA generada para {oferta}")
            
            if ena_rows:
                ena_df = pd.DataFrame(ena_rows)
                resultados[f"DEMANDA ASIGNADA {oferta} IT1_NO_COMPRADA"] = ena_df
                print(f"  Hoja ENA generada para {oferta}")
        
        # ========================================
        # GENERAR HOJAS PARA OFERTAS RECHAZADAS - CORREGIDO
        # ========================================
        
        print(f"\nGenerando hojas para {len(ofertas_rechazadas)} ofertas rechazadas...")
        
        for oferta in ofertas_rechazadas:
            if ofertas_df is not None:
                oferta_data = ofertas_df[ofertas_df['CÓDIGO OFERTA'] == oferta]
                if not oferta_data.empty:
                    print(f"  Generando hojas para {oferta} (rechazada por evaluación)")
                    
                    # CORRECCIÓN CRÍTICA: Deduplicar datos ANTES de procesar
                    oferta_data_unique = oferta_data.drop_duplicates(
                        subset=['FECHA', 'Atributo'], 
                        keep='first'
                    )
                    
                    print(f"    Datos originales: {len(oferta_data)}, Únicos: {len(oferta_data_unique)}")
                    
                    # DA: Todo en 0 (no se compró nada)
                    # ENA: Capacidad completa (todo disponible) - SIN DUPLICADOS
                    da_rows = []
                    ena_rows = []
                    fechas_unicas = sorted(oferta_data_unique['FECHA'].unique())
                    
                    for fecha in fechas_unicas:
                        fila_da = {"FECHA": fecha, "X": fecha}
                        fila_ena = {"FECHA": fecha, "X": fecha}
                        
                        # Inicializar todas las horas
                        for hora in range(1, 25):
                            fila_da[hora] = 0  # DA: Todo en 0
                            fila_ena[hora] = 0  # ENA: Se llenará con datos únicos
                        
                        # CORRECCIÓN: Llenar ENA con datos únicos solamente
                        data_fecha = oferta_data_unique[oferta_data_unique['FECHA'] == fecha]
                        
                        # Crear diccionario para evitar duplicados por hora
                        datos_por_hora = {}
                        for _, row in data_fecha.iterrows():
                            hora = int(row['Atributo'])
                            cantidad = row['CANTIDAD']
                            if 1 <= hora <= 24:
                                # Solo tomar el primer valor para cada hora
                                if hora not in datos_por_hora:
                                    datos_por_hora[hora] = cantidad
                        
                        # Aplicar los datos únicos
                        for hora, cantidad in datos_por_hora.items():
                            fila_ena[hora] = cantidad
                        
                        da_rows.append(fila_da)
                        ena_rows.append(fila_ena)
                    
                    if da_rows and ena_rows:
                        da_df = pd.DataFrame(da_rows)
                        ena_df = pd.DataFrame(ena_rows)
                        
                        resultados[f"DEMANDA ASIGNADA {oferta} IT1_COMPRAR"] = da_df
                        resultados[f"DEMANDA ASIGNADA {oferta} IT1_NO_COMPRADA"] = ena_df
                        
                        print(f"  Hojas DA y ENA generadas para {oferta} (sin duplicados)")
                    else:
                        print(f"  No se generaron datos para {oferta}")
        
        # ========================================
        # CALCULAR DEMANDA FALTANTE
        # ========================================
        
        print(f"\nCalculando demanda faltante...")
        demanda_faltante = []
        
        for fecha in todas_fechas:
            fila = {"FECHA": fecha, "X": fecha}
            
            for hora in range(1, 25):
                # Demanda total
                demanda_total = pyo.value(model.D[fecha, hora]) if (fecha, hora) in model.D else 0
                
                # Energía asignada total
                energia_asignada = 0
                for oferta in ofertas_validas:
                    if (oferta, fecha, hora) in model.EA:
                        energia_asignada += pyo.value(model.EA[oferta, fecha, hora])
                
                # Déficit
                deficit = max(0, demanda_total - energia_asignada)
                fila[hora] = deficit
            
            demanda_faltante.append(fila)
        
        if demanda_faltante:
            df_faltante = pd.DataFrame(demanda_faltante)
            resultados["DEMANDA_FALTANTE"] = df_faltante
            print(f"Demanda faltante calculada")
        
        # ========================================
        # GENERAR RESUMEN EJECUTIVO
        # ========================================
        
        print(f"\nGenerando resumen ejecutivo...")
        resumen_ejecutivo_rows = []
        
        # Agrupar fechas por mes
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
        
        # Calcular demanda no asignada por mes
        demanda_no_asignada_por_mes = {}
        demanda_faltante_df = pd.DataFrame(demanda_faltante)
        
        for _, row in demanda_faltante_df.iterrows():
            fecha = row["FECHA"]
            key = (fecha.year, fecha.month)
            
            if key not in demanda_no_asignada_por_mes:
                demanda_no_asignada_por_mes[key] = 0
            
            for hora in range(1, 25):
                if hora in row and not pd.isna(row[hora]):
                    demanda_no_asignada_por_mes[key] += row[hora]
        
        # Generar resumen por mes
        for key in sorted(fechas_por_mes.keys()):
            datos_mes = fechas_por_mes[key]
            display_key = datos_mes["display"]
            fechas = datos_mes["fechas"]
            
            row_resumen = {"FECHA": display_key}
            
            # Procesar TODAS las ofertas
            for oferta in todas_las_ofertas:
                total_energia = 0
                total_costo_indexado = 0
                total_costo_sin_indexar = 0
                
                # Buscar en resultados generados
                key_da = f"DEMANDA ASIGNADA {oferta} IT1_COMPRAR"
                if key_da in resultados:
                    df_da = resultados[key_da]
                    
                    for fecha in fechas:
                        fecha_rows = df_da[df_da["FECHA"] == fecha]
                        if not fecha_rows.empty:
                            for hora in range(1, 25):
                                energia_asignada = fecha_rows.iloc[0].get(hora, 0)
                                if energia_asignada > 0:
                                    # Buscar precio en el modelo o en ofertas_df
                                    precio_indexado = 0
                                    precio_sin_indexar = 0
                                    
                                    if oferta in ofertas_validas and (oferta, fecha, hora) in model.OFH:
                                        precio_indexado = pyo.value(model.PO[oferta, fecha, hora])
                                        precio_sin_indexar = precio_indexado
                                        
                                        # Buscar precio original en ofertas_df
                                        if ofertas_df is not None:
                                            try:
                                                ofertas_filtradas = ofertas_df[
                                                    (ofertas_df['CÓDIGO OFERTA'] == oferta) & 
                                                    (ofertas_df['FECHA'] == fecha) & 
                                                    (ofertas_df['Atributo'] == hora)
                                                ]
                                                
                                                if not ofertas_filtradas.empty and 'PRECIO' in ofertas_filtradas.columns:
                                                    precio_original = ofertas_filtradas.iloc[0]['PRECIO']
                                                    if not pd.isna(precio_original):
                                                        precio_sin_indexar = precio_original
                                            except Exception as e:
                                                logger.warning(f"Error al buscar precio sin indexar: {e}")
                                    
                                    total_energia += energia_asignada
                                    total_costo_indexado += energia_asignada * precio_indexado
                                    total_costo_sin_indexar += energia_asignada * precio_sin_indexar
                
                # Calcular precios promedio
                precio_promedio_indexado = total_costo_indexado / total_energia if total_energia > 0 else 0
                precio_promedio_sin_indexar = total_costo_sin_indexar / total_energia if total_energia > 0 else 0
                
                # Añadir al resumen
                row_resumen[f"{oferta} CANTIDAD (KWh)"] = total_energia
                row_resumen[f"{oferta} PRECIO ($/KWh)"] = precio_promedio_sin_indexar
                row_resumen[f"{oferta} PRECIO INDEXADO ($/KWh)"] = precio_promedio_indexado
            
            # Agregar demanda no asignada
            row_resumen["DEMANDA NO ASIGNADA (KWh)"] = demanda_no_asignada_por_mes.get(key, 0)
            
            resumen_ejecutivo_rows.append(row_resumen)
        
        if resumen_ejecutivo_rows:
            resumen_df = pd.DataFrame(resumen_ejecutivo_rows)
            resultados["RESUMEN EJECUTIVO"] = resumen_df
            print(f"Resumen ejecutivo generado")
        
        # ========================================
        # ESTADÍSTICAS FINALES
        # ========================================
        
        total_demanda = sum(pyo.value(model.D[a, h]) for a in model.A for h in model.H)
        total_asignado = sum(pyo.value(model.EA[oferta, fecha, hora]) 
                           for oferta in ofertas_validas 
                           for fecha in model.A 
                           for hora in model.H 
                           if (oferta, fecha, hora) in model.EA)
        total_deficit = sum(row.get(hora, 0) for row in demanda_faltante for hora in range(1, 25))
        
        logger.info(f"Demanda total: {total_demanda:.2f} kWh")
        logger.info(f"Energía asignada total: {total_asignado:.2f} kWh")
        logger.info(f"Déficit total: {total_deficit:.2f} kWh")
        
        if total_demanda > 0:
            porcentaje_cubierto = (total_asignado / total_demanda) * 100
            logger.info(f"Porcentaje cubierto: {porcentaje_cubierto:.2f}%")
        
        print(f"\nRESUMEN FINAL:")
        print(f"   Total ofertas procesadas: {len(todas_las_ofertas)}")
        print(f"   Hojas DA/ENA generadas: {len([k for k in resultados.keys() if 'DEMANDA ASIGNADA' in k])}")
        
    except Exception as e:
        print(f"ERROR GENERAL en extraer_resultados: {e}")
        import traceback
        traceback.print_exc()
        return {}
    
    return resultados