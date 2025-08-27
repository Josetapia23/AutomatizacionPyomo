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
    VERSIÓN CORREGIDA: Garantiza conservación total de energía procesando TODAS las fechas.
    
    Args:
        model (ConcreteModel): Modelo Pyomo resuelto
        ofertas_df (DataFrame, opcional): DataFrame con las ofertas originales COMPLETAS
        log_detallado (bool, opcional): Si es True, muestra detalles de asignación por hora
        
    Returns:
        dict: Diccionario con los DataFrames de resultados
    """
    import pyomo.environ as pyo
    import pandas as pd
    import logging
    
    logger = logging.getLogger(__name__)
    
    logger.info("Extrayendo resultados del modelo con conservación TOTAL de energía...")
    print("Extrayendo resultados del modelo con conservación TOTAL de energía...")
    
    # Verificar que tenemos datos originales completos
    if ofertas_df is None or ofertas_df.empty:
        print("ERROR: Se requieren datos completos de ofertas para conservar energía")
        return {}
    
    # ========================================
    # PASO 1: CLASIFICAR OFERTAS CORRECTAMENTE
    # ========================================
    
    # Obtener TODAS las ofertas del archivo original
    todas_las_ofertas_originales = sorted(ofertas_df['CÓDIGO OFERTA'].unique())
    
    # Identificar ofertas que SÍ están en el modelo Pyomo (tienen combinaciones en model.OFH)
    ofertas_en_modelo_pyomo = []
    for i in model.I:
        if any((i, a, h) in model.OFH for a in model.A for h in model.H):
            ofertas_en_modelo_pyomo.append(i)
    
    # Ofertas NO en modelo = todas las demás
    ofertas_fuera_modelo = [oferta for oferta in todas_las_ofertas_originales 
                           if oferta not in ofertas_en_modelo_pyomo]
    
    print(f"\n📊 CLASIFICACIÓN DE OFERTAS:")
    print(f"   Total ofertas originales: {len(todas_las_ofertas_originales)}")
    print(f"   EN modelo Pyomo: {len(ofertas_en_modelo_pyomo)} - {ofertas_en_modelo_pyomo}")
    print(f"   FUERA modelo: {len(ofertas_fuera_modelo)} - {ofertas_fuera_modelo}")
    
    # Obtener fechas del modelo Pyomo
    fechas_modelo_pyomo = sorted(list(model.A))
    horas_modelo = sorted(list(model.H))
    
    print(f"   Fechas en modelo Pyomo: {len(fechas_modelo_pyomo)}")
    print(f"   Rango Pyomo: {min(fechas_modelo_pyomo)} a {max(fechas_modelo_pyomo)}")
    
    # Diccionario para almacenar todos los resultados
    resultados = {}
    
    try:
        # ========================================
        # PASO 2: PROCESAR OFERTAS EN MODELO PYOMO - TODAS SUS FECHAS
        # ========================================
        
        print(f"\n🔄 PASO 2: Procesando ofertas EN modelo Pyomo...")
        
        for oferta in ofertas_en_modelo_pyomo:
            print(f"\n  📋 Procesando oferta: {oferta}")
            
            # Obtener TODAS las fechas originales de esta oferta (no solo las del modelo)
            oferta_data_completa = ofertas_df[ofertas_df['CÓDIGO OFERTA'] == oferta]
            todas_fechas_oferta = sorted(oferta_data_completa['FECHA'].unique())
            
            # Separar fechas EN modelo vs FUERA del modelo
            fechas_en_modelo = [f for f in todas_fechas_oferta if f in fechas_modelo_pyomo]
            fechas_fuera_modelo = [f for f in todas_fechas_oferta if f not in fechas_modelo_pyomo]
            
            print(f"     Fechas EN modelo: {len(fechas_en_modelo)}")
            print(f"     Fechas FUERA modelo: {len(fechas_fuera_modelo)}")
            
            # Inicializar estructuras de datos
            da_rows = []
            ena_rows = []
            total_asignado_pyomo = 0
            total_capacidad_fuera_modelo = 0
            
            # PROCESAR TODAS LAS FECHAS (EN + FUERA del modelo)
            for fecha in todas_fechas_oferta:
                fila_da = {"FECHA": fecha, "X": fecha}
                fila_ena = {"FECHA": fecha, "X": fecha}
                
                # Inicializar todas las horas en 0
                for hora in range(1, 25):
                    fila_da[hora] = 0
                    fila_ena[hora] = 0
                
                if fecha in fechas_en_modelo:
                    # FECHAS EN MODELO PYOMO: Usar resultados del optimizador + recuperar filtrados
                    data_fecha_modelo = oferta_data_completa[oferta_data_completa['FECHA'] == fecha]
                    
                    # PASO 1: Procesar combinaciones EN model.OFH (optimizadas)
                    for hora in horas_modelo:
                        if (oferta, fecha, hora) in model.OFH:
                            # Obtener asignación y capacidad del optimizador
                            asignacion = pyo.value(model.EA[oferta, fecha, hora]) if (oferta, fecha, hora) in model.EA else 0
                            capacidad = pyo.value(model.CO[oferta, fecha, hora]) if (oferta, fecha, hora) in model.CO else 0
                            
                            # DA = Lo que asignó el optimizador
                            fila_da[hora] = asignacion
                            total_asignado_pyomo += asignacion
                            
                            # ENA = Capacidad - asignado
                            fila_ena[hora] = max(0, capacidad - asignacion)
                    
                    # PASO 2: RECUPERAR combinaciones FILTRADAS (no en model.OFH)
                    # Agregar por hora todos los datos originales que NO se optimizaron
                    capacidades_filtradas_por_hora = {}
                    for hora in range(1, 25):
                        capacidades_filtradas_por_hora[hora] = 0
                    
                    # SUMAR todos los registros originales para esta fecha
                    for _, row in data_fecha_modelo.iterrows():
                        hora = int(row['Atributo'])
                        cantidad = row['CANTIDAD']
                        
                        if (1 <= hora <= 24 and not pd.isna(cantidad) and 
                            (oferta, fecha, hora) not in model.OFH):  # Solo los que NO están en OFH
                            capacidades_filtradas_por_hora[hora] += cantidad
                            total_capacidad_fuera_modelo += cantidad
                    
                    # Agregar capacidad filtrada a ENA (DA sigue siendo 0 para estas)
                    for hora in range(1, 25):
                        fila_ena[hora] += capacidades_filtradas_por_hora[hora]
                
                else:
                    # FECHAS FUERA DEL MODELO: Usar datos originales completos
                    data_fecha = oferta_data_completa[oferta_data_completa['FECHA'] == fecha]
                    
                    # Agregar por hora (pueden existir múltiples registros válidos)
                    capacidades_por_hora = {}
                    for hora in range(1, 25):
                        capacidades_por_hora[hora] = 0
                    
                    # SUMAR todos los registros para la misma fecha-hora
                    for _, row in data_fecha.iterrows():
                        hora = int(row['Atributo'])
                        cantidad = row['CANTIDAD']
                        
                        if 1 <= hora <= 24 and not pd.isna(cantidad):
                            capacidades_por_hora[hora] += cantidad
                            total_capacidad_fuera_modelo += cantidad
                    
                    # DA = 0 (no optimizado), ENA = capacidad completa
                    for hora in range(1, 25):
                        fila_da[hora] = 0
                        fila_ena[hora] = capacidades_por_hora[hora]
                
                da_rows.append(fila_da)
                ena_rows.append(fila_ena)
            
            # Guardar resultados
            if da_rows and ena_rows:
                da_df = pd.DataFrame(da_rows)
                ena_df = pd.DataFrame(ena_rows)
                
                resultados[f"DEMANDA ASIGNADA {oferta} IT1_COMPRAR"] = da_df
                resultados[f"DEMANDA ASIGNADA {oferta} IT1_NO_COMPRADA"] = ena_df
                
                print(f"     ✅ DA: {total_asignado_pyomo:.2f} kWh asignados por Pyomo")
                print(f"     ✅ ENA: {total_capacidad_fuera_modelo:.2f} kWh recuperados fuera modelo")
                print(f"     📋 Total fechas procesadas: {len(todas_fechas_oferta)}")
        
        # ========================================
        # PASO 3: PROCESAR OFERTAS COMPLETAMENTE FUERA DEL MODELO
        # ========================================
        
        print(f"\n🔄 PASO 3: Procesando ofertas FUERA del modelo...")
        
        for oferta in ofertas_fuera_modelo:
            print(f"\n  📋 Procesando oferta rechazada: {oferta}")
            
            # USAR DATOS ORIGINALES COMPLETOS
            oferta_data = ofertas_df[ofertas_df['CÓDIGO OFERTA'] == oferta]
            
            if oferta_data.empty:
                print(f"     ⚠️ No hay datos para {oferta}")
                continue
            
            print(f"     📊 Registros encontrados: {len(oferta_data)}")
            
            # Obtener TODAS las fechas de esta oferta
            fechas_oferta = sorted(oferta_data['FECHA'].unique())
            
            # Crear estructura agregando por fecha-hora
            capacidades_totales = {}
            for fecha in fechas_oferta:
                capacidades_totales[fecha] = {}
                for hora in range(1, 25):
                    capacidades_totales[fecha][hora] = 0
            
            # SUMAR todas las capacidades por fecha-hora
            total_capacidad_oferta = 0
            for _, row in oferta_data.iterrows():
                fecha = row['FECHA']
                hora = int(row['Atributo'])
                cantidad = row['CANTIDAD']
                
                if 1 <= hora <= 24 and not pd.isna(cantidad):
                    capacidades_totales[fecha][hora] += cantidad
                    total_capacidad_oferta += cantidad
            
            print(f"     💰 Capacidad total: {total_capacidad_oferta:.2f} kWh")
            
            # Crear DataFrames: DA=0, ENA=capacidad completa
            da_rows = []
            ena_rows = []
            
            for fecha in fechas_oferta:
                fila_da = {"FECHA": fecha, "X": fecha}
                fila_ena = {"FECHA": fecha, "X": fecha}
                
                for hora in range(1, 25):
                    fila_da[hora] = 0  # DA: Nada asignado
                    fila_ena[hora] = capacidades_totales[fecha][hora]  # ENA: Todo disponible
                
                da_rows.append(fila_da)
                ena_rows.append(fila_ena)
            
            # Guardar resultados
            if da_rows and ena_rows:
                da_df = pd.DataFrame(da_rows)
                ena_df = pd.DataFrame(ena_rows)
                
                resultados[f"DEMANDA ASIGNADA {oferta} IT1_COMPRAR"] = da_df
                resultados[f"DEMANDA ASIGNADA {oferta} IT1_NO_COMPRADA"] = ena_df
                
                print(f"     ✅ Hojas generadas - Todo va a ENA: {total_capacidad_oferta:.2f} kWh")
        
        # ========================================
        # PASO 4: VERIFICACIÓN CRÍTICA DE CONSERVACIÓN
        # ========================================
        
        print(f"\n🔍 PASO 4: VERIFICACIÓN CRÍTICA DE CONSERVACIÓN...")
        
        # Calcular total original
        total_original = ofertas_df['CANTIDAD'].sum()
        
        # Calcular total en resultados
        total_da = 0
        total_ena = 0
        
        for clave, df in resultados.items():
            if "_COMPRAR" in clave:
                # Sumar DA
                for _, row in df.iterrows():
                    for hora in range(1, 25):
                        if hora in row and pd.notna(row[hora]):
                            total_da += row[hora]
            
            elif "_NO_COMPRADA" in clave:
                # Sumar ENA
                for _, row in df.iterrows():
                    for hora in range(1, 25):
                        if hora in row and pd.notna(row[hora]):
                            total_ena += row[hora]
        
        total_resultados = total_da + total_ena
        diferencia = abs(total_original - total_resultados)
        
        print(f"📊 VERIFICACIÓN FINAL DE CONSERVACIÓN:")
        print(f"   💾 Total original (CANTIDADES Y PRECIOS): {total_original:,.2f} kWh")
        print(f"   ⚡ Total DA (asignado): {total_da:,.2f} kWh")
        print(f"   🔋 Total ENA (no asignado): {total_ena:,.2f} kWh")
        print(f"   🧮 Total en resultados (DA + ENA): {total_resultados:,.2f} kWh")
        print(f"   📏 Diferencia: {diferencia:,.2f} kWh")
        
        if diferencia < 1:  # Tolerancia de 1 kWh por redondeos
            print(f"   ✅ CONSERVACIÓN PERFECTA - Energía preservada al 100%")
        else:
            porcentaje_perdida = (diferencia / total_original) * 100
            print(f"   ⚠️ PÉRDIDA DETECTADA: {porcentaje_perdida:.4f}% del total")
            
            # Mostrar desglose por oferta para debug
            print(f"\n   🔍 DESGLOSE POR OFERTA:")
            for oferta in todas_las_ofertas_originales:
                oferta_original = ofertas_df[ofertas_df['CÓDIGO OFERTA'] == oferta]['CANTIDAD'].sum()
                
                # Buscar en resultados
                total_oferta_resultados = 0
                key_da = f"DEMANDA ASIGNADA {oferta} IT1_COMPRAR"
                key_ena = f"DEMANDA ASIGNADA {oferta} IT1_NO_COMPRADA"
                
                for key in [key_da, key_ena]:
                    if key in resultados:
                        df_oferta = resultados[key]
                        for _, row in df_oferta.iterrows():
                            for hora in range(1, 25):
                                if hora in row and pd.notna(row[hora]):
                                    total_oferta_resultados += row[hora]
                
                diff_oferta = abs(oferta_original - total_oferta_resultados)
                if diff_oferta > 1:
                    print(f"     🚨 {oferta}: Original {oferta_original:,.0f} vs Resultado {total_oferta_resultados:,.0f} kWh (diff: {diff_oferta:,.0f})")
                else:
                    print(f"     ✅ {oferta}: Conservación correcta ({oferta_original:,.0f} kWh)")
        
        # ========================================
        # PASO 5: GENERAR DEMANDA FALTANTE Y RESUMEN
        # ========================================
        
        print(f"\n🔄 PASO 5: Generando demanda faltante y resumen...")
        
        # Calcular demanda faltante (solo para fechas del modelo)
        demanda_faltante = []
        for fecha in fechas_modelo_pyomo:
            fila = {"FECHA": fecha, "X": fecha}
            
            for hora in range(1, 25):
                # Demanda total
                demanda_total = pyo.value(model.D[fecha, hora]) if (fecha, hora) in model.D else 0
                
                # Energía asignada total (solo de ofertas en modelo)
                energia_asignada = 0
                for oferta in ofertas_en_modelo_pyomo:
                    if (oferta, fecha, hora) in model.EA:
                        energia_asignada += pyo.value(model.EA[oferta, fecha, hora])
                
                # Déficit
                deficit = max(0, demanda_total - energia_asignada)
                fila[hora] = deficit
            
            demanda_faltante.append(fila)
        
        if demanda_faltante:
            df_faltante = pd.DataFrame(demanda_faltante)
            resultados["DEMANDA_FALTANTE"] = df_faltante
            print(f"   ✅ Demanda faltante calculada")
        
        # Generar resumen ejecutivo usando TODAS las ofertas
        print(f"   🔄 Generando resumen ejecutivo...")
        resumen_ejecutivo_rows = []
        
        # Agrupar fechas por mes (usar todas las fechas disponibles)
        todas_fechas_resultados = set()
        for clave, df in resultados.items():
            if "_COMPRAR" in clave and not df.empty:
                todas_fechas_resultados.update(df['FECHA'].unique())
        
        fechas_por_mes = {}
        for fecha in sorted(todas_fechas_resultados):
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
        if demanda_faltante:
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
            for oferta in todas_las_ofertas_originales:
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
                                    # Buscar precio en datos originales
                                    precio_indexado = 0
                                    precio_sin_indexar = 0
                                    
                                    # Para ofertas en modelo, usar precio del modelo si disponible
                                    if oferta in ofertas_en_modelo_pyomo and (oferta, fecha, hora) in model.OFH:
                                        precio_indexado = pyo.value(model.PO[oferta, fecha, hora])
                                        precio_sin_indexar = precio_indexado
                                    
                                    # Buscar precio original en ofertas_df
                                    try:
                                        ofertas_filtradas = ofertas_df[
                                            (ofertas_df['CÓDIGO OFERTA'] == oferta) & 
                                            (ofertas_df['FECHA'] == fecha) & 
                                            (ofertas_df['Atributo'] == hora)
                                        ]
                                        
                                        if not ofertas_filtradas.empty and 'PRECIO' in ofertas_filtradas.columns:
                                            precio_original = ofertas_filtradas['PRECIO'].mean()
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
            print(f"   ✅ Resumen ejecutivo generado")
        
        # ========================================
        # PASO 6: ESTADÍSTICAS FINALES
        # ========================================
        
        total_demanda = sum(pyo.value(model.D[a, h]) for a in model.A for h in model.H)
        total_asignado_pyomo = sum(pyo.value(model.EA[oferta, fecha, hora]) 
                                  for oferta in ofertas_en_modelo_pyomo 
                                  for fecha in model.A 
                                  for hora in model.H 
                                  if (oferta, fecha, hora) in model.EA)
        
        logger.info(f"Demanda total: {total_demanda:.2f} kWh")
        logger.info(f"Energía asignada por Pyomo: {total_asignado_pyomo:.2f} kWh")
        
        print(f"\n📋 RESUMEN FINAL COMPLETO:")
        print(f"   🎯 Total ofertas procesadas: {len(todas_las_ofertas_originales)}")
        print(f"   ⚡ Ofertas en modelo Pyomo: {len(ofertas_en_modelo_pyomo)}")
        print(f"   🚫 Ofertas fuera modelo: {len(ofertas_fuera_modelo)}")
        print(f"   📊 Hojas DA/ENA generadas: {len([k for k in resultados.keys() if 'DEMANDA ASIGNADA' in k])}")
        print(f"   ✅ Conservación de energía: {'PERFECTA' if diferencia < 1 else f'PÉRDIDA {diferencia:,.0f} kWh'}")
        
    except Exception as e:
        print(f"🚨 ERROR GENERAL en extraer_resultados: {e}")
        import traceback
        traceback.print_exc()
        return {}
    
    return resultados