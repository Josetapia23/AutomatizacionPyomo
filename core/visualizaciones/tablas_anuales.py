# core/visualizaciones/tablas_anuales.py

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.offline as pyo
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def extraer_datos_mensuales(resultados_dict):
    """
    Extrae datos del RESUMEN EJECUTIVO organizados por mes/año y oferta.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de optimización
        
    Returns:
        dict: Diccionario con estructura {
            'fechas': ['09/2025', '10/2025', ...],
            'ofertas': ['OFERTA-001', 'OFERTA-002', ...],
            'cantidades': {fecha: {oferta: cantidad_kwh}},
            'precios_indexados': {fecha: {oferta: precio}},
            'precios_no_indexados': {fecha: {oferta: precio}}
        }
    """
    logger.info("Extrayendo datos mensuales del RESUMEN EJECUTIVO")
    
    # Verificar que existe el resumen ejecutivo
    if "RESUMEN EJECUTIVO" not in resultados_dict:
        logger.error("No se encontró RESUMEN EJECUTIVO en resultados")
        return None
    
    resumen_df = resultados_dict["RESUMEN EJECUTIVO"]
    
    if resumen_df.empty:
        logger.error("RESUMEN EJECUTIVO está vacío")
        return None
    
    print(f"\n📊 Extrayendo datos mensuales para tablas...")
    print(f"   Filas en resumen: {len(resumen_df)}")
    
    # 🔧 DEBUG: Imprimir todas las columnas para ver el formato real
    print(f"\n🔍 DEBUG - Columnas del RESUMEN EJECUTIVO:")
    for i, col in enumerate(resumen_df.columns[:10]):  # Mostrar primeras 10
        print(f"   {i}: '{col}'")
    
    # 🔧 CORREGIDO: Identificar todas las ofertas únicas
    ofertas = set()
    for col in resumen_df.columns:
        # Buscar columnas que terminan con "CANTIDAD (KWh)"
        if col.endswith("CANTIDAD (KWh)"):
            # Extraer el nombre de la oferta (todo antes de " CANTIDAD")
            oferta = col.replace(" CANTIDAD (KWh)", "").strip()
            ofertas.add(oferta)
            print(f"   ✓ Oferta encontrada: '{oferta}'")
    
    if not ofertas:
        logger.error("No se encontraron ofertas en el RESUMEN EJECUTIVO")
        print("❌ No se encontraron columnas con 'CANTIDAD (KWh)'")
        return None
    
    ofertas = sorted(list(ofertas))
    print(f"\n   📋 Total ofertas encontradas: {len(ofertas)}")
    print(f"   📋 Nombres de ofertas:")
    for i, oferta in enumerate(ofertas[:5], 1):  # Mostrar primeras 5
        print(f"      {i}. '{oferta}'")
    if len(ofertas) > 5:
        print(f"      ... y {len(ofertas) - 5} más")
    
    # Inicializar estructuras de datos
    datos_mensuales = {
        'fechas': [],
        'ofertas': ofertas,
        'cantidades': {},          # {fecha: {oferta: cantidad_kwh}}
        'precios_indexados': {},   # {fecha: {oferta: precio}}
        'precios_no_indexados': {} # {fecha: {oferta: precio}}
    }
    
    # 🔧 DEBUG: Imprimir primeras fechas para verificar formato
    print(f"\n🔍 DEBUG - Primeras fechas en el resumen:")
    for i, fecha in enumerate(resumen_df['FECHA'].head(3)):
        print(f"   {i}: '{fecha}' (tipo: {type(fecha).__name__})")
    
    # Procesar cada fila del resumen (cada fila = un mes)
    for idx, row in resumen_df.iterrows():
        fecha_str = row['FECHA']  # Formato: MM/YYYY
        
        # 🔧 CORREGIDO: Normalizar formato de fecha
        try:
            if isinstance(fecha_str, str):
                # Ya está en formato string, verificar si tiene el formato correcto
                if '/' in fecha_str:
                    # Formato MM/YYYY o DD/MM/YYYY
                    partes = fecha_str.split('/')
                    if len(partes) == 2:
                        # Ya está en formato MM/YYYY
                        fecha_display = fecha_str
                    elif len(partes) == 3:
                        # Formato DD/MM/YYYY, convertir a MM/YYYY
                        mes = partes[1]
                        año = partes[2]
                        fecha_display = f"{mes}/{año}"
                    else:
                        print(f"   ⚠️ Formato de fecha no reconocido: {fecha_str}")
                        continue
                else:
                    print(f"   ⚠️ Fecha sin separador '/': {fecha_str}")
                    continue
            else:
                # Si es datetime, convertir a MM/YYYY
                try:
                    fecha_display = fecha_str.strftime('%m/%Y')
                except:
                    print(f"   ⚠️ No se pudo convertir fecha: {fecha_str}")
                    continue
        except Exception as e:
            logger.warning(f"Error procesando fecha: {fecha_str} - {e}")
            continue
        
        # Agregar fecha a la lista
        datos_mensuales['fechas'].append(fecha_display)
        
        # Inicializar diccionarios para esta fecha
        datos_mensuales['cantidades'][fecha_display] = {}
        datos_mensuales['precios_indexados'][fecha_display] = {}
        datos_mensuales['precios_no_indexados'][fecha_display] = {}
        
        # Procesar cada oferta
        for oferta in ofertas:
            # 🔧 CORREGIDO: Construir nombres de columnas exactos
            col_cantidad = f"{oferta} CANTIDAD (KWh)"
            col_precio_indexado = f"{oferta} PRECIO INDEXADO ($/KWh)"
            col_precio_no_indexado = f"{oferta} PRECIO ($/KWh)"
            
            # Extraer cantidad
            if col_cantidad in resumen_df.columns:
                cantidad = row[col_cantidad]
                if pd.notna(cantidad):
                    datos_mensuales['cantidades'][fecha_display][oferta] = float(cantidad)
                else:
                    datos_mensuales['cantidades'][fecha_display][oferta] = 0
            else:
                datos_mensuales['cantidades'][fecha_display][oferta] = 0
            
            # Extraer precio indexado
            if col_precio_indexado in resumen_df.columns:
                precio = row[col_precio_indexado]
                if pd.notna(precio):
                    datos_mensuales['precios_indexados'][fecha_display][oferta] = float(precio)
                else:
                    datos_mensuales['precios_indexados'][fecha_display][oferta] = 0
            else:
                datos_mensuales['precios_indexados'][fecha_display][oferta] = 0
            
            # Extraer precio no indexado
            if col_precio_no_indexado in resumen_df.columns:
                precio = row[col_precio_no_indexado]
                if pd.notna(precio):
                    datos_mensuales['precios_no_indexados'][fecha_display][oferta] = float(precio)
                else:
                    datos_mensuales['precios_no_indexados'][fecha_display][oferta] = 0
            else:
                datos_mensuales['precios_no_indexados'][fecha_display][oferta] = 0
    
    if not datos_mensuales['fechas']:
        logger.error("No se procesaron fechas válidas")
        print("❌ No se pudieron procesar las fechas")
        return None
    
    print(f"\n   ✅ Meses procesados: {len(datos_mensuales['fechas'])}")
    print(f"   ✅ Rango: {datos_mensuales['fechas'][0]} - {datos_mensuales['fechas'][-1]}")
    print(f"✅ Datos mensuales extraídos correctamente")
    
    return datos_mensuales

def acortar_nombre_oferta(nombre_completo):
    """
    Acorta el nombre de la oferta para mejor visualización.
    Ejemplo: 'AES-OFERTA-001' -> 'AES-001'
    
    Args:
        nombre_completo (str): Nombre completo de la oferta
        
    Returns:
        str: Nombre acortado
    """
    # Remover la palabra "OFERTA-" o "OFERTA" del medio
    nombre_corto = nombre_completo.replace("-OFERTA-", "-").replace("OFERTA-", "")
    return nombre_corto

def calcular_tablas_resumen(datos_mensuales):
    """
    Calcula las 4 tablas a partir de los datos mensuales.
    
    Args:
        datos_mensuales (dict): Datos extraídos por extraer_datos_mensuales()
        
    Returns:
        dict: {
            'tabla_funcion_objetivo': {...},
            'tabla_cantidades': {...},
            'tabla_precio_indexado': {...},
            'tabla_precio_no_indexado': {...}
        }
    """
    logger.info("Calculando las 4 tablas de resumen")
    print(f"\n🔢 Calculando tablas de resumen...")
    
    fechas = datos_mensuales['fechas']
    ofertas = datos_mensuales['ofertas']
    
    # Inicializar tablas
    tablas = {
        'tabla_funcion_objetivo': {
            'fechas': fechas.copy(),
            'ofertas': ofertas,
            'datos': {}  # {fecha: {oferta: valor}}
        },
        'tabla_cantidades': {
            'fechas': fechas.copy(),
            'ofertas': ofertas,
            'datos': {}
        },
        'tabla_precio_indexado': {
            'fechas': fechas.copy(),
            'ofertas': ofertas,
            'datos': {}
        },
        'tabla_precio_no_indexado': {
            'fechas': fechas.copy(),
            'ofertas': ofertas,
            'datos': {}
        }
    }
    
    # Totales para función objetivo
    totales_funcion_objetivo = {oferta: 0 for oferta in ofertas}
    
    # Calcular valores para cada fecha y oferta
    for fecha in fechas:
        # Inicializar diccionarios para esta fecha
        tablas['tabla_funcion_objetivo']['datos'][fecha] = {}
        tablas['tabla_cantidades']['datos'][fecha] = {}
        tablas['tabla_precio_indexado']['datos'][fecha] = {}
        tablas['tabla_precio_no_indexado']['datos'][fecha] = {}
        
        for oferta in ofertas:
            # Obtener datos del mes
            cantidad_kwh = datos_mensuales['cantidades'][fecha][oferta]
            precio_indexado = datos_mensuales['precios_indexados'][fecha][oferta]
            precio_no_indexado = datos_mensuales['precios_no_indexados'][fecha][oferta]
            
            # TABLA 1: Función Objetivo (Millones $)
            # = (Cantidad_kWh × Precio_Indexado) / 1,000,000
            funcion_objetivo = (cantidad_kwh * precio_indexado) / 1_000_000
            tablas['tabla_funcion_objetivo']['datos'][fecha][oferta] = funcion_objetivo
            totales_funcion_objetivo[oferta] += funcion_objetivo
            
            # TABLA 2: Cantidades (GWh)
            # = Cantidad_kWh / 1,000,000
            cantidad_gwh = cantidad_kwh / 1_000_000
            tablas['tabla_cantidades']['datos'][fecha][oferta] = cantidad_gwh
            
            # TABLA 3: Precio Indexado ($/kWh)
            # = Precio directo del resumen
            tablas['tabla_precio_indexado']['datos'][fecha][oferta] = precio_indexado
            
            # TABLA 4: Precio No Indexado ($/kWh)
            # = Precio directo del resumen
            tablas['tabla_precio_no_indexado']['datos'][fecha][oferta] = precio_no_indexado
    
    # Agregar fila de totales a función objetivo
    tablas['tabla_funcion_objetivo']['totales'] = totales_funcion_objetivo
    
    print(f"✅ Tablas calculadas correctamente")
    print(f"   - Función Objetivo con totales: {len(fechas)} meses + 1 fila total")
    print(f"   - Otras 3 tablas: {len(fechas)} meses cada una")
    
    return tablas

def crear_tabla_plotly(titulo, datos_tabla, ofertas, incluir_totales=False, formato='dinero', nota=None):
    """
    Crea una tabla de Plotly con formato específico.
    """
    fechas = datos_tabla['fechas']
    datos = datos_tabla['datos']
    
    # Preparar encabezados con nombres acortados
    headers = ['MES/AÑO']
    for oferta in ofertas:
        nombre_corto = acortar_nombre_oferta(oferta)
        headers.append(nombre_corto)
    
    # Preparar datos por columna
    columnas = []
    
    # Primera columna: fechas
    col_fechas = fechas.copy()
    if incluir_totales:
        col_fechas.append('TOTAL')
    columnas.append(col_fechas)
    
    # Columnas de ofertas
    for oferta in ofertas:
        col_valores = []
        for fecha in fechas:
            valor = datos[fecha][oferta]
            
            # No mostrar valores muy pequeños o cero
            if valor == 0 or abs(valor) < 0.01:
                col_valores.append("-")
            else:
                # Formatear según tipo
                if formato == 'dinero':
                    col_valores.append(f"${valor:,.2f}")
                elif formato == 'gwh':
                    col_valores.append(f"{valor:.2f}")
                elif formato == 'precio':
                    col_valores.append(f"${valor:.2f}")
                else:
                    col_valores.append(f"{valor:.2f}")
        
        # Agregar total si corresponde
        if incluir_totales and 'totales' in datos_tabla:
            total = datos_tabla['totales'][oferta]
            if total == 0 or abs(total) < 0.01:
                col_valores.append("-")
            else:
                if formato == 'dinero':
                    col_valores.append(f"<b>${total:,.2f}</b>")
                else:
                    col_valores.append(f"<b>{total:.2f}</b>")
        
        columnas.append(col_valores)
    
    # Colores para la tabla
    header_color = '#1f4e79'
    cell_colors = []
    
    # Color de celdas: alternar filas
    num_filas = len(col_fechas)
    for i in range(len(columnas)):
        if i == 0:
            # Primera columna (fechas) en gris claro
            colors = ['#e8e8e8' if j % 2 == 0 else '#f5f5f5' for j in range(num_filas)]
        else:
            # Otras columnas en blanco/gris muy claro
            colors = ['#ffffff' if j % 2 == 0 else '#f9f9f9' for j in range(num_filas)]
        
        # Si hay totales, última fila en amarillo claro
        if incluir_totales and num_filas > 0:
            colors[-1] = '#fff3cd'
        
        cell_colors.append(colors)
    
    # Calcular ancho de columnas dinámicamente
    # Primera columna (fechas): fija en 100px
    # Otras columnas: ajustar según el nombre más largo
    ancho_fecha = 100
    
    # Encontrar el nombre más largo de oferta
    max_len_nombre = max(len(acortar_nombre_oferta(o)) for o in ofertas)
    # Aproximadamente 10px por carácter, mínimo 90px, máximo 150px
    ancho_oferta = min(max(max_len_nombre * 10, 90), 150)
    
    column_widths = [ancho_fecha] + [ancho_oferta] * len(ofertas)
    
    # Crear tabla
    tabla = go.Table(
        header=dict(
            values=[f'<b>{h}</b>' for h in headers],
            fill_color=header_color,
            align=['left'] + ['right'] * len(ofertas),
            font=dict(color='white', size=11, family='Arial'),
            height=40
        ),
        cells=dict(
            values=columnas,
            fill_color=cell_colors,
            align=['left'] + ['right'] * len(ofertas),
            font=dict(color='#2c3e50', size=10, family='Arial'),
            height=30,
            line=dict(color='#dddddd', width=1)
        ),
        columnwidth=column_widths
    )
    
    return tabla

def generar_tabla_html(titulo, datos_tabla, ofertas, incluir_totales=False, formato='dinero'):
    """
    Genera una tabla HTML pura con scroll y columnas/filas fijas.
    ACTUALIZADO: Filtra filas vacías y muestra total único.
    """
    fechas = datos_tabla['fechas']
    datos = datos_tabla['datos']
    
    # Preparar encabezados con nombres acortados
    headers_ofertas = [acortar_nombre_oferta(oferta) for oferta in ofertas]
    
    # Filtrar fechas que tienen al menos un valor
    fechas_con_datos = []
    for fecha in fechas:
        tiene_datos = False
        for oferta in ofertas:
            valor = datos[fecha][oferta]
            if valor != 0 and abs(valor) >= 0.01:
                tiene_datos = True
                break
        if tiene_datos:
            fechas_con_datos.append(fecha)
    
    print(f"   📅 Fechas filtradas: {len(fechas)} → {len(fechas_con_datos)} (con datos)")
    
    # Iniciar HTML de la tabla
    filas_html = []
    
    # Generar filas de datos (solo fechas con datos)
    for fecha in fechas_con_datos:
        celdas = [f'<td class="fecha-col">{fecha}</td>']
        
        for oferta in ofertas:
            valor = datos[fecha][oferta]
            
            # Formatear valor
            if valor == 0 or abs(valor) < 0.01:
                valor_formateado = "-"
            else:
                if formato == 'dinero':
                    valor_formateado = f"${valor:,.2f}"
                elif formato == 'gwh':
                    valor_formateado = f"{valor:.2f}"
                elif formato == 'precio':
                    valor_formateado = f"${valor:.2f}"
                else:
                    valor_formateado = f"{valor:.2f}"
            
            celdas.append(f'<td class="dato-col">{valor_formateado}</td>')
        
        filas_html.append(f'<tr>{"".join(celdas)}</tr>')
    
    # Agregar fila de TOTAL ÚNICO si corresponde
    if incluir_totales and 'totales' in datos_tabla:
        # Calcular el gran total sumando todos los totales por oferta
        gran_total = sum(datos_tabla['totales'].values())
        
        if formato == 'dinero':
            total_formateado = f"${gran_total:,.2f}"
        else:
            total_formateado = f"{gran_total:.2f}"
        
        # Fila de total: primera celda con "TOTAL", última celda con el valor, resto vacías
        num_columnas = len(ofertas) + 1  # +1 por la columna de fechas
        celdas_total = [
            '<td class="fecha-col total-row"><strong>TOTAL</strong></td>'
        ]
        # Celdas vacías para todas las ofertas menos la última
        for i in range(len(ofertas) - 1):
            celdas_total.append('<td class="dato-col total-row"></td>')
        # Última celda con el gran total
        celdas_total.append(f'<td class="dato-col total-row total-final"><strong>{total_formateado}</strong></td>')
        
        filas_html.append(f'<tr>{"".join(celdas_total)}</tr>')
    
    # Construir encabezados de columna
    headers_html = '<th class="fecha-col">MES/AÑO</th>'
    for header in headers_ofertas:
        headers_html += f'<th class="dato-col">{header}</th>'
    
    # Construir tabla completa
    tabla_html = f'''
    <div class="tabla-scroll-container">
        <table class="tabla-datos">
            <thead>
                <tr>{headers_html}</tr>
            </thead>
            <tbody>
                {"".join(filas_html)}
            </tbody>
        </table>
    </div>
    '''
    
    return tabla_html

def crear_html_con_scroll_individual(fig1, fig2, fig3, fig4):
    """
    Crea HTML personalizado con tablas HTML puras que tienen scroll horizontal
    con primera columna y primera fila fijas.
    
    Args:
        fig1, fig2, fig3, fig4: No se usan, solo por compatibilidad
        
    Returns:
        str: Contenido HTML completo
    """
    # Las figuras de Plotly ya no se usan, se pasan por compatibilidad pero no se utilizan
    # En su lugar generaremos tablas HTML directamente
    return None  # Esta función se reemplaza completamente abajo


def generar_tablas_resumen_anual(resultados_dict, output_dir):
    """
    Función principal que genera las 4 tablas de resumen anual con HTML puro.
    
    Args:
        resultados_dict (dict): Diccionario con resultados de optimización
        output_dir (Path): Directorio donde guardar el archivo HTML
        
    Returns:
        Path: Ruta al archivo HTML generado, o None si hay error
    """
    logger.info("=== GENERANDO TABLAS DE RESUMEN ANUAL ===")
    print(f"\n" + "="*60)
    print(f"📊 GENERANDO TABLAS DE RESUMEN ANUAL")
    print(f"="*60)
    
    try:
        # Paso 1: Extraer datos mensuales
        datos_mensuales = extraer_datos_mensuales(resultados_dict)
        if datos_mensuales is None:
            logger.error("No se pudieron extraer datos mensuales")
            return None
        
        # Paso 2: Calcular las 4 tablas
        tablas = calcular_tablas_resumen(datos_mensuales)
        if tablas is None:
            logger.error("No se pudieron calcular las tablas")
            return None
        
        ofertas = tablas['tabla_funcion_objetivo']['ofertas']
        
        # Paso 3: Generar HTML de cada tabla
        print(f"\n🎨 Generando tablas HTML...")
        
        html_tabla1 = generar_tabla_html(
            'FUNCIÓN OBJETIVO',
            tablas['tabla_funcion_objetivo'],
            ofertas,
            incluir_totales=True,
            formato='dinero'
        )
        
        html_tabla2 = generar_tabla_html(
            'CANTIDADES GWh-año',
            tablas['tabla_cantidades'],
            ofertas,
            incluir_totales=False,
            formato='gwh'
        )
        
        html_tabla3 = generar_tabla_html(
            'PRECIO INDEXADO ($/kWh)',
            tablas['tabla_precio_indexado'],
            ofertas,
            incluir_totales=False,
            formato='precio'
        )
        
        html_tabla4 = generar_tabla_html(
            'PRECIO NO INDEXADO ($/kWh)',
            tablas['tabla_precio_no_indexado'],
            ofertas,
            incluir_totales=False,
            formato='precio'
        )
        
        # Paso 4: Crear HTML completo
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Resumen Anual de Optimización Energética</title>
    <style>
        * {{
            box-sizing: border-box;
        }}
        
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f8f9fa;
        }}
        
        .header {{
            text-align: center;
            color: #1f4e79;
            margin-bottom: 30px;
            position: sticky;
            top: 0;
            background-color: #f8f9fa;
            z-index: 200;
            padding: 15px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .header h1 {{
            margin: 0;
        }}
        
        .grid-container {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 25px;
            max-width: 100%;
        }}
        
        .table-wrapper {{
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            padding: 0;
            overflow: hidden;
        }}
        
        .table-title {{
            background: #1f4e79;
            color: white;
            padding: 15px;
            margin: 0;
            font-size: 16px;
            font-weight: bold;
            text-align: center;
            position: sticky;
            top: 0;
            z-index: 150;
        }}
        
        .tabla-scroll-container {{
            overflow: auto;
            max-height: 500px;
            position: relative;
        }}
        
        .tabla-datos {{
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            font-size: 11px;
        }}
        
        .tabla-datos thead {{
            position: sticky;
            top: 0;
            z-index: 100;
        }}
        
        .tabla-datos th {{
            background-color: #1f4e79;
            color: white;
            padding: 12px 8px;
            text-align: center;
            font-weight: bold;
            border: 1px solid #ddd;
            white-space: nowrap;
        }}
        
        .tabla-datos td {{
            padding: 10px 8px;
            border: 1px solid #ddd;
            text-align: right;
        }}
        
        .tabla-datos tbody tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        
        .tabla-datos tbody tr:hover {{
            background-color: #e8f4f8;
        }}
        
        /* Primera columna fija (fechas) */
        .fecha-col {{
            position: sticky;
            left: 0;
            background-color: #e8e8e8;
            z-index: 50;
            text-align: left !important;
            font-weight: 500;
            min-width: 100px;
            box-shadow: 2px 0 4px rgba(0,0,0,0.1);
        }}
        
        thead .fecha-col {{
            background-color: #1f4e79;
            z-index: 110;
        }}
        
        /* Fila de totales */
        .total-row {{
            background-color: #fff3cd !important;
            font-weight: bold;
        }}
        
        /* Celda de total final */
        .total-final {{
            background-color: #ffc107 !important;
            color: #000;
            font-size: 13px;
            text-align: right !important;
        }}
        
        /* Scrollbar personalizado */
        .tabla-scroll-container::-webkit-scrollbar {{
            height: 12px;
            width: 12px;
        }}
        
        .tabla-scroll-container::-webkit-scrollbar-track {{
            background: #f1f1f1;
            border-radius: 10px;
        }}
        
        .tabla-scroll-container::-webkit-scrollbar-thumb {{
            background: #888;
            border-radius: 10px;
        }}
        
        .tabla-scroll-container::-webkit-scrollbar-thumb:hover {{
            background: #555;
        }}
        
        .nota {{
            text-align: center;
            margin-top: 30px;
            padding: 15px;
            background-color: #fff3cd;
            border: 1px solid #ffc107;
            border-radius: 5px;
            color: #856404;
            font-size: 14px;
        }}
        
        @media (max-width: 1400px) {{
            .grid-container {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📈 RESUMEN ANUAL DE OPTIMIZACIÓN ENERGÉTICA</h1>
    </div>
    
    <div class="grid-container">
        <div class="table-wrapper">
            <div class="table-title">📊 FUNCIÓN OBJETIVO</div>
            {html_tabla1}
        </div>
        
        <div class="table-wrapper">
            <div class="table-title">⚡ CANTIDADES GWh-año</div>
            {html_tabla2}
        </div>
        
        <div class="table-wrapper">
            <div class="table-title">💰 PRECIO INDEXADO ($/kWh)</div>
            {html_tabla3}
        </div>
        
        <div class="table-wrapper">
            <div class="table-title">💵 PRECIO NO INDEXADO ($/kWh)</div>
            {html_tabla4}
        </div>
    </div>
    
    <div class="nota">
        <strong>Nota:</strong> Función Objetivo en unidades de millones. Cantidades en GWh. Precios en $/kWh.
    </div>
</body>
</html>
"""
        
        # Paso 5: Guardar archivo HTML
        archivo_salida = output_dir / "09_tablas_resumen_anual.html"
        with open(archivo_salida, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"\n✅ TABLAS DE RESUMEN ANUAL GENERADAS")
        print(f"📁 Archivo: {archivo_salida.name}")
        print(f"="*60)
        
        logger.info(f"Tablas de resumen anual guardadas en: {archivo_salida}")
        return archivo_salida
        
    except Exception as e:
        logger.exception(f"Error al generar tablas de resumen anual: {e}")
        print(f"\n❌ ERROR: {e}")
        return None
    
