"""
Visualizaciones básicas para el sistema de optimización energética.
Incluye gráficas de resumen, tortas y otras visualizaciones sencillas.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging

from .utils import format_number, convert_to_gwh, format_date_for_display

# Configurar logging
logger = logging.getLogger(__name__)

def calcular_totales_energia(resultados_dict):
    """
    FUNCIÓN AUXILIAR: Calcula cuánta energía total se asignó y cuánta quedó sin asignar.
    
    Esta función revisa todos los resultados de la optimización y suma:
    - Energía ASIGNADA: Lo que SÍ se compró de las ofertas
    - Energía NO ASIGNADA: Lo que quedó sin cubrir (demanda faltante)
    
    Args:
        resultados_dict (dict): Diccionario con todos los resultados de la optimización
        
    Returns:
        tuple: (total_asignada_gwh, total_no_asignada_gwh, demanda_faltante_gwh)
    """
    total_asignada = 0
    total_no_asignada = 0
    demanda_faltante = 0
    
    # Revisar todas las hojas de resultados
    for nombre_hoja, df in resultados_dict.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            
            # CASO 1: Hojas de demanda asignada (DA) = Lo que SÍ se compró
            if "DEMANDA ASIGNADA" in nombre_hoja and "_COMPRAR" in nombre_hoja:
                # Sumar todas las columnas de horas (1-24)
                for col in df.columns:
                    if isinstance(col, int) and 1 <= col <= 24:
                        total_asignada += df[col].sum()
            
            # CASO 2: Hoja de demanda faltante = Lo que NO se pudo cubrir
            elif nombre_hoja == "DEMANDA_FALTANTE":
                for col in df.columns:
                    if isinstance(col, int) and 1 <= col <= 24:
                        demanda_faltante += df[col].sum()
    
    # Convertir de kWh a GWh (dividir por 1,000,000)
    total_asignada_gwh = convert_to_gwh(total_asignada)
    demanda_faltante_gwh = convert_to_gwh(demanda_faltante)
    
    # La energía no asignada es igual a la demanda faltante
    total_no_asignada_gwh = demanda_faltante_gwh
    
    return total_asignada_gwh, total_no_asignada_gwh, demanda_faltante_gwh

def obtener_precios_extremos(resultados_dict):
    """
    FUNCIÓN AUXILIAR: Busca el precio más alto, más bajo y promedio ponderado.
    
    Revisa el resumen ejecutivo para encontrar:
    - Precio MÍNIMO de adjudicación
    - Precio MÁXIMO de adjudicación  
    - Precio PROMEDIO ponderado por cantidad
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        tuple: (precio_min, precio_max, precio_ponderado)
    """
    precio_min = float('inf')  # Inicializar con infinito
    precio_max = 0
    total_energia = 0
    total_costo = 0
    
    # Buscar en el resumen ejecutivo
    if "RESUMEN EJECUTIVO" in resultados_dict:
        df_resumen = resultados_dict["RESUMEN EJECUTIVO"]
        
        # Buscar todas las columnas que contienen precios indexados
        for columna in df_resumen.columns:
            if "PRECIO INDEXADO" in columna:
                precios = df_resumen[columna].dropna()
                if not precios.empty:
                    precio_min = min(precio_min, precios.min())
                    precio_max = max(precio_max, precios.max())
            
            # Para calcular precio ponderado: buscar cantidades
            if "CANTIDAD" in columna:
                nombre_oferta = columna.split(" CANTIDAD")[0]
                columna_precio = f"{nombre_oferta} PRECIO INDEXADO"
                
                if columna_precio in df_resumen.columns:
                    for _, fila in df_resumen.iterrows():
                        cantidad = fila.get(columna, 0)
                        precio = fila.get(columna_precio, 0)
                        
                        if cantidad > 0 and precio > 0:
                            total_energia += cantidad
                            total_costo += cantidad * precio
    
    # Calcular precio promedio ponderado
    precio_ponderado = total_costo / total_energia if total_energia > 0 else 0
    
    # Si no se encontraron precios, usar 0
    if precio_min == float('inf'):
        precio_min = 0
    
    return precio_min, precio_max, precio_ponderado

def crear_grafica_torta_energia(resultados_dict):
    """
    GRÁFICA DE TORTA: Muestra el porcentaje de energía asignada vs no asignada.
    
    Crea una gráfica circular (tipo pastel) que muestra:
    - % de energía que SÍ se asignó a ofertas
    - % de energía que NO se pudo asignar
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Gráfica de torta interactiva
    """
    # Calcular totales usando la función auxiliar
    total_asignada, total_no_asignada, _ = calcular_totales_energia(resultados_dict)
    total_energia = total_asignada + total_no_asignada
    
    # Calcular porcentajes
    pct_asignada = (total_asignada / total_energia * 100) if total_energia > 0 else 0
    pct_no_asignada = (total_no_asignada / total_energia * 100) if total_energia > 0 else 0
    
    # Crear la figura
    fig = go.Figure()
    
    # Añadir la gráfica de torta
    fig.add_trace(
        go.Pie(
            labels=['Energía Asignada', 'Energía No Asignada'],
            values=[total_asignada, total_no_asignada],
            marker=dict(colors=['#2E86AB', '#E63946']),  # Azul y rojo
            hoverinfo='label+percent+value',
            hovertemplate='%{label}<br>%{value:.2f} GWh<br>%{percent}<extra></extra>',
            textinfo='percent+label',
            texttemplate='%{percent:.1f}%<br>%{label}',
            hole=0.4,  # Hacer un donut (hueco en el centro)
            pull=[0.05, 0],  # Separar un poco la primera sección
            insidetextfont=dict(color='white')
        )
    )
    
    # Añadir texto en el centro
    fig.add_annotation(
        text=f"<b>{total_energia:.1f} GWh</b><br>Total",
        x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=15),
        xref="paper", yref="paper"
    )
    
    # Configurar el diseño
    fig.update_layout(
        title_text="<b>Distribución Porcentual de Energía</b>",
        height=500,
        font=dict(family="Arial, sans-serif", size=12),
        hoverlabel=dict(bgcolor="white", font_size=14),
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
        margin=dict(t=100, b=100, l=80, r=80)
    )
    
    return fig

def crear_grafica_barras_por_hora(resultados_dict):
    """
    GRÁFICA PRINCIPAL: Energía asignada vs no asignada por cada hora del día.
    
    Esta es la gráfica que solicita el cliente:
    - Barras azules: GWh asignados por hora
    - Barras grises: GWh no asignados por hora  
    - Línea verde: % de energía no asignada por hora
    
    Replica las fórmulas de Excel del cliente:
    - GWh Asignados = SUMAR.SI.CONJUNTO donde TIPO="DA"
    - GWh No Asignado = SUMAR.SI.CONJUNTO donde TIPO="ENA"
    - % No Asignado = GWh_No_Asignado / (GWh_Asignado + GWh_No_Asignado)
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        
    Returns:
        plotly.graph_objects.Figure: Gráfica interactiva
    """
    print("🔍 Creando gráfica de barras por hora...")
    
    # Preparar arrays para las 24 horas del día
    horas = list(range(1, 25))
    gwh_asignados_por_hora = [0] * 24
    gwh_no_asignados_por_hora = [0] * 24
    porcentaje_no_asignado_por_hora = [0] * 24
    
    # PASO 1: CALCULAR GWh ASIGNADOS por hora
    # (Equivale a la fórmula del cliente: SUMAR.SI.CONJUNTO donde TIPO="DA")
    print("   📊 Calculando energía ASIGNADA por hora...")
    for nombre_hoja, df in resultados_dict.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            # Buscar hojas de "DEMANDA ASIGNADA" que terminan en "_COMPRAR"
            if "DEMANDA ASIGNADA" in nombre_hoja and "_COMPRAR" in nombre_hoja:
                print(f"      Procesando: {nombre_hoja}")
                
                # Para cada hora (columnas 1-24)
                for hora in horas:
                    if hora in df.columns:
                        valor_en_kwh = df[hora].sum()  # Sumar todos los días para esta hora
                        gwh_asignados_por_hora[hora-1] += valor_en_kwh
    
    # PASO 2: CALCULAR GWh NO ASIGNADOS por hora  
    # (Equivale a la fórmula del cliente: SUMAR.SI.CONJUNTO donde TIPO="ENA")
    print("   📊 Calculando energía NO ASIGNADA por hora...")
    if "DEMANDA_FALTANTE" in resultados_dict:
        df_faltante = resultados_dict["DEMANDA_FALTANTE"]
        print(f"      Procesando demanda faltante: {len(df_faltante)} filas")
        
        # Para cada hora (columnas 1-24)
        for hora in horas:
            if hora in df_faltante.columns:
                valor_en_kwh = df_faltante[hora].sum()  # Sumar todos los días para esta hora
                gwh_no_asignados_por_hora[hora-1] += valor_en_kwh
    
    # PASO 3: CONVERTIR DE kWh A GWh y CALCULAR PORCENTAJES
    # (Como en la fórmula del cliente: dividir por 1,000,000)
    print("   📊 Convirtiendo a GWh y calculando porcentajes...")
    for hora in range(24):
        # Convertir a GWh
        gwh_asignados_por_hora[hora] = gwh_asignados_por_hora[hora] / 1_000_000
        gwh_no_asignados_por_hora[hora] = gwh_no_asignados_por_hora[hora] / 1_000_000
        
        # Calcular % no asignado (como en la fórmula del cliente)
        total_hora = gwh_asignados_por_hora[hora] + gwh_no_asignados_por_hora[hora]
        if total_hora > 0:
            porcentaje_no_asignado_por_hora[hora] = (gwh_no_asignados_por_hora[hora] / total_hora) * 100
        else:
            porcentaje_no_asignado_por_hora[hora] = 0
    
    # Mostrar resumen en consola
    total_asignado = sum(gwh_asignados_por_hora)
    total_no_asignado = sum(gwh_no_asignados_por_hora)
    print(f"   ✅ Total GWh asignados: {total_asignado:.2f}")
    print(f"   ✅ Total GWh no asignados: {total_no_asignado:.2f}")
    print(f"   ✅ Porcentaje promedio no asignado: {sum(porcentaje_no_asignado_por_hora)/24:.1f}%")
    
    # PASO 4: CREAR LA GRÁFICA (exactamente como en la imagen del cliente)
    fig = go.Figure()
    
    # BARRAS AZULES: GWh Asignados
    fig.add_trace(
        go.Bar(
            x=horas,
            y=gwh_asignados_por_hora,
            name='GWh Asignados',
            marker_color='#1f4e79',  # Azul oscuro exacto de la imagen
            hovertemplate='Hora %{x}<br>GWh Asignados: %{y:.3f}<extra></extra>',
            text=[f'{val:.3f}' for val in gwh_asignados_por_hora],
            textposition='inside',
            textfont=dict(color='white', size=9)
        )
    )
    
    # BARRAS GRISES: GWh No Asignado
    fig.add_trace(
        go.Bar(
            x=horas,
            y=gwh_no_asignados_por_hora,
            name='GWh No Asignado',
            marker_color='#d9d9d9',  # Gris claro exacto de la imagen
            hovertemplate='Hora %{x}<br>GWh No Asignado: %{y:.3f}<extra></extra>',
            text=[f'{val:.3f}' if val > 0 else '' for val in gwh_no_asignados_por_hora],
            textposition='inside',
            textfont=dict(color='black', size=9)
        )
    )
    
    # LÍNEA VERDE: % No Asignado (eje derecho)
    fig.add_trace(
        go.Scatter(
            x=horas,
            y=porcentaje_no_asignado_por_hora,
            name='% No Asignado',
            yaxis='y2',  # Usar el eje Y derecho
            line=dict(color='#70ad47', width=3),  # Verde exacto de la imagen
            mode='lines+markers+text',
            marker=dict(size=8, symbol='circle', color='#70ad47'),
            text=[f'{p:.0f}%' for p in porcentaje_no_asignado_por_hora],
            textposition='top center',
            textfont=dict(color='#70ad47', size=10),
            hovertemplate='Hora %{x}<br>% No Asignado: %{y:.1f}%<extra></extra>'
        )
    )
    
    # CONFIGURAR EL DISEÑO
    fig.update_layout(
        # Título principal
        title={
            'text': '<b>ENERGÍA ASIGNADA Y NO ASIGNADA</b>',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16, 'color': '#333'}
        },
        
        # Configuración general
        barmode='stack',  # Barras apiladas como en la imagen
        height=500,
        width=1000,
        font=dict(family="Arial, sans-serif", size=11),
        plot_bgcolor='white',
        paper_bgcolor='white',
        
        # EJE X (horizontal): Horas
        xaxis=dict(
            title='HORAS',
            titlefont=dict(size=12, color='black'),
            tickfont=dict(size=10, color='black'),
            tickvals=horas,  # Mostrar todas las horas 1-24
            gridcolor='lightgray',
            gridwidth=1,
            showgrid=True
        ),
        
        # EJE Y IZQUIERDO: GWh
        yaxis=dict(
            title='GWh',
            titlefont=dict(size=12, color='black'),
            tickfont=dict(size=10, color='black'),
            gridcolor='lightgray',
            gridwidth=1,
            showgrid=True,
            side='left'
        ),
        
        # EJE Y DERECHO: Porcentaje (para la línea verde)
        yaxis2=dict(
            title='% No Asignado',
            titlefont=dict(size=12, color='#70ad47'),
            tickfont=dict(size=10, color='#70ad47'),
            overlaying='y',  # Superponer sobre el eje Y izquierdo
            side='right',
            range=[0, max(porcentaje_no_asignado_por_hora) * 1.2 if max(porcentaje_no_asignado_por_hora) > 0 else 5],
            tickformat='.0f',
            ticksuffix='%',
            showgrid=False  # No mostrar grilla para este eje
        ),
        
        # LEYENDA (horizontal, arriba)
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            font=dict(size=11)
        ),
        
        # MÁRGENES
        margin=dict(l=60, r=60, t=80, b=60)
    )
    
    print("✅ Gráfica de barras por hora creada exitosamente")
    return fig

# ALIAS para mantener compatibilidad con el código existente
crear_grafica_distribucion_porcentual = crear_grafica_torta_energia
crear_grafica_resumen_adjudicacion = crear_grafica_barras_por_hora