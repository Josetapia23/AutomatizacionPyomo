"""
Funciones avanzadas para la generación de visualizaciones del sistema de optimización energética.
Incluye mapas de calor, distribuciones por agente y análisis temporales detallados.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import logging
from datetime import datetime
from pathlib import Path

from .utils import (
    format_number, convert_to_gwh, extract_dates_from_results,
    extract_offers_from_results, format_date_for_display
)

logger = logging.getLogger(__name__)

def crear_mapa_calor_mensual(resultados_dict):
    """
    Crea un mapa de calor mensual de demanda faltante.
    Replica la lógica de Excel: SUMAR.SI.CONJUNTO por año y mes.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados de la optimización
        
    Returns:
        plotly.graph_objects.Figure: Figura con el mapa de calor mensual
    """
    logger.info("Creando mapa de calor mensual de demanda faltante")
    
    try:
        # Buscar la hoja de demanda faltante
        if "DEMANDA_FALTANTE" not in resultados_dict:
            logger.warning("No se encontró DEMANDA_FALTANTE en los resultados")
            return None
        
        demanda_faltante_df = resultados_dict["DEMANDA_FALTANTE"]
        
        if demanda_faltante_df.empty:
            logger.warning("La hoja DEMANDA_FALTANTE está vacía")
            return None
        
        # Crear DataFrame expandido para replicar estructura Excel
        data_expandida = []
        
        for _, row in demanda_faltante_df.iterrows():
            fecha = row['FECHA']
            
            # Extraer año y mes de la fecha
            if hasattr(fecha, 'year') and hasattr(fecha, 'month'):
                año = fecha.year
                mes = fecha.month
            else:
                # Si la fecha viene como string, convertirla
                try:
                    fecha_dt = pd.to_datetime(fecha)
                    año = fecha_dt.year
                    mes = fecha_dt.month
                except:
                    logger.warning(f"No se pudo procesar la fecha: {fecha}")
                    continue
            
            # Para cada hora (columnas 1-24), crear un registro
            for hora in range(1, 25):
                if hora in row and pd.notna(row[hora]):
                    valor_kwh = float(row[hora])
                    valor_gwh = convert_to_gwh(valor_kwh)
                    
                    data_expandida.append({
                        'FECHA': fecha,
                        'AÑO': año,
                        'MES': mes,
                        'HORA': hora,
                        'DEMANDA_FALTANTE_GWh': valor_gwh
                    })
        
        if not data_expandida:
            logger.warning("No se encontraron datos válidos para el mapa de calor")
            return None
        
        # Convertir a DataFrame
        df_expandido = pd.DataFrame(data_expandida)
        
        # Obtener años únicos ordenados
        años_unicos = sorted(df_expandido['AÑO'].unique())
        meses = list(range(1, 13))  # 1-12
        
        # Crear matriz para el heatmap (12 meses x N años)
        # Sumar por mes y año (replicando la fórmula Excel)
        matriz_calor = []
        
        for mes in meses:
            fila_mes = []
            for año in años_unicos:
                # Filtrar datos para este mes y año específicos
                datos_filtrados = df_expandido[
                    (df_expandido['MES'] == mes) & 
                    (df_expandido['AÑO'] == año)
                ]
                
                # Sumar todos los valores para este mes/año
                total_mes_año = datos_filtrados['DEMANDA_FALTANTE_GWh'].sum()
                fila_mes.append(total_mes_año)
            
            matriz_calor.append(fila_mes)
        
        # Convertir a numpy array para facilitar manipulación
        matriz_calor = np.array(matriz_calor)
        
        # Nombres de meses
        nombres_meses = [
            'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
            'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
        ]
        
        # Crear etiquetas para los años
        etiquetas_años = [str(año) for año in años_unicos]
        
        # Crear el mapa de calor
        fig = go.Figure(data=go.Heatmap(
            z=matriz_calor,
            x=etiquetas_años,
            y=nombres_meses,
            colorscale='RdYlBu_r',  # Rojo para valores altos, azul para bajos
            hoverongaps=False,
            text=matriz_calor,
            texttemplate="%{text:.2f}",
            textfont={"size": 10},
            colorbar=dict(
                title="Demanda Faltante (GWh)",
                tickmode="auto",
                thickness=20,
                len=0.8,
                x=1.02,  # Mover más a la derecha
                tickfont=dict(size=10)  # Reducir tamaño de fuente
            )
        ))
        
        # Configurar layout
        fig.update_layout(
            title={
                'text': "MAPA DE CALOR: DEMANDA FALTANTE MENSUAL",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'color': '#1f4e79', 'family': 'Arial Black'}
            },
            xaxis_title="AÑO",
            yaxis_title="MES",
            width=900,  # Aumentar ancho para dar espacio al colorbar
            height=600,
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(size=12),
            margin=dict(l=100, r=150, t=80, b=100)  # Más margen derecho
        )
        
        # Configurar ejes
        fig.update_xaxes(
            tickfont=dict(size=12),
            title_font=dict(size=14)
        )
        
        fig.update_yaxes(
            tickfont=dict(size=12),
            title_font=dict(size=14)
        )
        
        # Agregar anotación explicativa (movida para no tapar)
        fig.add_annotation(
            x=0.02, y=-0.08, 
            xref="paper", yref="paper",
            text="<b>Valores en GWh</b><br>Rojo = Mayor demanda faltante<br>Azul = Menor demanda faltante",
            showarrow=False,
            font=dict(size=10, color="#666666"),
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="#cccccc",
            borderwidth=1,
            align="left"
        )
        
        logger.info("Mapa de calor mensual creado exitosamente")
        print(f"📊 Mapa de calor generado: {len(años_unicos)} años × 12 meses")
        
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear mapa de calor mensual: {e}")
        print(f"❌ Error en mapa de calor mensual: {e}")
        return None

def crear_tabla_energia_faltante_horaria(resultados_dict):
    """
    Crea tabla de ENERGÍA FALTANTE HORARIA MENSUAL POR AÑO en GWh.
    Similar a la tabla mostrada en Excel con años, meses (1-12) y horas (1-24).
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Figura con tabla de energía faltante horaria
    """
    logger.info("Creando tabla de energía faltante horaria mensual por año")
    
    try:
        # Buscar la hoja de demanda faltante
        if "DEMANDA_FALTANTE" not in resultados_dict:
            logger.warning("No se encontró DEMANDA_FALTANTE en los resultados")
            return None
        
        demanda_faltante_df = resultados_dict["DEMANDA_FALTANTE"]
        
        if demanda_faltante_df.empty:
            logger.warning("La hoja DEMANDA_FALTANTE está vacía")
            return None
        
        # DEBUG: Imprimir información sobre los datos
        print(f"DEBUG - Shape de DEMANDA_FALTANTE: {demanda_faltante_df.shape}")
        print(f"DEBUG - Columnas: {demanda_faltante_df.columns.tolist()}")
        print("DEBUG - Primeras 3 filas:")
        print(demanda_faltante_df.head(3))
        
        # Verificar que tenemos las columnas de horas
        columnas_horas = [col for col in demanda_faltante_df.columns if isinstance(col, int) and 1 <= col <= 24]
        print(f"DEBUG - Columnas de horas encontradas: {len(columnas_horas)}")
        
        # Crear estructura para almacenar datos por año/mes/hora
        datos_por_ano = {}
        total_registros = 0
        valores_encontrados = 0
        
        # Procesar cada fila de demanda faltante
        for idx, row in demanda_faltante_df.iterrows():
            fecha = row['FECHA']
            
            # Extraer año y mes
            if hasattr(fecha, 'year') and hasattr(fecha, 'month'):
                año = fecha.year
                mes = fecha.month
            else:
                try:
                    fecha_dt = pd.to_datetime(fecha)
                    año = fecha_dt.year
                    mes = fecha_dt.month
                except:
                    logger.warning(f"No se pudo procesar la fecha: {fecha}")
                    continue
            
            # Inicializar estructura si no existe
            if año not in datos_por_ano:
                datos_por_ano[año] = {}
                for m in range(1, 13):  # 12 meses
                    datos_por_ano[año][m] = {}
                    for h in range(1, 25):  # 24 horas
                        datos_por_ano[año][m][h] = 0.0
            
            # Sumar valores por hora
            for hora in range(1, 25):
                if hora in row:
                    valor = row[hora]
                    if pd.notna(valor) and valor != 0:
                        valor_kwh = float(valor)
                        valor_gwh = valor_kwh / 1_000_000  # Convertir a GWh
                        datos_por_ano[año][mes][hora] += valor_gwh
                        valores_encontrados += 1
                        if valores_encontrados <= 10:  # Mostrar primeros 10 valores
                            print(f"DEBUG - Valor encontrado: Año {año}, Mes {mes}, Hora {hora} = {valor_gwh:.4f} GWh")
            
            total_registros += 1
        
        print(f"DEBUG - Total registros procesados: {total_registros}")
        print(f"DEBUG - Total valores encontrados (no cero): {valores_encontrados}")
        print(f"DEBUG - Años encontrados: {list(datos_por_ano.keys())}")
        
        if not datos_por_ano:
            logger.warning("No se encontraron datos para procesar")
            return None
        
        # Crear figura con subplots para cada año
        años_ordenados = sorted(datos_por_ano.keys())
        num_años = len(años_ordenados)
        
        # Crear subplots (una fila por año)
        fig = make_subplots(
            rows=num_años, 
            cols=1,
            subplot_titles=[f"AÑO {año}" for año in años_ordenados],
            vertical_spacing=0.1,
            row_heights=[1.0/num_años] * num_años
        )
        
        # Para almacenar valores mínimos y máximos
        valor_min = float('inf')
        valor_max = 0
        
        # Procesar cada año
        for idx, año in enumerate(años_ordenados, 1):
            # Crear matriz para el heatmap (12 meses x 24 horas)
            matriz_año = []
            
            for mes in range(1, 13):
                fila_mes = []
                for hora in range(1, 25):
                    valor = datos_por_ano[año][mes][hora]
                    fila_mes.append(valor)
                    if valor > 0:
                        valor_min = min(valor_min, valor)
                        valor_max = max(valor_max, valor)
                matriz_año.append(fila_mes)
            
            # DEBUG: Verificar si hay datos en la matriz
            suma_año = sum(sum(fila) for fila in matriz_año)
            print(f"DEBUG - Año {año}: Suma total = {suma_año:.2f} GWh")
            
            # Crear heatmap para este año
            heatmap = go.Heatmap(
                z=matriz_año,
                x=[str(h) for h in range(1, 25)],  # Horas 1-24
                y=[str(m) for m in range(1, 13)],  # Meses 1-12
                colorscale=[
                    [0, 'rgb(255,255,255)'],      # Blanco para 0
                    [0.2, 'rgb(200,200,255)'],    # Azul muy claro
                    [0.4, 'rgb(0,0,255)'],        # Azul
                    [0.6, 'rgb(255,255,0)'],      # Amarillo
                    [0.8, 'rgb(255,128,0)'],      # Naranja
                    [1, 'rgb(255,0,0)']           # Rojo
                ],
                zmin=0,
                zmax=valor_max if valor_max > 0 else 1,
                colorbar=dict(
                    title=dict(text="GWh", side="right"),  # Corregido: usar dict para title
                    tickmode="linear",
                    x=1.02,
                    len=0.9/num_años,
                    y=1 - (idx-0.5)/num_años,
                    yanchor="middle"
                ) if idx == 1 else None,  # Solo mostrar colorbar en el primer año
                text=[[f"{val:.2f}" if val > 0 else "0" for val in row] for row in matriz_año],
                texttemplate="%{text}",
                textfont=dict(size=8),
                showscale=(idx == 1),  # Solo mostrar escala en el primero
                hoverongaps=False,
                hovertemplate="Año: " + str(año) + "<br>" +
                             "Mes: %{y}<br>" +
                             "Hora: %{x}<br>" +
                             "Energía Faltante: %{z:.2f} GWh<br>" +
                             "<extra></extra>"
            )
            
            fig.add_trace(heatmap, row=idx, col=1)
            
            # Actualizar ejes para este subplot
            fig.update_xaxes(
                title_text="HORA" if idx == num_años else "",
                tickmode="linear",
                tick0=1,
                dtick=1,
                showgrid=True,
                gridcolor='rgba(128,128,128,0.2)',
                row=idx, col=1
            )
            
            fig.update_yaxes(
                title_text="MES",
                tickmode="linear", 
                tick0=1,
                dtick=1,
                showgrid=True,
                gridcolor='rgba(128,128,128,0.2)',
                row=idx, col=1
            )
        
        # Configurar layout general
        fig.update_layout(
            title={
                'text': "ENERGÍA FALTANTE HORARIA MENSUAL POR AÑO (GWh)",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'color': '#1f4e79', 'family': 'Arial Black'}
            },
            width=1400,
            height=350 * num_años + 150,
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(size=10),
            margin=dict(l=80, r=150, t=100, b=80)
        )
        
        # Calcular y agregar estadísticas
        total_general = sum(
            datos_por_ano[año][mes][hora]
            for año in datos_por_ano
            for mes in datos_por_ano[año]
            for hora in datos_por_ano[año][mes]
        )
        
        # Encontrar el mes y hora con mayor faltante
        max_valor = 0
        max_info = ""
        for año in datos_por_ano:
            for mes in datos_por_ano[año]:
                for hora in datos_por_ano[año][mes]:
                    if datos_por_ano[año][mes][hora] > max_valor:
                        max_valor = datos_por_ano[año][mes][hora]
                        max_info = f"Año {año}, Mes {mes}, Hora {hora}"
        
        fig.add_annotation(
            x=0.02, y=-0.02,
            xref="paper", yref="paper",
            text=f"<b>Total Energía Faltante: {total_general:.2f} GWh</b><br>" +
                 f"<b>Máximo faltante: {max_valor:.2f} GWh ({max_info})</b>",
            showarrow=False,
            font=dict(size=12, color="#666666"),
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="#cccccc",
            borderwidth=1,
            align="left"
        )
        
        logger.info("Tabla de energía faltante horaria creada exitosamente")
        print(f"📊 Tabla de energía faltante generada: {num_años} años × 12 meses × 24 horas")
        print(f"📊 Total energía faltante: {total_general:.2f} GWh")
        
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear tabla de energía faltante horaria: {e}")
        print(f"❌ Error en tabla de energía faltante horaria: {e}")
        import traceback
        traceback.print_exc()
        return None
    
def crear_distribucion_por_agente(resultados_dict):
    """
    Crea gráfica de distribución por agente (EPM, AES, ISAGEN) separando DA/ENA.
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Figura con distribución por agente
    """
    logger.info("Creando gráfica de distribución por agente")
    
    try:
        # Extraer todas las fechas únicas y normalizarlas
        todas_fechas = set()
        for clave, df in resultados_dict.items():
            if isinstance(df, pd.DataFrame) and not df.empty and "FECHA" in df.columns:
                for fecha in df["FECHA"].unique():
                    # Normalizar fecha
                    if hasattr(fecha, 'date'):
                        fecha_normalizada = fecha.date()
                    elif isinstance(fecha, str):
                        fecha_normalizada = pd.to_datetime(fecha).date()
                    else:
                        fecha_normalizada = fecha
                    todas_fechas.add(fecha_normalizada)
        
        todas_fechas = sorted(list(todas_fechas))
        
        if not todas_fechas:
            logger.warning("No se encontraron fechas para procesar")
            return None
        
        # Inicializar diccionario para almacenar datos por agente
        datos_agentes = {}
        
        # Procesar cada hoja de resultados
        for clave, df in resultados_dict.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            
            # Identificar si es DA (COMPRAR) o ENA (NO_COMPRADA)
            if "DEMANDA ASIGNADA" in clave:
                # Extraer nombre del agente de la clave
                try:
                    # Formato: "DEMANDA ASIGNADA {agente} IT{num}_{tipo}"
                    partes = clave.split("DEMANDA ASIGNADA ")[1]
                    agente = partes.split(" IT")[0]
                    
                    if "_COMPRAR" in clave:
                        tipo = "DA"
                    elif "_NO_COMPRADA" in clave:
                        tipo = "ENA"
                    else:
                        continue
                    
                    clave_agente = f"{agente}_{tipo}"
                    
                    if clave_agente not in datos_agentes:
                        datos_agentes[clave_agente] = {fecha: 0.0 for fecha in todas_fechas}
                    
                    # Sumar valores por fecha
                    for _, row in df.iterrows():
                        fecha = row['FECHA']
                        
                        # Normalizar fecha para comparación consistente
                        if hasattr(fecha, 'date'):
                            fecha_normalizada = fecha.date()
                        elif isinstance(fecha, str):
                            fecha_normalizada = pd.to_datetime(fecha).date()
                        else:
                            fecha_normalizada = fecha
                        
                        # Buscar fecha equivalente en todas_fechas
                        fecha_encontrada = None
                        for fecha_ref in todas_fechas:
                            if hasattr(fecha_ref, 'date'):
                                fecha_ref_normalizada = fecha_ref.date()
                            elif isinstance(fecha_ref, str):
                                fecha_ref_normalizada = pd.to_datetime(fecha_ref).date()
                            else:
                                fecha_ref_normalizada = fecha_ref
                            
                            if fecha_normalizada == fecha_ref_normalizada:
                                fecha_encontrada = fecha_ref
                                break
                        
                        if fecha_encontrada:
                            # Sumar todas las horas (columnas 1-24)
                            total_fecha = 0
                            for hora in range(1, 25):
                                if hora in row and pd.notna(row[hora]):
                                    total_fecha += float(row[hora])
                            
                            # Convertir a GWh y acumular
                            datos_agentes[clave_agente][fecha_encontrada] += convert_to_gwh(total_fecha)
                
                except Exception as e:
                    logger.warning(f"Error procesando clave {clave}: {e}")
                    continue
        
        if not datos_agentes:
            logger.warning("No se encontraron datos de agentes")
            return None
        
        # Crear la gráfica
        fig = go.Figure()
        
        # Definir colores por agente y tipo
        colores = {
            'EPM_DA': '#4472C4',      # Azul
            'EPM_ENA': '#FFA500',     # Naranja
            'AES_DA': '#70AD47',      # Verde
            'AES_ENA': '#C65911',     # Marrón/Naranja oscuro
            'ISAGEN_DA': '#7030A0',   # Morado
            'ISAGEN_ENA': '#A0522D'   # Marrón
        }
        
        # Agregar trazas para cada agente-tipo
        for agente_tipo, datos in datos_agentes.items():
            fechas_ordenadas = sorted(datos.keys())
            valores = [datos[fecha] for fecha in fechas_ordenadas]
            
            # Determinar color
            color = colores.get(agente_tipo, '#666666')
            
            # Formatear nombre para la leyenda
            if '_DA' in agente_tipo:
                nombre_leyenda = f"({agente_tipo.replace('_DA', '')}, DA)"
            elif '_ENA' in agente_tipo:
                nombre_leyenda = f"({agente_tipo.replace('_ENA', '')}, ENA)"
            else:
                nombre_leyenda = agente_tipo
            
            # Asegurar que las fechas sean consistentes para el ordenamiento
            fechas_consistentes = []
            valores_consistentes = []
            
            for fecha in fechas_ordenadas:
                # Convertir fecha a string consistente
                if hasattr(fecha, 'strftime'):
                    fecha_str = fecha.strftime('%d/%m/%Y')
                else:
                    fecha_str = str(fecha)
                
                fechas_consistentes.append(fecha_str)
                valores_consistentes.append(datos[fecha])
            
            fig.add_trace(go.Bar(
                x=fechas_consistentes,
                y=valores_consistentes,
                name=nombre_leyenda,
                marker_color=color,
                opacity=0.8
            ))
        
        # Configurar layout
        fig.update_layout(
            title={
                'text': "DISTRIBUCIÓN DE GWh POR TIPO DE ASIGNACIÓN (DA/ENA) POR AGENTE",
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16, 'color': '#1f4e79'}
            },
            xaxis_title="Fecha",
            yaxis_title="GWh",
            barmode='group',  # Barras agrupadas
            width=1200,
            height=600,
            plot_bgcolor='white',
            paper_bgcolor='white',
            legend=dict(
                title="Agente - Tipo Asignación",
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02,
                font=dict(size=10)
            ),
            margin=dict(l=60, r=150, t=80, b=100)
        )
        
        # Configurar ejes
        fig.update_xaxes(
            tickangle=45,
            tickfont=dict(size=10)
        )
        
        fig.update_yaxes(
            showgrid=True,
            gridcolor="lightgray",
            tickfont=dict(size=12)
        )
        
        logger.info("Gráfica de distribución por agente creada exitosamente")
        return fig
        
    except Exception as e:
        logger.error(f"Error al crear distribución por agente: {e}")
        print(f"❌ Error en distribución por agente: {e}")
        return None

def crear_tabla_valores_horarios(resultados_dict):
    """
    Crea tabla de valores horarios por año (similar a la imagen del Excel).
    
    Args:
        resultados_dict (dict): Diccionario con los resultados
        
    Returns:
        plotly.graph_objects.Figure: Figura con tabla de valores horarios
    """
    logger.info("Creando tabla de valores horarios por año")
    
    try:
        # Implementación pendiente - se puede agregar después del mapa de calor
        logger.info("Tabla de valores horarios - pendiente de implementación")
        return None
        
    except Exception as e:
        logger.error(f"Error al crear tabla de valores horarios: {e}")
        return None