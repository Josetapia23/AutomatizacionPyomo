# Sistema de Optimización Energética con Pyomo

Este proyecto implementa un sistema de optimización para asignar ofertas energéticas a demandas específicas, utilizando el framework de optimización Pyomo.

## Características

- Procesamiento de ofertas energéticas
- Proyección de indexadores
- Evaluación de ofertas según criterios específicos
- Optimización de asignación de ofertas a demanda
- Exportación de resultados a formatos Excel

## Estructura del Proyecto

```
energia_pyomo/               # Directorio raíz del paquete
├── __init__.py             # Inicializa el paquete
├── config.py               # Configuración y rutas del proyecto
├── main.py                 # Punto de entrada principal
├── core/                   # Funcionalidades principales
│   ├── __init__.py
│   ├── ofertas.py          # Procesamiento de ofertas
│   ├── indexadores.py      # Manejo de indexadores
│   ├── evaluacion.py       # Evaluación de ofertas
│   └── utils.py            # Funciones auxiliares
├── optimizacion/           # Funcionalidades de optimización con Pyomo
│   ├── __init__.py
│   ├── modelo.py           # Definición del modelo Pyomo
│   ├── restricciones.py    # Restricciones del modelo
│   └── solver.py           # Configuración y ejecución del solver
```

## Requisitos

- Python 3.8+
- Pyomo
- CBC Solver (instalado separadamente)
- Pandas
- NumPy
- Matplotlib
- Openpyxl

## Instalación

1. Clone este repositorio:
   ```bash
   git clone https://github.com/tuusuario/energia-pyomo.git
   cd energia-pyomo
   ```

2. Instale las dependencias:
   ```bash
   pip install -r requirements.txt
   ```

3. Instale el solver CBC:
   - Windows: Descargue CBC desde https://www.coin-or.org/download/binary/Cbc/ y colóquelo en la carpeta CBC
   - Mac OS: `brew install cbc`
   - Linux: `apt-get install coinor-cbc`

## Uso

### Modo Interactivo

```bash
python main.py
```

Esto mostrará un menú con las siguientes opciones:
1. Ejecutar flujo completo
2. Crear/actualizar proyección de indexadores
3. Procesar ofertas
4. Optimizar asignación de ofertas
5. Ver configuración actual
0. Salir

### Modo Automático

```bash
python main.py --automatico
```

Esto ejecutará el flujo completo sin mostrar el menú interactivo.

## Estructura de Datos

### Carpetas y Archivos

- `data/datos_iniciales.xlsx`: Contiene los datos iniciales como demanda, indexadores, etc.
- `OFERTAS/`: Contiene los archivos Excel de ofertas a procesar
- `output/`: Almacena los resultados generados

### Hojas Excel

Cada archivo de oferta debe contener tres hojas:
1. `cantidad`: Datos de cantidades por hora y fecha
2. `precios`: Datos de precios por hora y fecha
3. `indexador`: Parámetros del indexador

## Modelo de Optimización

El modelo utiliza Pyomo para formular y resolver un problema de optimización lineal que minimiza el costo total de la energía asignada, sujeto a:
- Restricciones de capacidad de cada oferta
- Restricciones de balance de demanda
- Restricciones de evaluación de ofertas

## Licencia

Este proyecto está licenciado bajo los términos de la licencia MIT.
