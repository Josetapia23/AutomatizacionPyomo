# -*- mode: python ; coding: utf-8 -*-
"""
Archivo de especificación para PyInstaller
Sistema de Optimización Energética - VERSIÓN COMPLETA

Uso:
    pyinstaller optimizacion_energia.spec
"""

import sys
from pathlib import Path

block_cipher = None

# Directorio base del proyecto
BASE_DIR = Path.cwd()

# Encontrar el directorio de site-packages (compatible con venv)
if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
    # Estamos en un virtualenv/venv
    SITE_PACKAGES = Path(sys.prefix) / 'Lib' / 'site-packages'
else:
    # Instalación global de Python
    SITE_PACKAGES = Path(sys.executable).parent / 'Lib' / 'site-packages'

print(f"📍 Site-packages encontrado en: {SITE_PACKAGES}")
print(f"📍 Verificando pyomo en: {SITE_PACKAGES / 'pyomo'}")

# Verificar que pyomo existe
if not (SITE_PACKAGES / 'pyomo').exists():
    print("❌ ERROR: No se encuentra pyomo en site-packages")
    print(f"   Buscado en: {SITE_PACKAGES / 'pyomo'}")
    sys.exit(1)
else:
    print("✅ Pyomo encontrado correctamente")

# ==================== ARCHIVOS Y CARPETAS A INCLUIR ====================

# 1. Archivos de datos adicionales (datas)
datas = [
    # Incluir CBC solver (CRÍTICO)
    (str(BASE_DIR / 'CBC' / 'cbc.exe'), 'CBC'),
    
    # Incluir módulos del proyecto
    (str(BASE_DIR / 'core'), 'core'),
    (str(BASE_DIR / 'optimizacion'), 'optimizacion'),
    
    # Incluir archivos de configuración
    (str(BASE_DIR / 'config.py'), '.'),
    
    # Incluir plantillas Excel
    (str(BASE_DIR / 'plantillas'), 'plantillas'),
    
    # Incluir recursos (íconos)
    (str(BASE_DIR / 'recursos'), 'recursos'),
    
    # INCLUIR TODO PYOMO COMO DATOS (solución definitiva)
    (str(SITE_PACKAGES / 'pyomo'), 'pyomo'),
    
    # Incluir README si existe
    (str(BASE_DIR / 'README.md'), '.') if (BASE_DIR / 'README.md').exists() else None,
]

# Filtrar None values
datas = [d for d in datas if d is not None]

# 2. Módulos ocultos - ULTRA COMPLETO
hiddenimports = [
    # PyQt5
    'PyQt5',
    'PyQt5.QtCore',
    'PyQt5.QtGui',
    'PyQt5.QtWidgets',
    
    # Pyomo - ABSOLUTAMENTE TODO
    'pyomo',
    'pyomo.environ',
    'pyomo.opt',
    'pyomo.opt.base',
    'pyomo.opt.plugins',
    'pyomo.opt.plugins.sol',
    'pyomo.opt.plugins.sol.sol',
    'pyomo.opt.solver',
    'pyomo.opt.results',
    'pyomo.core',
    'pyomo.core.base',
    'pyomo.core.base.plugin',
    'pyomo.core.expr',
    'pyomo.core.expr.numvalue',
    'pyomo.core.expr.numeric_expr',
    'pyomo.core.kernel',
    'pyomo.common',
    'pyomo.common.plugin',
    'pyomo.common.plugins',
    'pyomo.common.collections',
    'pyomo.common.dependencies',
    'pyomo.common.fileutils',
    'pyomo.solvers',
    'pyomo.solvers.plugins',
    'pyomo.solvers.plugins.solvers',
    'pyomo.solvers.plugins.solvers.CBCplugin',
    'pyomo.repn',
    'pyomo.repn.plugins',
    'pyomo.repn.plugins.nl_writer',
    'pyomo.dataportal',
    'pyomo.dataportal.plugins',
    'pyomo.dataportal.plugins.sheet',
    'pyomo.dataportal.plugins.json_dict',
    'pyomo.dataportal.plugins.db_table',
    'pyomo.util',
    
    # Pandas y NumPy
    'pandas',
    'pandas._libs',
    'pandas._libs.tslibs',
    'pandas._libs.tslibs.timedeltas',
    'pandas._libs.tslibs.np_datetime',
    'pandas._libs.tslibs.nattype',
    'pandas._libs.tslibs.timestamps',
    'numpy',
    'numpy.core',
    'numpy.core._multiarray_umath',
    'numpy.random',
    'numpy.random._common',
    'numpy.random._bounded_integers',
    'numpy.random._mt19937',
    'numpy.random._philox',
    'numpy.random._pcg64',
    'numpy.random._sfc64',
    'numpy.random._generator',
    
    # Excel
    'openpyxl',
    'openpyxl.cell',
    'openpyxl.cell._writer',
    'openpyxl.styles',
    'openpyxl.worksheet',
    'xlsxwriter',
    
    # Visualizaciones
    'matplotlib',
    'matplotlib.pyplot',
    'matplotlib.backends',
    'matplotlib.backends.backend_agg',
    'plotly',
    'plotly.graph_objs',
    'plotly.graph_objects',
    'kaleido',
    
    # Utilidades
    'pathlib',
    'datetime',
    'logging',
    'platform',
    'collections',
    'collections.abc',
    'importlib',
    'importlib.metadata',
    'pkg_resources',
]

# ==================== ANÁLISIS ====================
a = Analysis(
    ['gui_pyqt.py'],  # Archivo principal de entrada
    pathex=[str(BASE_DIR)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tcl',
        'tk',
        '_tkinter',
        'tkinter',
        'Tkinter',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# ==================== PYZ (Archivo Python comprimido) ====================
pyz = PYZ(
    a.pure, 
    a.zipped_data,
    cipher=block_cipher
)

# ==================== EXE (Ejecutable) ====================
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='OptimizacionEnergia',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,  # CON consola para permitir input()
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=str(BASE_DIR / 'recursos' / 'excel_icon.png') if (BASE_DIR / 'recursos' / 'excel_icon.png').exists() else None,
)

# ==================== COLLECT (Recolectar todo) ====================
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='OptimizacionEnergia'
)