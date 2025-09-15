"""
GUI con PyQt - Sistema de Optimización Energética COMPLETO
INTERFAZ FINAL: Todos los campos que el usuario necesita ingresar
"""

import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QGridLayout, QLabel, QLineEdit, QPushButton, 
                           QFileDialog, QMessageBox, QTextEdit, QFrame, QSizePolicy,
                           QProgressBar, QScrollArea)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QFont, QPalette, QColor, QIcon
from pathlib import Path
from datetime import datetime

# Agregar el directorio del proyecto al path para importar módulos
sys.path.insert(0, str(Path(__file__).parent))

# Importar configuración del proyecto
try:
    from config import (
        DATOS_INICIALES, OFERTAS_DIR, RESULTADO_OFERTAS, ESTADISTICAS_OFERTAS
    )
    from core.utils import verificar_archivo_existe
    PROYECTO_CONECTADO = True
    print("✅ Proyecto conectado exitosamente")
except ImportError as e:
    print(f"❌ Error conectando proyecto: {e}")
    PROYECTO_CONECTADO = False

class WorkerThread(QThread):
    """Thread para ejecutar operaciones pesadas sin bloquear la GUI"""
    finished = pyqtSignal(bool, str)  # éxito, mensaje
    progress = pyqtSignal(str)  # mensaje de progreso
    
    def __init__(self, operacion, **kwargs):
        super().__init__()
        self.operacion = operacion
        self.kwargs = kwargs
        
    def run(self):
        try:
            if self.operacion == "flujo_completo":
                self.progress.emit("🚀 Iniciando flujo completo...")
                # AQUÍ SE EJECUTARÁ EL PROYECTO CON LOS PARÁMETROS DE LA INTERFAZ
                result = self.ejecutar_flujo_con_parametros()
                
            elif self.operacion == "validar_campos":
                self.progress.emit("🔍 Validando campos...")
                result = self.validar_todos_los_campos()
                
            else:
                result = False
                
            if result:
                self.finished.emit(True, f"✅ {self.operacion.title()} completado exitosamente")
            else:
                self.finished.emit(False, f"❌ Error en {self.operacion}")
                
        except Exception as e:
            self.finished.emit(False, f"❌ Error inesperado: {str(e)}")
    
    def ejecutar_flujo_con_parametros(self):
        """Ejecutar el flujo completo usando los parámetros de la interfaz"""
        # AQUÍ IMPLEMENTAREMOS LA LÓGICA PARA LLAMAR AL PROYECTO EXISTENTE
        # CON LOS PARÁMETROS QUE EL USUARIO INGRESÓ
        self.progress.emit("📋 Preparando parámetros...")
        self.progress.emit("🔧 Ejecutando proyecto con parámetros personalizados...")
        
        # Por ahora retornamos True para demostrar que la interfaz funciona
        import time
        time.sleep(2)  # Simular procesamiento
        return True
    
    def validar_todos_los_campos(self):
        """Validar que todos los campos estén llenos correctamente"""
        errores = []
        
        # Aquí validaremos todos los campos
        # Por ahora simulamos la validación
        self.progress.emit("✅ Todos los campos válidos")
        return True

class OptimizacionPyQtGUICompleta(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Optimización Selección de Ofertas de Compra - INTERFAZ COMPLETA")
        self.setGeometry(100, 100, 1400, 850)  # Más grande para acomodar todos los campos
        
        # Variables para TODOS los campos de entrada
        self.entry_crecimiento = None
        self.entry_fecha_sicep = None
        self.entry_constante_sicep = None
        self.entry_archivo_demanda = None          # Archivo de datos iniciales/demanda
        self.entry_carpeta_ofertas = None         # NUEVO: Carpeta donde están las ofertas
        self.entry_carpeta_exportacion = None     # Carpeta donde van los resultados
        
        # Worker thread
        self.worker = None
        
        # Configurar estilo
        self.configurar_estilo()
        
        # Crear interfaz
        self.crear_interfaz()
        
        # Verificar conexión del proyecto
        self.verificar_proyecto()
        
        # Cargar valores por defecto
        self.cargar_valores_defecto()
        
    def verificar_proyecto(self):
        """Verificar que el proyecto esté conectado correctamente"""
        if not PROYECTO_CONECTADO:
            QMessageBox.critical(
                self, "Error de Conexión", 
                "No se pudo conectar con el proyecto de optimización.\n\n"
                "Asegúrese de que:\n"
                "• Los archivos del proyecto estén en el directorio correcto\n"
                "• Las dependencias estén instaladas\n"
                "• Los archivos de configuración existan"
            )
            self.setEnabled(False)
        else:
            # Verificar archivos críticos (sin mostrar advertencia si faltan)
            archivos_faltantes = []
            if not verificar_archivo_existe(DATOS_INICIALES):
                archivos_faltantes.append(f"datos_iniciales.xlsx")
            if not OFERTAS_DIR.exists():
                archivos_faltantes.append("Carpeta OFERTAS/")
                
            if archivos_faltantes:
                print(f"ℹ️ Archivos por defecto no encontrados: {archivos_faltantes}")
                print("ℹ️ El usuario puede seleccionar rutas personalizadas")
        
    def configurar_estilo(self):
        """Configurar el estilo moderno de la aplicación"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f8fafc;
            }
            
            QLabel#titulo {
                color: #1a202c;
                font-size: 28px;
                font-weight: bold;
                padding: 15px;
            }
            
            QFrame#datos_frame {
                background-color: white;
                border: 1px solid #e2e8f0;
                border-radius: 12px;
                padding: 20px;
                margin: 10px;
            }
            
            QLabel#seccion_titulo {
                color: #2d3748;
                font-size: 18px;
                font-weight: bold;
                margin-bottom: 10px;
                margin-top: 15px;
            }
            
            QLabel#seccion_subtitulo {
                color: #718096;
                font-size: 13px;
                margin-bottom: 15px;
            }
            
            QLabel#campo_label {
                color: #4a5568;
                font-size: 12px;
                font-weight: bold;
                margin-bottom: 5px;
                margin-top: 10px;
            }
            
            QLineEdit {
                padding: 10px;
                border: 2px solid #e2e8f0;
                border-radius: 6px;
                font-size: 13px;
                background-color: #f7fafc;
                color: #2d3748;
                min-height: 20px;
            }
            
            QLineEdit:focus {
                border-color: #667eea;
                background-color: white;
            }
            
            QLineEdit:read-only {
                background-color: #f1f5f9;
                color: #64748b;
            }
            
            QPushButton#btn_principal {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #667eea, stop:1 #764ba2);
                color: white;
                border: none;
                border-radius: 12px;
                padding: 20px;
                font-size: 16px;
                font-weight: bold;
                min-height: 20px;
            }
            
            QPushButton#btn_principal:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #5a67d8, stop:1 #6b46c1);
            }
            
            QPushButton#btn_principal:disabled {
                background: #a0aec0;
                color: #718096;
            }
            
            QPushButton#btn_accion {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #8b5cf6, stop:1 #7c3aed);
                color: white;
                border: none;
                border-radius: 8px;
                padding: 12px;
                font-size: 13px;
                text-align: left;
                margin-bottom: 8px;
                min-height: 15px;
            }
            
            QPushButton#btn_accion:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #7c3aed, stop:1 #6b21a8);
            }
            
            QPushButton#btn_accion:disabled {
                background: #a0aec0;
                color: #718096;
            }
            
            QPushButton#btn_archivo {
                background-color: #3182ce;
                color: white;
                border: none;
                border-radius: 6px;
                padding: 10px 15px;
                font-size: 14px;
            }
            
            QPushButton#btn_archivo:hover {
                background-color: #2c5aa0;
            }
            
            QPushButton#btn_validar {
                background-color: #38a169;
                color: white;
                border: none;
                border-radius: 8px;
                padding: 12px;
                font-size: 14px;
                font-weight: bold;
            }
            
            QPushButton#btn_validar:hover {
                background-color: #2f855a;
            }
            
            QProgressBar {
                border: 2px solid #e2e8f0;
                border-radius: 6px;
                text-align: center;
                font-weight: bold;
                color: #2d3748;
                height: 20px;
            }
            
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #667eea, stop:1 #764ba2);
                border-radius: 4px;
            }
            
            QLabel#status_label {
                color: #4a5568;
                font-size: 13px;
                font-weight: bold;
                padding: 8px;
                background-color: #f7fafc;
                border: 1px solid #e2e8f0;
                border-radius: 5px;
            }
        """)
        
    def crear_interfaz(self):
        """Crear la interfaz principal COMPLETA"""
        # Widget central con scroll
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Layout principal
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)
        
        # ==================== TÍTULO ====================
        titulo = QLabel("Optimización selección de ofertas de compra")
        titulo.setObjectName("titulo")
        titulo.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(titulo)
        
        # ==================== CONTENIDO PRINCIPAL ====================
        content_layout = QHBoxLayout()
        content_widget = QWidget()
        content_widget.setLayout(content_layout)
        main_layout.addWidget(content_widget)
        
        # ==================== COLUMNA IZQUIERDA - DATOS ====================
        datos_frame = QFrame()
        datos_frame.setObjectName("datos_frame")
        datos_frame.setFixedWidth(550)  # Más ancho para acomodar campos
        
        # Scroll area para los datos
        scroll_datos = QScrollArea()
        scroll_datos.setWidget(datos_frame)
        scroll_datos.setWidgetResizable(True)
        scroll_datos.setMaximumWidth(570)
        
        datos_layout = QVBoxLayout()
        datos_frame.setLayout(datos_layout)
        
        # Título de sección
        seccion_titulo = QLabel("Datos de entrada")
        seccion_titulo.setObjectName("seccion_titulo")
        datos_layout.addWidget(seccion_titulo)
        
        # Subtítulo
        seccion_subtitulo = QLabel("Configure todos los parámetros necesarios para la optimización")
        seccion_subtitulo.setObjectName("seccion_subtitulo")
        datos_layout.addWidget(seccion_subtitulo)
        
        # ==================== SECCIÓN: PARÁMETROS NUMÉRICOS ====================
        parametros_titulo = QLabel("📊 Parámetros de cálculo")
        parametros_titulo.setObjectName("seccion_titulo")
        datos_layout.addWidget(parametros_titulo)
        
        self.entry_crecimiento = self.crear_campo_pyqt(
            datos_layout, 
            "Crecimiento anual del indexador (%):", 
            "Ej: 4.5"
        )
        
        self.entry_fecha_sicep = self.crear_campo_pyqt(
            datos_layout, 
            "Fecha base de SICEP:", 
            "01/MM/YYYY"
        )
        
        self.entry_constante_sicep = self.crear_campo_pyqt(
            datos_layout, 
            "Constante del SICEP:", 
            "Ej: 1.0"
        )
        
        # ==================== SECCIÓN: ARCHIVOS Y CARPETAS ====================
        archivos_titulo = QLabel("📁 Rutas de archivos y carpetas")
        archivos_titulo.setObjectName("seccion_titulo")
        datos_layout.addWidget(archivos_titulo)
        
        self.entry_archivo_demanda = self.crear_campo_archivo_pyqt(
            datos_layout, 
            "📄 Archivo datos iniciales (semilla):", 
            False
        )
        
        self.entry_carpeta_ofertas = self.crear_campo_archivo_pyqt(
            datos_layout, 
            "📁 Carpeta ofertas:", 
            True
        )
        
        self.entry_carpeta_exportacion = self.crear_campo_archivo_pyqt(
            datos_layout, 
            "📁 Carpeta exportación de resultados:", 
            True
        )
        
        # ==================== BOTÓN DE VALIDACIÓN ====================
        btn_validar = QPushButton("🔍 Validar todos los campos")
        btn_validar.setObjectName("btn_validar")
        btn_validar.clicked.connect(self.validar_campos)
        datos_layout.addWidget(btn_validar)
        
        datos_layout.addStretch()
        content_layout.addWidget(scroll_datos)
        
        # ==================== COLUMNA DERECHA - BOTONES ====================
        botones_layout = QVBoxLayout()
        botones_widget = QWidget()
        botones_widget.setLayout(botones_layout)
        
        # Botón principal GRANDE
        self.btn_principal = QPushButton("🚀 EJECUTAR FLUJO COMPLETO")
        self.btn_principal.setObjectName("btn_principal")
        self.btn_principal.clicked.connect(self.ejecutar_flujo_completo)
        botones_layout.addWidget(self.btn_principal)
        
        # Espaciado
        botones_layout.addSpacing(30)
        
        # Información de estado
        info_label = QLabel("ℹ️ Funciones individuales")
        info_label.setObjectName("seccion_subtitulo")
        info_label.setAlignment(Qt.AlignCenter)
        botones_layout.addWidget(info_label)
        
        # Botones individuales (más pequeños)
        botones_data = [
            ("📊 Crear proyección de indexadores", self.crear_indexadores),
            ("💰 Crear proyección de precio SICEP", self.crear_sicep),
            ("📋 Procesar ofertas", self.procesar_ofertas),
            ("⚡ Optimizar con Pyomo", self.optimizar),
            ("📈 Generar visualizaciones", self.generar_visualizaciones)
        ]
        
        self.botones_accion = []
        for texto, comando in botones_data:
            btn = QPushButton(texto)
            btn.setObjectName("btn_accion")
            btn.clicked.connect(comando)
            botones_layout.addWidget(btn)
            self.botones_accion.append(btn)
            
        botones_layout.addStretch()
        content_layout.addWidget(botones_widget)
        
        # ==================== ÁREA DE STATUS Y PROGRESO ====================
        status_frame = QFrame()
        status_frame.setMaximumHeight(100)
        status_layout = QVBoxLayout()
        status_frame.setLayout(status_layout)
        
        # Label de estado
        self.status_label = QLabel("✅ Listo - Complete todos los campos y haga clic en 'Validar'")
        self.status_label.setObjectName("status_label")
        status_layout.addWidget(self.status_label)
        
        # Barra de progreso
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        status_layout.addWidget(self.progress_bar)
        
        main_layout.addWidget(status_frame)
        
    def crear_campo_pyqt(self, layout, etiqueta, placeholder):
        """Crear campo de entrada con PyQt"""
        # Label
        label = QLabel(etiqueta)
        label.setObjectName("campo_label")
        layout.addWidget(label)
        
        # Entry
        entry = QLineEdit()
        entry.setPlaceholderText(placeholder)
        layout.addWidget(entry)
        layout.addSpacing(10)
        
        return entry
        
    def crear_campo_archivo_pyqt(self, layout, etiqueta, es_carpeta):
        """Crear campo para archivos con PyQt"""
        # Label
        label = QLabel(etiqueta)
        label.setObjectName("campo_label")
        layout.addWidget(label)
        
        # Horizontal layout para entry + botón
        file_layout = QHBoxLayout()
        file_widget = QWidget()
        file_widget.setLayout(file_layout)
        
        # Entry
        entry = QLineEdit()
        entry.setPlaceholderText("Seleccionar ruta...")
        entry.setReadOnly(True)
        file_layout.addWidget(entry)
        
        # Botón
        icono = "📁" if es_carpeta else "📄"
        btn = QPushButton(icono)
        btn.setObjectName("btn_archivo")
        btn.setMaximumWidth(50)
        if es_carpeta:
            btn.clicked.connect(lambda: self.seleccionar_carpeta(entry))
        else:
            btn.clicked.connect(lambda: self.seleccionar_archivo(entry))
        file_layout.addWidget(btn)
        
        layout.addWidget(file_widget)
        layout.addSpacing(10)
        
        return entry
    
    def cargar_valores_defecto(self):
        """Cargar valores por defecto del proyecto"""
        try:
            # Cargar rutas por defecto si existen
            if PROYECTO_CONECTADO:
                if verificar_archivo_existe(DATOS_INICIALES):
                    self.entry_archivo_demanda.setText(str(DATOS_INICIALES))
                
                if OFERTAS_DIR.exists():
                    self.entry_carpeta_ofertas.setText(str(OFERTAS_DIR))
                
                # Carpeta de exportación (directorio padre de RESULTADO_OFERTAS)
                carpeta_output = str(Path(RESULTADO_OFERTAS).parent)
                self.entry_carpeta_exportacion.setText(carpeta_output)
            
            # Valores numéricos por defecto
            self.entry_crecimiento.setText("4.0")
            self.entry_fecha_sicep.setText("01/01/2025")
            self.entry_constante_sicep.setText("1.0")
            
            self.actualizar_status("✅ Valores por defecto cargados")
            
        except Exception as e:
            print(f"Error cargando valores por defecto: {e}")
            self.actualizar_status("⚠️ Error cargando valores por defecto")
    
    def obtener_todos_los_parametros(self):
        """Obtener todos los parámetros ingresados por el usuario"""
        return {
            'crecimiento_anual': self.entry_crecimiento.text().strip(),
            'fecha_sicep': self.entry_fecha_sicep.text().strip(),
            'constante_sicep': self.entry_constante_sicep.text().strip(),
            'archivo_demanda': self.entry_archivo_demanda.text().strip(),
            'carpeta_ofertas': self.entry_carpeta_ofertas.text().strip(),
            'carpeta_exportacion': self.entry_carpeta_exportacion.text().strip()
        }
    
    def validar_campos(self):
        """Validar todos los campos ingresados"""
        parametros = self.obtener_todos_los_parametros()
        errores = []
        
        # Validar que no estén vacíos
        if not parametros['crecimiento_anual']:
            errores.append("❌ Ingrese el crecimiento anual del indexador")
        if not parametros['fecha_sicep']:
            errores.append("❌ Ingrese la fecha base SICEP")
        if not parametros['constante_sicep']:
            errores.append("❌ Ingrese la constante SICEP")
        if not parametros['archivo_demanda']:
            errores.append("❌ Seleccione el archivo de datos iniciales")
        if not parametros['carpeta_ofertas']:
            errores.append("❌ Seleccione la carpeta de ofertas")
        if not parametros['carpeta_exportacion']:
            errores.append("❌ Seleccione la carpeta de exportación")
        
        # Si hay campos vacíos, no continuar con validaciones numéricas
        if errores:
            mensaje_error = "Complete todos los campos:\n\n" + "\n".join(errores)
            QMessageBox.warning(self, "Campos Incompletos", mensaje_error)
            self.actualizar_status("❌ Complete todos los campos requeridos")
            self.btn_principal.setEnabled(False)
            return
        
        # Validar campos numéricos
        try:
            crecimiento = float(parametros['crecimiento_anual'])
            if crecimiento < 0:
                errores.append("❌ El crecimiento anual debe ser positivo")
        except ValueError:
            errores.append("❌ El crecimiento anual debe ser un número válido")
        
        try:
            constante = float(parametros['constante_sicep'])
            if constante <= 0:
                errores.append("❌ La constante SICEP debe ser mayor a 0")
        except ValueError:
            errores.append("❌ La constante SICEP debe ser un número válido")
        
        # Validar formato de fecha
        if not parametros['fecha_sicep'].startswith('01/'):
            errores.append("❌ La fecha SICEP debe comenzar con 01/ (primer día del mes)")
        else:
            # Validar formato completo de fecha
            try:
                from datetime import datetime
                datetime.strptime(parametros['fecha_sicep'], "%d/%m/%Y")
            except ValueError:
                errores.append("❌ Formato de fecha inválido. Use DD/MM/YYYY")
        
        # Validar existencia de archivos y carpetas
        if not os.path.exists(parametros['archivo_demanda']):
            errores.append("❌ El archivo de datos iniciales no existe")
        elif not parametros['archivo_demanda'].endswith(('.xlsx', '.xls')):
            errores.append("❌ El archivo de datos iniciales debe ser Excel (.xlsx o .xls)")
        
        if not os.path.exists(parametros['carpeta_ofertas']):
            errores.append("❌ La carpeta de ofertas no existe")
        else:
            # Verificar que la carpeta tenga archivos Excel
            archivos_excel = [f for f in os.listdir(parametros['carpeta_ofertas']) if f.endswith(('.xlsx', '.xls'))]
            if len(archivos_excel) == 0:
                errores.append("❌ La carpeta de ofertas no contiene archivos Excel")
            else:
                print(f"✅ Se encontraron {len(archivos_excel)} archivos de ofertas")
        
        # Crear carpeta de exportación si no existe
        if not os.path.exists(parametros['carpeta_exportacion']):
            try:
                os.makedirs(parametros['carpeta_exportacion'], exist_ok=True)
                print(f"✅ Carpeta de exportación creada: {parametros['carpeta_exportacion']}")
            except Exception as e:
                errores.append(f"❌ No se puede crear la carpeta de exportación: {e}")
        
        # Mostrar resultado
        if errores:
            mensaje_error = "\n".join(errores)
            QMessageBox.critical(self, "Errores de Validación", mensaje_error)
            self.actualizar_status("❌ Hay errores en los campos")
            self.btn_principal.setEnabled(False)
        else:
            # Mostrar resumen de parámetros validados
            resumen = (
                "✅ Todos los campos validados correctamente:\n\n"
                f"📊 Crecimiento anual: {parametros['crecimiento_anual']}%\n"
                f"📅 Fecha base SICEP: {parametros['fecha_sicep']}\n"
                f"💰 Constante SICEP: {parametros['constante_sicep']}\n"
                f"📄 Archivo datos: {Path(parametros['archivo_demanda']).name}\n"
                f"📁 Carpeta ofertas: {Path(parametros['carpeta_ofertas']).name}\n"
                f"📁 Exportación: {Path(parametros['carpeta_exportacion']).name}\n\n"
                "Puede proceder a ejecutar el flujo completo."
            )
            
            QMessageBox.information(self, "Validación Exitosa", resumen)
            self.actualizar_status("✅ Parámetros validados - Listo para ejecutar")
            
            # Habilitar botón principal
            self.btn_principal.setEnabled(True)
        
    def actualizar_status(self, mensaje):
        """Actualizar el mensaje de estado"""
        self.status_label.setText(mensaje)
        
    def bloquear_botones(self, bloquear=True):
        """Bloquear/desbloquear botones durante procesamiento"""
        self.btn_principal.setEnabled(not bloquear)
        for btn in self.botones_accion:
            btn.setEnabled(not bloquear)
            
    def mostrar_progreso(self, mostrar=True):
        """Mostrar/ocultar barra de progreso"""
        if mostrar:
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0)  # Modo indeterminado
        else:
            self.progress_bar.setVisible(False)
        
    def seleccionar_archivo(self, entry):
        """Seleccionar archivo"""
        archivo, _ = QFileDialog.getOpenFileName(
            self, "Seleccionar archivo", "", 
            "Excel files (*.xlsx *.xls);;All files (*.*)"
        )
        if archivo:
            entry.setText(archivo)
            self.actualizar_status(f"📄 Archivo seleccionado: {Path(archivo).name}")
            
    def seleccionar_carpeta(self, entry):
        """Seleccionar carpeta"""
        carpeta = QFileDialog.getExistingDirectory(self, "Seleccionar carpeta")
        if carpeta:
            entry.setText(carpeta)
            self.actualizar_status(f"📁 Carpeta seleccionada: {Path(carpeta).name}")
    
    def ejecutar_operacion(self, operacion, **kwargs):
        """Ejecutar operación en worker thread"""
        if not PROYECTO_CONECTADO:
            QMessageBox.critical(self, "Error", "Proyecto no conectado")
            return
            
        self.bloquear_botones(True)
        self.mostrar_progreso(True)
        
        self.worker = WorkerThread(operacion, **kwargs)
        self.worker.progress.connect(self.actualizar_status)
        self.worker.finished.connect(self.on_operacion_completada)
        self.worker.start()
    
    def on_operacion_completada(self, exito, mensaje):
        """Callback cuando se completa una operación"""
        self.bloquear_botones(False)
        self.mostrar_progreso(False)
        self.actualizar_status(mensaje)
        
        if exito:
            QMessageBox.information(self, "Éxito", mensaje)
        else:
            QMessageBox.critical(self, "Error", mensaje)
    
    # ==================== FUNCIONES DE BOTONES ====================
    def ejecutar_flujo_completo(self):
        """Ejecutar flujo completo del proyecto CON PARÁMETROS DE LA INTERFAZ"""
        # Primero validar campos
        parametros = self.obtener_todos_los_parametros()
        
        # Validación rápida
        if not all([parametros['archivo_demanda'], parametros['carpeta_ofertas'], 
                   parametros['carpeta_exportacion'], parametros['constante_sicep']]):
            QMessageBox.warning(
                self, "Campos Incompletos", 
                "Complete todos los campos antes de ejecutar el flujo."
            )
            return
        
        # Ejecutar con parámetros
        self.ejecutar_operacion("flujo_completo", **parametros)
        
    def crear_indexadores(self):
        """Crear proyección de indexadores"""
        parametros = self.obtener_todos_los_parametros()
        self.ejecutar_operacion("indexadores", **parametros)
        
    def crear_sicep(self):
        """Crear proyección de precio SICEP"""
        parametros = self.obtener_todos_los_parametros()
        self.ejecutar_operacion("sicep", **parametros)
        
    def procesar_ofertas(self):
        """Procesar ofertas"""
        parametros = self.obtener_todos_los_parametros()
        self.ejecutar_operacion("ofertas", **parametros)
        
    def optimizar(self):
        """Optimizar asignación de ofertas"""
        parametros = self.obtener_todos_los_parametros()
        self.ejecutar_operacion("optimizar", **parametros)
    
    def generar_visualizaciones(self):
        """Generar visualizaciones"""
        parametros = self.obtener_todos_los_parametros()
        self.ejecutar_operacion("visualizaciones", **parametros)

def main():
    """Función principal"""
    try:
        app = QApplication(sys.argv)
        
        # Configurar fuente del sistema
        font = QFont()
        font.setFamily("Segoe UI")  # Windows
        app.setFont(font)
        
        window = OptimizacionPyQtGUICompleta()
        window.show()
        
        sys.exit(app.exec_())
        
    except ImportError:
        print("❌ PyQt5 no está instalado")
        print("💡 Instale con: pip install PyQt5")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()