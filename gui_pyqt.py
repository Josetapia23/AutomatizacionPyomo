"""
GUI con PyQt - Sistema de Optimización Energética
ASPECTO PROFESIONAL: Interfaz nativa del sistema operativo
"""

import sys
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                           QHBoxLayout, QGridLayout, QLabel, QLineEdit, QPushButton, 
                           QFileDialog, QMessageBox, QTextEdit, QFrame, QSizePolicy)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QPalette, QColor, QIcon
import os
from datetime import datetime

class OptimizacionPyQtGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Optimización Selección de Ofertas de Compra")
        self.setGeometry(100, 100, 1200, 800)
        
        # Variables
        self.crecimiento_var = ""
        self.fecha_sicep_var = ""
        self.constante_sicep_var = ""
        self.ruta_demanda_var = ""
        self.ruta_exportacion_var = ""
        
        # Configurar estilo
        self.configurar_estilo()
        
        # Crear interfaz
        self.crear_interfaz()
        
    def configurar_estilo(self):
        """Configurar el estilo moderno de la aplicación"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f8fafc;
            }
            
            QLabel#titulo {
                color: #1a202c;
                font-size: 32px;
                font-weight: bold;
                padding: 20px;
            }
            
            QFrame#datos_frame {
                background-color: white;
                border: 1px solid #e2e8f0;
                border-radius: 12px;
                padding: 20px;
            }
            
            QLabel#seccion_titulo {
                color: #2d3748;
                font-size: 20px;
                font-weight: bold;
                margin-bottom: 10px;
            }
            
            QLabel#seccion_subtitulo {
                color: #718096;
                font-size: 14px;
                margin-bottom: 20px;
            }
            
            QLabel#campo_label {
                color: #4a5568;
                font-size: 12px;
                font-weight: bold;
                margin-bottom: 5px;
            }
            
            QLineEdit {
                padding: 12px;
                border: 2px solid #e2e8f0;
                border-radius: 8px;
                font-size: 14px;
                background-color: #f7fafc;
                color: #2d3748;
            }
            
            QLineEdit:focus {
                border-color: #667eea;
                background-color: white;
            }
            
            QPushButton#btn_principal {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #667eea, stop:1 #764ba2);
                color: white;
                border: none;
                border-radius: 12px;
                padding: 18px;
                font-size: 16px;
                font-weight: bold;
            }
            
            QPushButton#btn_principal:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #5a67d8, stop:1 #6b46c1);
            }
            
            QPushButton#btn_accion {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #8b5cf6, stop:1 #7c3aed);
                color: white;
                border: none;
                border-radius: 10px;
                padding: 15px;
                font-size: 14px;
                text-align: left;
                margin-bottom: 10px;
            }
            
            QPushButton#btn_accion:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #7c3aed, stop:1 #6b21a8);
            }
            
            QPushButton#btn_config {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #6b7280, stop:1 #4b5563);
                color: white;
                border: none;
                border-radius: 10px;
                padding: 15px;
                font-size: 14px;
                text-align: left;
            }
            
            QPushButton#btn_config:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #4b5563, stop:1 #374151);
            }
            
            QPushButton#btn_archivo {
                background-color: #3182ce;
                color: white;
                border: none;
                border-radius: 8px;
                padding: 12px 20px;
                font-size: 16px;
            }
            
            QPushButton#btn_archivo:hover {
                background-color: #2c5aa0;
            }
            
            QTextEdit {
                border: 1px solid #e2e8f0;
                border-radius: 8px;
                background-color: #f8f9fa;
                font-family: 'Consolas', monospace;
                font-size: 12px;
                padding: 10px;
            }
        """)
        
    def crear_interfaz(self):
        """Crear la interfaz principal"""
        # Widget central
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Layout principal
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)
        
        # ==================== TÍTULO ====================
        titulo = QLabel("Optimización selección\nde ofertas de compra")
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
        datos_frame.setFixedWidth(500)
        
        datos_layout = QVBoxLayout()
        datos_frame.setLayout(datos_layout)
        
        # Título de sección
        seccion_titulo = QLabel("Datos de entrada")
        seccion_titulo.setObjectName("seccion_titulo")
        datos_layout.addWidget(seccion_titulo)
        
        # Subtítulo
        seccion_subtitulo = QLabel("Optimización")
        seccion_subtitulo.setObjectName("seccion_subtitulo")
        datos_layout.addWidget(seccion_subtitulo)
        
        # Campos de entrada
        self.crear_campo_pyqt(datos_layout, "Crecimiento anual del indexador:", "Ingrese porcentaje")
        self.crear_campo_pyqt(datos_layout, "Fecha base de SICEP", "01/MM/YYY")
        self.crear_campo_pyqt(datos_layout, "Constante del SICEP", "#")
        self.crear_campo_archivo_pyqt(datos_layout, "Cargar demanda", False)
        self.crear_campo_archivo_pyqt(datos_layout, "Exportación de resultados", True)
        
        datos_layout.addStretch()
        content_layout.addWidget(datos_frame)
        
        # ==================== COLUMNA DERECHA - BOTONES ====================
        botones_layout = QVBoxLayout()
        botones_widget = QWidget()
        botones_widget.setLayout(botones_layout)
        
        # Botón principal
        btn_principal = QPushButton("▶ Ejecutar flujo completo")
        btn_principal.setObjectName("btn_principal")
        btn_principal.clicked.connect(self.ejecutar_flujo_completo)
        botones_layout.addWidget(btn_principal)
        
        # Espaciado
        botones_layout.addSpacing(20)
        
        # Botones individuales
        botones_lista = [
            ("📊 Crear/Actualizar proyección de indexadores", "btn_accion", self.crear_indexadores),
            ("📊 Crear/Actualizar proyección de precio SICEP", "btn_accion", self.crear_sicep),
            ("📋 Procesar ofertas (Solo tabla maestra y precios)", "btn_accion", self.procesar_ofertas),
            ("⚡ Optimizar asignación de ofertas con Pyomo", "btn_accion", self.optimizar),
            ("⚙️ Ver configuración actual", "btn_config", self.ver_config)
        ]
        
        for texto, estilo, comando in botones_lista:
            btn = QPushButton(texto)
            btn.setObjectName(estilo)
            btn.clicked.connect(comando)
            botones_layout.addWidget(btn)
            
        botones_layout.addStretch()
        content_layout.addWidget(botones_widget)
        
        # ==================== ÁREA DE LOG ====================
        log_frame = QFrame()
        log_frame.setMaximumHeight(150)
        log_layout = QVBoxLayout()
        log_frame.setLayout(log_layout)
        
        log_titulo = QLabel("Log de Ejecución")
        log_titulo.setObjectName("campo_label")
        log_layout.addWidget(log_titulo)
        
        self.log_text = QTextEdit()
        self.log_text.setMaximumHeight(120)
        log_layout.addWidget(self.log_text)
        
        main_layout.addWidget(log_frame)
        
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
        layout.addSpacing(15)
        
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
        btn.setMaximumWidth(60)
        if es_carpeta:
            btn.clicked.connect(lambda: self.seleccionar_carpeta(entry))
        else:
            btn.clicked.connect(lambda: self.seleccionar_archivo(entry))
        file_layout.addWidget(btn)
        
        layout.addWidget(file_widget)
        layout.addSpacing(15)
        
    def log_mensaje(self, mensaje):
        """Agregar mensaje al log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.append(f"[{timestamp}] {mensaje}")
        
    def seleccionar_archivo(self, entry):
        """Seleccionar archivo"""
        archivo, _ = QFileDialog.getOpenFileName(
            self, "Seleccionar archivo", "", 
            "Excel files (*.xlsx *.xls);;All files (*.*)"
        )
        if archivo:
            entry.setText(archivo)
            self.log_mensaje(f"📄 Archivo: {os.path.basename(archivo)}")
            
    def seleccionar_carpeta(self, entry):
        """Seleccionar carpeta"""
        carpeta = QFileDialog.getExistingDirectory(self, "Seleccionar carpeta")
        if carpeta:
            entry.setText(carpeta)
            self.log_mensaje(f"📁 Carpeta: {os.path.basename(carpeta)}")
    
    # ==================== FUNCIONES DE BOTONES ====================
    def ejecutar_flujo_completo(self):
        self.log_mensaje("🚀 Ejecutando flujo completo...")
        QMessageBox.information(
            self, "PyQt Demo", 
            "¡Interfaz PyQt funcionando!\n\n" +
            "Aspecto más nativo y profesional.\n" +
            "Pero requiere reescribir TODO el código."
        )
        
    def crear_indexadores(self):
        self.log_mensaje("📊 Creando indexadores...")
        QMessageBox.information(self, "Info", "Crear indexadores (Demo PyQt)")
        
    def crear_sicep(self):
        self.log_mensaje("📊 Creando SICEP...")
        QMessageBox.information(self, "Info", "Crear SICEP (Demo PyQt)")
        
    def procesar_ofertas(self):
        self.log_mensaje("📋 Procesando ofertas...")
        QMessageBox.information(self, "Info", "Procesar ofertas (Demo PyQt)")
        
    def optimizar(self):
        self.log_mensaje("⚡ Optimizando...")
        QMessageBox.information(self, "Info", "Optimizar (Demo PyQt)")
        
    def ver_config(self):
        self.log_mensaje("⚙️ Configuración...")
        QMessageBox.information(self, "Info", "Ver configuración (Demo PyQt)")

def main():
    """Función principal"""
    try:
        app = QApplication(sys.argv)
        
        # Configurar fuente del sistema
        font = QFont()
        font.setFamily("Segoe UI")  # Windows
        app.setFont(font)
        
        window = OptimizacionPyQtGUI()
        window.show()
        
        sys.exit(app.exec_())
        
    except ImportError:
        print("❌ PyQt5 no está instalado")
        print("💡 Instale con: pip install PyQt5")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()