"""
Módulo auxiliar para mostrar diálogos
Detecta si estamos en GUI o consola y muestra el diálogo apropiado
"""

import sys
import threading

# Variable global para el worker thread (se establece desde gui_pyqt.py)
_current_worker = None

def set_worker_thread(worker):
    """Establece el worker thread actual para poder emitir señales"""
    global _current_worker
    _current_worker = worker

def mostrar_dialogo_confirmacion(titulo, mensaje, detalles=None):
    """
    Muestra un diálogo de confirmación (Sí/No)
    
    Args:
        titulo (str): Título del diálogo
        mensaje (str): Mensaje principal
        detalles (str): Detalles adicionales (opcional)
    
    Returns:
        bool: True si el usuario confirmó, False si canceló
    """
    # Detectar si estamos en modo GUI (ejecutable o con PyQt5 disponible)
    try:
        from PyQt5.QtWidgets import QApplication
        
        # Verificar si hay una instancia de QApplication activa
        app = QApplication.instance()
        
        if app is not None:
            # Verificar si estamos en el thread principal
            current_thread = threading.current_thread()
            main_thread = threading.main_thread()
            
            if current_thread != main_thread:
                # ESTAMOS EN UN WORKER THREAD
                global _current_worker
                
                if _current_worker is not None:
                    # Emitir señal para mostrar diálogo en thread principal
                    print(f"\n⏳ Solicitando confirmación del usuario...")
                    _current_worker.ask_confirmation.emit(titulo, mensaje, detalles or "")
                    
                    # Esperar respuesta del usuario
                    response = _current_worker.wait_for_user_response()
                    
                    if response:
                        print(f"✅ Usuario confirmó: Continuar")
                    else:
                        print(f"❌ Usuario canceló el proceso")
                    
                    return response
                else:
                    # No hay worker configurado, auto-continuar con log
                    print(f"\n⚠️ {titulo}")
                    print(f"📋 {mensaje}")
                    if detalles:
                        print(f"ℹ️ {detalles}")
                    print(f"✅ Continuando automáticamente (no hay worker configurado)\n")
                    return True
            
            # Estamos en el thread principal, podemos mostrar el diálogo directamente
            from PyQt5.QtWidgets import QMessageBox
            
            msgBox = QMessageBox()
            msgBox.setIcon(QMessageBox.Warning)
            msgBox.setWindowTitle(titulo)
            msgBox.setText(mensaje)
            
            if detalles:
                msgBox.setInformativeText(detalles)
            
            msgBox.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            msgBox.setDefaultButton(QMessageBox.Yes)
            
            # Personalizar botones en español
            btnYes = msgBox.button(QMessageBox.Yes)
            btnYes.setText("Sí, continuar")
            
            btnNo = msgBox.button(QMessageBox.No)
            btnNo.setText("No, cancelar")
            
            resultado = msgBox.exec_()
            
            return resultado == QMessageBox.Yes
        else:
            # PyQt5 disponible pero no hay QApplication activa
            # Usar consola
            return _mostrar_dialogo_consola(mensaje, detalles)
            
    except ImportError:
        # PyQt5 no disponible, usar consola
        return _mostrar_dialogo_consola(mensaje, detalles)

def _mostrar_dialogo_consola(mensaje, detalles=None):
    """
    Versión de consola del diálogo de confirmación
    """
    print(f"\n{'='*60}")
    print(mensaje)
    if detalles:
        print(f"\n{detalles}")
    print(f"{'='*60}")
    
    while True:
        respuesta = input("\n¿Continuar? (s/n): ").lower().strip()
        if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
            return True
        elif respuesta in ['n', 'no']:
            return False
        else:
            print("❌ Respuesta inválida. Por favor ingrese 's' para Sí o 'n' para No.")

def mostrar_mensaje_info(titulo, mensaje):
    """Muestra un mensaje informativo"""
    print(f"\nℹ️ {titulo}: {mensaje}")

def mostrar_mensaje_error(titulo, mensaje):
    """Muestra un mensaje de error"""
    print(f"\n❌ ERROR - {titulo}: {mensaje}")