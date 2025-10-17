@echo off
REM ============================================================
REM Script para construir el ejecutable de Optimizacion Energia
REM ============================================================

echo.
echo ========================================
echo  CONSTRUCCION DE EJECUTABLE
echo  Sistema de Optimizacion Energetica
echo ========================================
echo.

REM Verificar que Python este instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python no esta instalado o no esta en el PATH
    echo Por favor instale Python 3.8 o superior
    pause
    exit /b 1
)

echo [OK] Python detectado
echo.

REM Verificar que PyInstaller este instalado
python -c "import PyInstaller" >nul 2>&1
if errorlevel 1 (
    echo [!] PyInstaller no esta instalado
    echo [*] Instalando PyInstaller...
    pip install pyinstaller
    if errorlevel 1 (
        echo [ERROR] No se pudo instalar PyInstaller
        pause
        exit /b 1
    )
)

echo [OK] PyInstaller disponible
echo.

REM Verificar que exista el archivo .spec
if not exist "optimizacion_energia.spec" (
    echo [ERROR] No se encuentra el archivo optimizacion_energia.spec
    echo Asegurese de tener el archivo .spec en el directorio actual
    pause
    exit /b 1
)

echo [OK] Archivo .spec encontrado
echo.

REM Verificar que exista el solver CBC
if not exist "CBC\cbc.exe" (
    echo [ADVERTENCIA] No se encuentra CBC\cbc.exe
    echo El ejecutable puede no funcionar correctamente sin el solver
    echo.
    echo Descargue CBC desde:
    echo https://www.coin-or.org/download/binary/Cbc/
    echo.
    choice /C SN /M "Desea continuar de todas formas?"
    if errorlevel 2 exit /b 1
)

echo [OK] Solver CBC encontrado
echo.

REM Limpiar compilaciones anteriores
echo [*] Limpiando compilaciones anteriores...
if exist "build" rmdir /s /q build
if exist "dist" rmdir /s /q dist
echo [OK] Limpieza completada
echo.

REM Construir el ejecutable
echo ========================================
echo [*] INICIANDO CONSTRUCCION...
echo ========================================
echo.

pyinstaller optimizacion_energia.spec

if errorlevel 1 (
    echo.
    echo [ERROR] La construccion fallo
    pause
    exit /b 1
)

echo.
echo ========================================
echo [OK] CONSTRUCCION EXITOSA!
echo ========================================
echo.

REM Verificar que se creo el ejecutable
if exist "dist\OptimizacionEnergia\OptimizacionEnergia.exe" (
    echo [OK] Ejecutable creado correctamente
    echo.
    echo Ubicacion: dist\OptimizacionEnergia\
    echo.
    
    REM Crear carpetas necesarias en dist
    echo [*] Creando estructura de carpetas...
    if not exist "dist\OptimizacionEnergia\data" mkdir "dist\OptimizacionEnergia\data"
    if not exist "dist\OptimizacionEnergia\OFERTAS" mkdir "dist\OptimizacionEnergia\OFERTAS"
    if not exist "dist\OptimizacionEnergia\output" mkdir "dist\OptimizacionEnergia\output"
    
    echo [OK] Carpetas creadas
    echo.
    
    echo ========================================
    echo  INSTRUCCIONES DE USO:
    echo ========================================
    echo.
    echo 1. La carpeta completa esta en: dist\OptimizacionEnergia\
    echo.
    echo 2. Copie esta carpeta a donde desee instalar el programa
    echo.
    echo 3. Al ejecutar por primera vez, el programa creara carpetas
    echo    adicionales en: C:\Users\[Usuario]\OptimizacionEnergia\
    echo.
    echo 4. Ejecute: OptimizacionEnergia.exe
    echo.
    echo ========================================
    
    REM Preguntar si desea comprimir
    echo.
    choice /C SN /M "Desea crear un archivo ZIP para distribucion?"
    if not errorlevel 2 (
        echo.
        echo [*] Creando archivo ZIP...
        powershell -command "Compress-Archive -Path 'dist\OptimizacionEnergia' -DestinationPath 'OptimizacionEnergia_v1.0.0.zip' -Force"
        if exist "OptimizacionEnergia_v1.0.0.zip" (
            echo [OK] Archivo ZIP creado: OptimizacionEnergia_v1.0.0.zip
        )
    )
    
) else (
    echo [ERROR] No se encontro el ejecutable
    echo Revise los mensajes de error anteriores
)

echo.
pause