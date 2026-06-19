@echo off
REM ============================================================
REM  Construye un APP-IMAGE (carpeta con JRE embebido + launcher), SIN necesitar WiX.
REM  Sirve para probar el empaquetado/arranque en esta maquina. Usa el jar por defecto
REM  (modo simulador si pasas SIMULATOR.properties). NO es el instalador final.
REM ============================================================
setlocal
cd /d "%~dp0"
set APP_VERSION=1.0

echo [1/3] Compilando jar (build por defecto)...
call mvn -q -DskipTests package
if errorlevel 1 ( echo [ERROR] Fallo el build Maven & exit /b 1 )

echo [2/3] Preparando input...
if exist build\input rmdir /s /q build\input
mkdir build\input
copy /y "target\ORB-BLOOMBERG-%APP_VERSION%-fat.jar" build\input\ >nul
xcopy /y /i /e config build\input\config >nul

echo [3/3] jpackage --type app-image ...
if exist build\image rmdir /s /q build\image
mkdir build\image
jpackage ^
  --type app-image ^
  --name ORB-BLOOMBERG ^
  --app-version %APP_VERSION% ^
  --input build\input ^
  --main-jar ORB-BLOOMBERG-%APP_VERSION%-fat.jar ^
  --main-class cl.vc.arb.apps.fh.Bootstrap ^
  --dest build\image
if errorlevel 1 ( echo [ERROR] Fallo jpackage & exit /b 1 )

echo.
echo  OK -^> app-image en: build\image\ORB-BLOOMBERG\
echo  Ejecutable: build\image\ORB-BLOOMBERG\ORB-BLOOMBERG.exe
endlocal
