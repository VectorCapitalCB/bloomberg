@echo off
REM ============================================================
REM  Construye el instalador .msi ACTUALIZABLE de ORB-BLOOMBERG
REM  (JRE embebido + servicio de Windows que arranca solo).
REM
REM  Requisitos en ESTA maquina (build):
REM    - JDK 21 (trae jpackage)            [ya lo tienes]
REM    - WiX Toolset v3.11                 https://wixtoolset.org/  (jpackage lo usa para .msi)
REM    - blpapi.jar instalado en .m2       (install-blpapi.bat)
REM    - blpapi3_64.dll                    (se empaqueta para el runtime)
REM
REM  Para ACTUALIZAR: sube APP_VERSION, reconstruye, e instala el .msi nuevo.
REM  El UPGRADE_UUID es FIJO -> Windows actualiza en sitio (conserva config y logs en %PROGRAMDATA%).
REM ============================================================
setlocal
cd /d "%~dp0"

set APP_VERSION=1.0
set VENDOR=VectorCapital
set UPGRADE_UUID=7b9d2f4a-3c61-4e8b-a2d5-1f0e6c8b4a93

set BLPAPI_VERSION=3.24.6-1
set BLPAPI_JAR=%USERPROFILE%\.m2\repository\com\bloomberglp\blpapi\%BLPAPI_VERSION%\blpapi-%BLPAPI_VERSION%.jar
REM dll para empaquetar dentro del .msi: si dejas el dll del SDK en .\lib\ se usa ese
REM (util en una maquina de build SIN Bloomberg instalado); si no, busca la ruta tipica.
set BLPAPI_DLL=C:\blp\DAPI\blpapi3_64.dll
if exist "lib\blpapi3_64.dll" set BLPAPI_DLL=lib\blpapi3_64.dll

echo [1/4] Compilando jar con ingesta Bloomberg (perfil -Pbloomberg)...
call mvn -Pbloomberg -DskipTests package
if errorlevel 1 ( echo [ERROR] Fallo el build Maven & exit /b 1 )

echo [2/4] Preparando carpeta de input...
if exist build\input rmdir /s /q build\input
mkdir build\input
copy /y "target\ORB-BLOOMBERG-%APP_VERSION%-fat.jar" build\input\ >nul
if exist "%BLPAPI_JAR%" ( copy /y "%BLPAPI_JAR%" build\input\ >nul ) else ( echo [AVISO] no encontre %BLPAPI_JAR% )
if exist "%BLPAPI_DLL%" ( copy /y "%BLPAPI_DLL%" build\input\ >nul ) else ( echo [AVISO] no encontre %BLPAPI_DLL% - el servicio lo necesita en runtime )

echo [3/4] Limpiando destino...
if exist build\dist rmdir /s /q build\dist
mkdir build\dist

echo [4/4] jpackage -^> .msi ...
jpackage ^
  --type msi ^
  --name ORB-BLOOMBERG ^
  --app-version %APP_VERSION% ^
  --vendor "%VENDOR%" ^
  --description "ORB Bloomberg market data redistributor" ^
  --input build\input ^
  --main-jar ORB-BLOOMBERG-%APP_VERSION%-fat.jar ^
  --main-class cl.vc.arb.apps.fh.Bootstrap ^
  --java-options "-Djava.library.path=$APPDIR" ^
  --win-upgrade-uuid %UPGRADE_UUID% ^
  --launcher-as-service ^
  --win-menu ^
  --dest build\dist
if errorlevel 1 ( echo [ERROR] Fallo jpackage ^(WiX instalado?^) & exit /b 1 )

echo.
echo  OK -^> instalador en: build\dist\ORB-BLOOMBERG-%APP_VERSION%.msi
echo  Instala con doble click (o msiexec /i). El servicio "ORB-BLOOMBERG" arranca solo.
echo  Config y logs viven en: %%PROGRAMDATA%%\ORB-BLOOMBERG\
endlocal
