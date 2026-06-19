@echo off
REM ORB-BLOOMBERG en vivo contra Bloomberg Desktop API (DAPI).
REM Requisitos:
REM   1) Build con perfil bloomberg:  mvn -Pbloomberg -DskipTests package
REM   2) blpapi.jar instalado en .m2 (ver install-blpapi.bat)
REM   3) blpapi3_64.dll accesible (ajusta BLPAPI_DLL_DIR)
REM   4) Terminal Bloomberg logueada y corriendo en esta maquina
setlocal
cd /d "%~dp0"
if not exist logs mkdir logs

REM --- Ajusta estas dos rutas a tu instalacion ---
set BLPAPI_VERSION=3.24.6-1
set BLPAPI_DLL_DIR=C:\blp\DAPI

set BLPAPI_JAR=%USERPROFILE%\.m2\repository\com\bloomberglp\blpapi\%BLPAPI_VERSION%\blpapi-%BLPAPI_VERSION%.jar

if not exist "%BLPAPI_JAR%" (
  echo [ERROR] No se encuentra blpapi.jar en %BLPAPI_JAR%
  echo Instalalo primero:  install-blpapi.bat C:\ruta\blpapi-%BLPAPI_VERSION%.jar %BLPAPI_VERSION%
  exit /b 1
)

echo Arrancando ORB-BLOOMBERG (BLOOMBERG en vivo) ...
java -Djava.library.path="%BLPAPI_DLL_DIR%" -cp "target\ORB-BLOOMBERG-1.0-fat.jar;%BLPAPI_JAR%" cl.vc.arb.apps.fh.MainApp config\BLOOMBERG.properties
endlocal
