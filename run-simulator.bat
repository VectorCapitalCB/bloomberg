@echo off
REM ORB-BLOOMBERG en modo simulador (no requiere Terminal Bloomberg ni blpapi).
setlocal
cd /d "%~dp0"
if not exist logs mkdir logs
set APP_VERSION=1.14
echo Arrancando ORB-BLOOMBERG (SIMULADOR) ...
java -jar target\ORB-BLOOMBERG-%APP_VERSION%-fat.jar config\SIMULATOR.properties
endlocal
