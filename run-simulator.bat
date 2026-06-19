@echo off
REM ORB-BLOOMBERG en modo simulador (no requiere Terminal Bloomberg ni blpapi).
setlocal
cd /d "%~dp0"
if not exist logs mkdir logs
echo Arrancando ORB-BLOOMBERG (SIMULADOR) en server.host=0.0.0.0:8050 ...
java -jar target\ORB-BLOOMBERG-1.0-fat.jar config\SIMULATOR.properties
endlocal
