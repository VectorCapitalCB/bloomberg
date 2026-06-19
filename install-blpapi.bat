@echo off
REM Instala el blpapi.jar (descargado del SDK de Bloomberg) en tu repositorio Maven local.
REM Uso:   install-blpapi.bat  C:\ruta\blpapi-3.24.6-1.jar  3.24.6-1
REM El blpapi.jar NO esta en Maven Central (licencia Bloomberg); se baja del SDK de la Terminal.
setlocal
if "%~1"=="" (
  echo Uso: install-blpapi.bat ^<ruta-al-blpapi.jar^> ^<version^>
  echo Ej.: install-blpapi.bat C:\blp\API\APIv3\JavaAPI\lib\blpapi-3.24.6-1.jar 3.24.6-1
  exit /b 1
)
set JAR=%~1
set VER=%~2
if "%VER%"=="" set VER=3.24.6-1

mvn install:install-file -Dfile="%JAR%" -DgroupId=com.bloomberglp -DartifactId=blpapi -Dversion=%VER% -Dpackaging=jar
echo.
echo Listo. Ahora compila la ingesta Bloomberg con:  mvn -Pbloomberg -DskipTests package
echo (si tu version != 3.24.6-1, ajusta ^<blpapi.version^> en pom.xml)
endlocal
