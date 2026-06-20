@echo off
REM ============================================================
REM  ORB-BLOOMBERG - auto-arranque
REM  Crea una tarea programada que arranca el app al iniciar sesion del usuario.
REM  Corre en la SESION del usuario -> ve la Terminal Bloomberg (sin el problema
REM  de "Session 0" de un servicio LocalSystem).
REM
REM  Ejecutar UNA sola vez, como ADMINISTRADOR, en el PC con la Terminal.
REM ============================================================
echo Creando tarea de auto-arranque ORB-BLOOMBERG...
schtasks /create /tn "ORB-BLOOMBERG" /tr "'C:\Program Files\ORB-BLOOMBERG\ORB-BLOOMBERG.exe'" /sc onlogon /rl highest /f
if %errorlevel%==0 (
  echo.
  echo OK: arrancara solo en cada inicio de sesion.
  echo Arrancando ahora...
  schtasks /run /tn "ORB-BLOOMBERG"
  echo.
  echo Revisa el log en:  C:\ProgramData\ORB-BLOOMBERG\logs\orb-bloomberg.log
) else (
  echo.
  echo [ERROR] No se pudo crear la tarea. Cierra y vuelve a abrir este .bat con
  echo         clic derecho -^> "Ejecutar como administrador".
)
pause
