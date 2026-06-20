@echo off
REM Quita el auto-arranque de ORB-BLOOMBERG (detiene y borra la tarea programada).
REM Ejecutar como Administrador.
schtasks /end /tn "ORB-BLOOMBERG" 2>nul
schtasks /delete /tn "ORB-BLOOMBERG" /f
echo Auto-arranque eliminado.
pause
