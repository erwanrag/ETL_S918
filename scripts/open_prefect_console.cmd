@echo off
REM ======================================================================
REM  Clean launcher for Prefect Cloud console
REM ======================================================================

set TARGET_DIR=E:\Prefect
set ACTIVATE_SCRIPT=E:\Prefect\venv\Scripts\activate.ps1

REM --- IMPORTANT : remove any PREFECT_* inherited from parent process ---
set PREFECT_HOME=
set PREFECT_API_URL=
set PREFECT_API_KEY=

REM Vérification du venv
IF NOT EXIST "%ACTIVATE_SCRIPT%" (
    echo [ERREUR] Le script d'activation du venv est introuvable :
    echo %ACTIVATE_SCRIPT%
    pause
    exit /b 1
)

REM Ouvrir PowerShell dans E:\Prefect, activer le venv et rester ouvert
powershell -NoLogo -NoExit -Command "Set-Location '%TARGET_DIR%'; & '%ACTIVATE_SCRIPT%'"
