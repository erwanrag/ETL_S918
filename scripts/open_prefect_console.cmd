@echo off
set TARGET_DIR=E:\Prefect
set ACTIVATE_SCRIPT=E:\Prefect\venv\Scripts\activate.ps1

powershell -NoLogo -NoExit -Command "Set-Location '%TARGET_DIR%'; & '%ACTIVATE_SCRIPT%'; 
$env:PREFECT_API_URL='http://127.0.0.1:4200/api'"
