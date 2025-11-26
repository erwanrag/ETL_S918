@echo off
cd E:\Prefect\projects\ETL
powershell -NoExit -Command "E:\Prefect\venv\Scripts\activate.ps1; prefect worker start --pool default-agent-pool"
