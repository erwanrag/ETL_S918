@echo off
cd /d E:\Prefect\projects
call E:\Prefect\venv\Scripts\activate.bat
prefect worker start --pool default > E:\Prefect\logs\worker.log 2>&1