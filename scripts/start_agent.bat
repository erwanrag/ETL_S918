@echo off
cd /d "E:\Prefect\server"
set PREFECT_HOME=E:\Prefect\server
set PREFECT_API_URL=http://127.0.0.1:4200/api
timeout /t 10 /nobreak
"E:\SQLServer_Databases\CBM_ETL\ETL\prefect_env\Scripts\prefect.exe" agent start -q default
