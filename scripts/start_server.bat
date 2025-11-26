@echo off
cd /d "E:\Prefect\server"
set PREFECT_HOME=E:\Prefect\server
set PREFECT_API_URL=http://127.0.0.1:4200/api
"E:\SQLServer_Databases\CBM_ETL\ETL\prefect_env\Scripts\prefect.exe" server start --host 0.0.0.0 --port 4200
