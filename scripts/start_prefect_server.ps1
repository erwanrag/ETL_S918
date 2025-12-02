Set-Location "E:\Prefect"
& "E:\Prefect\venv\Scripts\Activate.ps1"

Remove-Item Env:\PREFECT_API_URL -ErrorAction SilentlyContinue
Remove-Item Env:\PREFECT_API_KEY -ErrorAction SilentlyContinue

prefect server start
