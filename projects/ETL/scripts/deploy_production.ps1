# E:\Prefect\projects\ETL\scripts\deploy_production.ps1

# Arrêter ancien scheduler
Get-Process | Where-Object {$_.ProcessName -like "*python*" -and $_.CommandLine -like "*serve_scheduler*"} | Stop-Process -Force

# Mise à jour Git
git pull origin main

# Redémarrer scheduler
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd E:\Prefect\projects\ETL; python flows\orchestration\serve_scheduler.py --config production"

Write-Host "[OK] Production redémarrée"