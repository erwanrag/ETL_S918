# Script d'inspection des tables RAW
# Fichier : E:\Prefect\postgresql\inspect_tables.ps1

Write-Host "================================" -ForegroundColor Cyan
Write-Host "INSPECTION DES TABLES RAW" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

$env:PGPASSWORD = "Dbyk!vMWZ4z0v*"

# ============================================================================
# 1. Inspecter raw_client
# ============================================================================

Write-Host "Table : raw.raw_client" -ForegroundColor Yellow
Write-Host ""

psql -h localhost -U postgres -d etl_db -c "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'raw_client' ORDER BY ordinal_position;"

Write-Host ""
Write-Host "Echantillon (2 lignes) :" -ForegroundColor Yellow
psql -h localhost -U postgres -d etl_db -x -c "SELECT * FROM raw.raw_client LIMIT 2;"

Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# ============================================================================
# 2. Inspecter raw_produit
# ============================================================================

Write-Host "Table : raw.raw_produit" -ForegroundColor Yellow
Write-Host ""

psql -h localhost -U postgres -d etl_db -c "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'raw_produit' ORDER BY ordinal_position;"

Write-Host ""
Write-Host "Echantillon (2 lignes) :" -ForegroundColor Yellow
psql -h localhost -U postgres -d etl_db -x -c "SELECT * FROM raw.raw_produit LIMIT 2;"

Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host "INSTRUCTIONS :" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Copiez la liste des colonnes ci-dessus" -ForegroundColor White
Write-Host "2. Partagez-la avec Claude" -ForegroundColor White
Write-Host "3. Claude generera les bons modeles dbt automatiquement" -ForegroundColor White
Write-Host ""