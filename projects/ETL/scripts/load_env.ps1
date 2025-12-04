# ============================================================================
# Script de chargement des variables d'environnement depuis .env
# ============================================================================
# Usage: . E:\Prefect\projects\ETL\scripts\load_env.ps1
# ============================================================================

Write-Host "`n[LOAD] Chargement variables depuis .env..." -ForegroundColor Cyan

$envFile = "E:\Prefect\projects\ETL\.env"

if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^([^#][^=]+)=(.+)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            Set-Item -Path "env:$name" -Value $value
            Write-Host "  [OK] $name" -ForegroundColor Green
        }
    }
    Write-Host "`n[OK] Variables d'environnement chargees !`n" -ForegroundColor Green
} else {
    Write-Host "`n[ERROR] Fichier .env introuvable : $envFile`n" -ForegroundColor Red
}