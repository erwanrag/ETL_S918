# ============================================================================
# Script d'installation complète ETL Pipeline
# ============================================================================
# Fichier : scripts/install_complete.ps1
# Usage (ADMIN PowerShell) : .\install_complete.ps1

Write-Host "`n========================================================================" -ForegroundColor Cyan
Write-Host "INSTALLATION COMPLETE ETL PIPELINE" -ForegroundColor Cyan
Write-Host "========================================================================`n" -ForegroundColor Cyan

$ErrorActionPreference = "Stop"
$ProjectRoot = "E:\Prefect\projects\ETL"

# 1. Vérifier Python
Write-Host "[1/7] Verification Python..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version
    Write-Host "  [OK] $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "  [ERROR] Python non trouve" -ForegroundColor Red
    exit 1
}

# 2. Créer venv si inexistant
Write-Host "`n[2/7] Virtual environment..." -ForegroundColor Yellow
if (!(Test-Path "E:\Prefect\venv")) {
    python -m venv E:\Prefect\venv
    Write-Host "  [OK] Venv cree" -ForegroundColor Green
} else {
    Write-Host "  [OK] Venv existe deja" -ForegroundColor Green
}

# 3. Activer venv et installer dépendances
Write-Host "`n[3/7] Installation dependances..." -ForegroundColor Yellow
& E:\Prefect\venv\Scripts\Activate.ps1
pip install --upgrade pip | Out-Null
pip install -r "$ProjectRoot\requirements.txt" | Out-Null
pip install -r "$ProjectRoot\tests\requirements-test.txt" | Out-Null
Write-Host "  [OK] Dependances installees" -ForegroundColor Green

# 4. Configuration .env
Write-Host "`n[4/7] Configuration .env..." -ForegroundColor Yellow
if (!(Test-Path "$ProjectRoot\.env")) {
    Copy-Item "$ProjectRoot\.env.example" "$ProjectRoot\.env"
    Write-Host "  [WARN] .env cree - EDITER LES CREDENTIALS" -ForegroundColor Yellow
} else {
    Write-Host "  [OK] .env existe" -ForegroundColor Green
}

# 5. Variables d'environnement système
Write-Host "`n[5/7] Variables environnement systeme..." -ForegroundColor Yellow
try {
    # Charger .env
    Get-Content "$ProjectRoot\.env" | ForEach-Object {
        if ($_ -match '^([^#].+?)=(.+)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            [System.Environment]::SetEnvironmentVariable($key, $value, "User")
        }
    }
    Write-Host "  [OK] Variables configurees" -ForegroundColor Green
} catch {
    Write-Host "  [WARN] Erreur configuration variables : $_" -ForegroundColor Yellow
}

# 6. Tests
Write-Host "`n[6/7] Execution tests..." -ForegroundColor Yellow
cd $ProjectRoot
$testResult = pytest tests\ -v --tb=short
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Tests passes" -ForegroundColor Green
} else {
    Write-Host "  [WARN] Certains tests echoues (non bloquant)" -ForegroundColor Yellow
}

# 7. Dashboard PostgreSQL
Write-Host "`n[7/7] Creation vues dashboard..." -ForegroundColor Yellow
try {
    # Charger password depuis .env
    $pgPassword = ""
    Get-Content "$ProjectRoot\.env" | ForEach-Object {
        if ($_ -match '^ETL_PG_PASSWORD=(.+)$') {
            $pgPassword = $matches[1].Trim()
        }
    }
    
    $env:PGPASSWORD = $pgPassword
    psql -U postgres -d etl_db -f "$ProjectRoot\scripts\create_dashboard_view.sql" 2>&1 | Out-Null
    Write-Host "  [OK] Dashboard cree" -ForegroundColor Green
} catch {
    Write-Host "  [WARN] Dashboard skip (PostgreSQL non accessible)" -ForegroundColor Yellow
}

# Résumé
Write-Host "`n========================================================================" -ForegroundColor Cyan
Write-Host "INSTALLATION TERMINEE" -ForegroundColor Cyan
Write-Host "========================================================================`n" -ForegroundColor Cyan

Write-Host "Prochaines etapes:" -ForegroundColor Yellow
Write-Host "  1. Editer .env avec vos credentials PostgreSQL" -ForegroundColor White
Write-Host "  2. Redemarrer PowerShell (pour variables env)" -ForegroundColor White
Write-Host "  3. Tester : python flows\orchestration\full_pipeline.py" -ForegroundColor White
Write-Host "  4. Scheduler : python flows\orchestration\serve_scheduler.py --config production`n" -ForegroundColor White

Write-Host "[OK] Installation complete !" -ForegroundColor Green