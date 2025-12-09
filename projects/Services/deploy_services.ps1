# ============================================================================
# Script PowerShell : Déploiement Services sur Prefect
# ============================================================================
# Usage : .\deploy_services.ps1
# ============================================================================

param(
    [string]$Action = "deploy",  # deploy, test, verify
    [switch]$SkipTests = $false
)

$ErrorActionPreference = "Stop"

Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "DÉPLOIEMENT SERVICES - DONNÉES DE RÉFÉRENCE" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

# Répertoires
$ServicesRoot = "E:\Prefect\projects\Services"
$ETLRoot = "E:\Prefect\projects\ETL"

# Vérifier existence
if (-not (Test-Path $ServicesRoot)) {
    Write-Host "[ERROR] Répertoire Services introuvable : $ServicesRoot" -ForegroundColor Red
    exit 1
}

# ============================================================================
# FONCTION : Vérifier Pré-requis
# ============================================================================

function Test-Prerequisites {
    Write-Host "[1/6] VÉRIFICATION PRÉ-REQUIS..." -ForegroundColor Yellow
    
    # Python
    try {
        $pythonVersion = python --version 2>&1
        Write-Host "  [OK] Python : $pythonVersion" -ForegroundColor Green
    } catch {
        Write-Host "  [ERROR] Python non trouvé" -ForegroundColor Red
        return $false
    }
    
    # Prefect
    try {
        $prefectVersion = prefect version 2>&1
        Write-Host "  [OK] Prefect : $prefectVersion" -ForegroundColor Green
    } catch {
        Write-Host "  [ERROR] Prefect non installé" -ForegroundColor Red
        Write-Host "  [FIX] pip install prefect" -ForegroundColor Yellow
        return $false
    }
    
    # Dépendances Python
    $packages = @("psycopg2", "requests")
    foreach ($pkg in $packages) {
        try {
            python -c "import $pkg" 2>&1 | Out-Null
            Write-Host "  [OK] Package Python : $pkg" -ForegroundColor Green
        } catch {
            Write-Host "  [ERROR] Package manquant : $pkg" -ForegroundColor Red
            Write-Host "  [FIX] pip install $pkg" -ForegroundColor Yellow
            return $false
        }
    }
    
    return $true
}

# ============================================================================
# FONCTION : Vérifier PostgreSQL
# ============================================================================

function Test-PostgreSQL {
    Write-Host "`n[2/6] VÉRIFICATION POSTGRESQL..." -ForegroundColor Yellow
    
    cd $ServicesRoot
    
    try {
        $result = python -c @"
import sys
sys.path.insert(0, r'$ETLRoot')
from flows.config.pg_config import config
import psycopg2

try:
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    cur.execute('SELECT 1')
    cur.close()
    conn.close()
    print('OK')
except Exception as e:
    print(f'ERROR: {e}')
    sys.exit(1)
"@
        
        if ($result -eq "OK") {
            Write-Host "  [OK] Connexion PostgreSQL réussie" -ForegroundColor Green
            return $true
        } else {
            Write-Host "  [ERROR] $result" -ForegroundColor Red
            return $false
        }
    } catch {
        Write-Host "  [ERROR] Impossible de tester connexion PostgreSQL" -ForegroundColor Red
        Write-Host "  $_" -ForegroundColor Red
        return $false
    }
}

# ============================================================================
# FONCTION : Vérifier Tables PostgreSQL
# ============================================================================

function Test-PostgreSQLTables {
    Write-Host "`n[3/6] VÉRIFICATION TABLES POSTGRESQL..." -ForegroundColor Yellow
    
    cd $ServicesRoot
    
    $tables = @("currencies", "currency_rates", "currency_rates_today", "time_dimension")
    $allExist = $true
    
    foreach ($table in $tables) {
        try {
            $result = python -c @"
import sys
sys.path.insert(0, r'$ETLRoot')
from flows.config.pg_config import config
import psycopg2

conn = psycopg2.connect(config.get_connection_string())
cur = conn.cursor()
cur.execute('''
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'reference' 
        AND table_name = '$table'
    )
''')
exists = cur.fetchone()[0]
cur.close()
conn.close()
print('EXISTS' if exists else 'NOT_FOUND')
"@
            
            if ($result -eq "EXISTS") {
                Write-Host "  [OK] Table : reference.$table" -ForegroundColor Green
            } else {
                Write-Host "  [WARN] Table manquante : reference.$table" -ForegroundColor Yellow
                $allExist = $false
            }
        } catch {
            Write-Host "  [ERROR] Erreur vérification table : $table" -ForegroundColor Red
            $allExist = $false
        }
    }
    
    if (-not $allExist) {
        Write-Host "`n  [ACTION] Créer les tables :" -ForegroundColor Yellow
        Write-Host "    psql -U postgres -d etl_db -f sql/create_tables.sql`n" -ForegroundColor Cyan
    }
    
    return $allExist
}

# ============================================================================
# FONCTION : Exécuter Tests
# ============================================================================

function Invoke-Tests {
    Write-Host "`n[4/6] EXÉCUTION TESTS..." -ForegroundColor Yellow
    
    cd $ServicesRoot
    
    if (Test-Path "tests/test_flows.py") {
        try {
            pytest tests/test_flows.py -v --tb=short
            Write-Host "  [OK] Tests réussis" -ForegroundColor Green
            return $true
        } catch {
            Write-Host "  [WARN] Certains tests ont échoué" -ForegroundColor Yellow
            return $false
        }
    } else {
        Write-Host "  [SKIP] Aucun test trouvé" -ForegroundColor Gray
        return $true
    }
}

# ============================================================================
# FONCTION : Déployer sur Prefect
# ============================================================================

function Deploy-Flows {
    Write-Host "`n[5/6] DÉPLOIEMENT PREFECT..." -ForegroundColor Yellow
    
    cd $ServicesRoot
    
    try {
        prefect deploy --all
        Write-Host "  [OK] Déploiements créés" -ForegroundColor Green
        return $true
    } catch {
        Write-Host "  [ERROR] Échec déploiement Prefect" -ForegroundColor Red
        Write-Host "  $_" -ForegroundColor Red
        return $false
    }
}

# ============================================================================
# FONCTION : Afficher Résumé
# ============================================================================

function Show-Summary {
    Write-Host "`n[6/6] RÉSUMÉ DÉPLOIEMENT" -ForegroundColor Yellow
    Write-Host "============================================================" -ForegroundColor Cyan
    
    cd $ServicesRoot
    
    try {
        Write-Host "`n📦 DEPLOYMENTS CRÉÉS :" -ForegroundColor Green
        prefect deployment ls | Select-String "services/"
        
        Write-Host "`n📋 COMMANDES UTILES :" -ForegroundColor Cyan
        Write-Host "  # Exécuter manuellement" -ForegroundColor Gray
        Write-Host "  prefect deployment run services/load-currency-codes" -ForegroundColor White
        Write-Host "  prefect deployment run services/load-exchange-rates" -ForegroundColor White
        Write-Host "  prefect deployment run services/build-time-dimension" -ForegroundColor White
        
        Write-Host "`n  # Monitoring" -ForegroundColor Gray
        Write-Host "  prefect server start" -ForegroundColor White
        Write-Host "  # → http://127.0.0.1:4200`n" -ForegroundColor Gray
        
        Write-Host "============================================================`n" -ForegroundColor Cyan
    } catch {
        Write-Host "  [WARN] Impossible d'afficher résumé" -ForegroundColor Yellow
    }
}

# ============================================================================
# EXECUTION PRINCIPALE
# ============================================================================

switch ($Action) {
    "deploy" {
        if (-not (Test-Prerequisites)) { exit 1 }
        if (-not (Test-PostgreSQL)) { exit 1 }
        
        $tablesExist = Test-PostgreSQLTables
        
        if (-not $SkipTests) {
            Invoke-Tests | Out-Null
        }
        
        if (-not (Deploy-Flows)) { exit 1 }
        
        Show-Summary
        
        Write-Host "✅ DÉPLOIEMENT TERMINÉ AVEC SUCCÈS`n" -ForegroundColor Green
    }
    
    "test" {
        if (-not (Test-Prerequisites)) { exit 1 }
        if (-not (Test-PostgreSQL)) { exit 1 }
        Invoke-Tests
    }
    
    "verify" {
        if (-not (Test-Prerequisites)) { exit 1 }
        if (-not (Test-PostgreSQL)) { exit 1 }
        Test-PostgreSQLTables | Out-Null
        Show-Summary
    }
    
    default {
        Write-Host "[ERROR] Action inconnue : $Action" -ForegroundColor Red
        Write-Host "Actions valides : deploy, test, verify" -ForegroundColor Yellow
        exit 1
    }
}