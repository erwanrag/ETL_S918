# ============================================================================
# Script de Backup Automatique PostgreSQL - etl_db
# ============================================================================
# Fichier : E:\Prefect\projects\ETL\scripts\backup_etl_db.ps1
# Usage   : .\backup_etl_db.ps1
# ============================================================================

# Configuration
$PG_BIN = "C:\Program Files\PostgreSQL\18\bin"  # Adapter selon votre installation
$DB_NAME = "etl_db"
$DB_USER = "postgres"
$BACKUP_DIR = "B:\PostgreSQL_Backup\etl_db"
$RETENTION_DAYS = 30

# Créer dossier de backup si inexistant
if (!(Test-Path $BACKUP_DIR)) {
    New-Item -ItemType Directory -Path $BACKUP_DIR -Force | Out-Null
    Write-Host "[OK] Dossier de backup créé : $BACKUP_DIR" -ForegroundColor Green
}

# Nom du fichier backup avec timestamp
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupFile = Join-Path $BACKUP_DIR "etl_db_$timestamp.backup"

Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "BACKUP PostgreSQL - $DB_NAME" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

# Charger mot de passe depuis .env
$envFile = "E:\Prefect\projects\ETL\.env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^ETL_PG_PASSWORD=(.+)$') {
            $env:PGPASSWORD = $matches[1].Trim()
        }
    }
}

if (!$env:PGPASSWORD) {
    Write-Host "[ERROR] Mot de passe PostgreSQL introuvable dans .env" -ForegroundColor Red
    Write-Host "[INFO] Définissez ETL_PG_PASSWORD dans .env" -ForegroundColor Yellow
    exit 1
}

Write-Host "[START] Début du backup..." -ForegroundColor Yellow
Write-Host "[INFO] Base de données : $DB_NAME" -ForegroundColor Gray
Write-Host "[INFO] Fichier : $backupFile" -ForegroundColor Gray

# Exécuter pg_dump
try {
    $pgDump = Join-Path $PG_BIN "pg_dump.exe"
    
    if (!(Test-Path $pgDump)) {
        Write-Host "[ERROR] pg_dump introuvable : $pgDump" -ForegroundColor Red
        Write-Host "[INFO] Vérifiez le chemin PostgreSQL dans la variable PG_BIN" -ForegroundColor Yellow
        exit 1
    }
    
    $startTime = Get-Date
    
    & $pgDump -U $DB_USER -h localhost -p 5432 -F c -b -v -f $backupFile $DB_NAME 2>&1 | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        $endTime = Get-Date
        $duration = ($endTime - $startTime).TotalSeconds
        $fileSize = (Get-Item $backupFile).Length / 1MB
        
        Write-Host "`n[OK] Backup réussi !" -ForegroundColor Green
        Write-Host "[INFO] Taille : $([math]::Round($fileSize, 2)) MB" -ForegroundColor Gray
        Write-Host "[INFO] Durée : $([math]::Round($duration, 2)) secondes" -ForegroundColor Gray
        Write-Host "[INFO] Fichier : $backupFile`n" -ForegroundColor Gray
    } else {
        Write-Host "`n[ERROR] Échec du backup (code: $LASTEXITCODE)" -ForegroundColor Red
        exit 1
    }
    
} catch {
    Write-Host "`n[ERROR] Exception lors du backup : $_" -ForegroundColor Red
    exit 1
}

# Nettoyage : supprimer backups > RETENTION_DAYS jours
Write-Host "[CLEANUP] Nettoyage des anciens backups (>$RETENTION_DAYS jours)..." -ForegroundColor Yellow

$cutoffDate = (Get-Date).AddDays(-$RETENTION_DAYS)
$oldBackups = Get-ChildItem -Path $BACKUP_DIR -Filter "etl_db_*.backup" | Where-Object { $_.LastWriteTime -lt $cutoffDate }

if ($oldBackups) {
    $oldBackups | ForEach-Object {
        Remove-Item $_.FullName -Force
        Write-Host "[DELETED] $($_.Name)" -ForegroundColor Gray
    }
    Write-Host "[OK] $($oldBackups.Count) ancien(s) backup(s) supprimé(s)" -ForegroundColor Green
} else {
    Write-Host "[INFO] Aucun ancien backup à supprimer" -ForegroundColor Gray
}

# Statistiques finales
$allBackups = Get-ChildItem -Path $BACKUP_DIR -Filter "etl_db_*.backup"
$totalSize = ($allBackups | Measure-Object -Property Length -Sum).Sum / 1GB

Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "STATISTIQUES" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Backups disponibles : $($allBackups.Count)" -ForegroundColor Gray
Write-Host "Espace utilisé : $([math]::Round($totalSize, 2)) GB" -ForegroundColor Gray
Write-Host "Dernier backup : $backupFile" -ForegroundColor Gray
Write-Host "============================================================`n" -ForegroundColor Cyan

# Nettoyer variable mot de passe
Remove-Item env:PGPASSWORD -ErrorAction SilentlyContinue

Write-Host "[OK] Backup terminé avec succès !`n" -ForegroundColor Green
