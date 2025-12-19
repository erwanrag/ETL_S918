# ============================================================================
# Script de Restore PostgreSQL - etl_db
# ============================================================================
# Fichier : E:\Prefect\projects\ETL\scripts\restore_etl_db.ps1
# Usage   : .\restore_etl_db.ps1 -BackupFile "chemin\vers\backup.backup"
# ============================================================================

param(
    [Parameter(Mandatory=$false)]
    [string]$BackupFile
)

# Configuration
$PG_BIN = "C:\Program Files\PostgreSQL\18\bin"
$DB_NAME = "etl_db"
$DB_USER = "postgres"
$BACKUP_DIR = "B:\PostgreSQL_Backup\etl_db"

Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "RESTORE PostgreSQL - $DB_NAME" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

# Si aucun fichier spécifié, lister les backups disponibles
if (!$BackupFile) {
    Write-Host "[INFO] Backups disponibles :`n" -ForegroundColor Yellow
    
    $backups = Get-ChildItem -Path $BACKUP_DIR -Filter "etl_db_*.backup" | Sort-Object LastWriteTime -Descending
    
    if ($backups.Count -eq 0) {
        Write-Host "[ERROR] Aucun backup trouvé dans $BACKUP_DIR" -ForegroundColor Red
        exit 1
    }
    
    for ($i = 0; $i -lt $backups.Count; $i++) {
        $size = [math]::Round($backups[$i].Length / 1MB, 2)
        $date = $backups[$i].LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss")
        Write-Host "  [$i] $($backups[$i].Name) - $size MB - $date" -ForegroundColor Gray
    }
    
    Write-Host "`n[PROMPT] Sélectionnez un backup (0-$($backups.Count - 1)) ou 'q' pour quitter : " -NoNewline -ForegroundColor Yellow
    $selection = Read-Host
    
    if ($selection -eq 'q') {
        Write-Host "[INFO] Opération annulée" -ForegroundColor Yellow
        exit 0
    }
    
    if ($selection -match '^\d+$' -and [int]$selection -ge 0 -and [int]$selection -lt $backups.Count) {
        $BackupFile = $backups[[int]$selection].FullName
    } else {
        Write-Host "[ERROR] Sélection invalide" -ForegroundColor Red
        exit 1
    }
}

# Vérifier que le fichier existe
if (!(Test-Path $BackupFile)) {
    Write-Host "[ERROR] Fichier de backup introuvable : $BackupFile" -ForegroundColor Red
    exit 1
}

Write-Host "`n[WARNING] ATTENTION : Cette opération va ÉCRASER la base de données $DB_NAME !" -ForegroundColor Red
Write-Host "[INFO] Backup à restaurer : $BackupFile" -ForegroundColor Gray
Write-Host "`n[PROMPT] Tapez 'CONFIRMER' pour continuer : " -NoNewline -ForegroundColor Yellow
$confirmation = Read-Host

if ($confirmation -ne 'CONFIRMER') {
    Write-Host "[INFO] Opération annulée" -ForegroundColor Yellow
    exit 0
}

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
    Write-Host "[ERROR] Mot de passe PostgreSQL introuvable" -ForegroundColor Red
    exit 1
}

Write-Host "`n[START] Début du restore..." -ForegroundColor Yellow

try {
    $pgRestore = Join-Path $PG_BIN "pg_restore.exe"
    
    if (!(Test-Path $pgRestore)) {
        Write-Host "[ERROR] pg_restore introuvable : $pgRestore" -ForegroundColor Red
        exit 1
    }
    
    $startTime = Get-Date
    
    # pg_restore avec --clean (supprime objets existants)
    & $pgRestore -U $DB_USER -h localhost -p 5432 -d $DB_NAME --clean --if-exists -v $BackupFile 2>&1 | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        $endTime = Get-Date
        $duration = ($endTime - $startTime).TotalSeconds
        
        Write-Host "`n[OK] Restore réussi !" -ForegroundColor Green
        Write-Host "[INFO] Durée : $([math]::Round($duration, 2)) secondes" -ForegroundColor Gray
    } else {
        Write-Host "`n[WARNING] Restore terminé avec warnings (code: $LASTEXITCODE)" -ForegroundColor Yellow
        Write-Host "[INFO] Vérifiez que les données sont correctes" -ForegroundColor Yellow
    }
    
} catch {
    Write-Host "`n[ERROR] Exception lors du restore : $_" -ForegroundColor Red
    exit 1
}

# Nettoyer variable mot de passe
Remove-Item env:PGPASSWORD -ErrorAction SilentlyContinue

Write-Host "`n[OK] Restore terminé !`n" -ForegroundColor Green
Write-Host "[INFO] Vérifiez l'intégrité des données avec :`n" -ForegroundColor Yellow
Write-Host "  psql -U postgres -d etl_db -c 'SELECT COUNT(*) FROM ods.ods_client;'" -ForegroundColor Gray
Write-Host ""
