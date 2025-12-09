# ============================================================================
# Script PowerShell : Nettoyage Logs Fichiers Prefect
# ============================================================================
# Objectif : Supprimer les anciens fichiers .log dans E:\Prefect\projects\ETL\logs
# Usage    : .\cleanup_prefect_file_logs.ps1 -RetentionDays 30
# ============================================================================

param(
    [int]$RetentionDays = 30,
    [string]$LogDir = "E:\Prefect\projects\ETL\logs"
)

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "NETTOYAGE LOGS FICHIERS PREFECT" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Répertoire : $LogDir" -ForegroundColor Gray
Write-Host "Rétention  : $RetentionDays jours" -ForegroundColor Gray
Write-Host ""

# Vérifier existence répertoire
if (-not (Test-Path $LogDir)) {
    Write-Host "[ERROR] Répertoire introuvable : $LogDir" -ForegroundColor Red
    exit 1
}

# Calculer date limite
$cutoffDate = (Get-Date).AddDays(-$RetentionDays)
Write-Host "[INFO] Suppression fichiers < $($cutoffDate.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor Yellow
Write-Host ""

# Rechercher anciens logs
$oldLogs = Get-ChildItem -Path $LogDir -Filter "*.log" -Recurse | Where-Object {
    $_.LastWriteTime -lt $cutoffDate
}

if ($oldLogs.Count -eq 0) {
    Write-Host "[INFO] Aucun fichier à supprimer" -ForegroundColor Green
    Write-Host "============================================================`n" -ForegroundColor Cyan
    exit 0
}

# Afficher liste
Write-Host "[FOUND] $($oldLogs.Count) fichier(s) à supprimer :" -ForegroundColor Yellow
foreach ($file in $oldLogs) {
    $age = [math]::Round((New-TimeSpan -Start $file.LastWriteTime -End (Get-Date)).TotalDays, 1)
    $size = [math]::Round($file.Length / 1KB, 2)
    Write-Host "  - $($file.Name) ($($size) KB, $($age)j)" -ForegroundColor Gray
}

# Confirmation
Write-Host ""
$confirm = Read-Host "Confirmer la suppression ? (O/N)"
if ($confirm -ne "O" -and $confirm -ne "o") {
    Write-Host "[CANCELLED] Opération annulée" -ForegroundColor Yellow
    exit 0
}

# Suppression
Write-Host ""
Write-Host "[DELETE] Suppression en cours..." -ForegroundColor Yellow
$deletedCount = 0
$totalSize = 0

foreach ($file in $oldLogs) {
    try {
        $totalSize += $file.Length
        Remove-Item $file.FullName -Force
        $deletedCount++
    } catch {
        Write-Host "[ERROR] Impossible de supprimer : $($file.Name)" -ForegroundColor Red
    }
}

# Résumé
$totalSizeMB = [math]::Round($totalSize / 1MB, 2)

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "RÉSUMÉ" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Fichiers supprimés : $deletedCount" -ForegroundColor Green
Write-Host "Espace libéré      : $totalSizeMB MB" -ForegroundColor Green
Write-Host "============================================================`n" -ForegroundColor Cyan

# Stats répertoire après nettoyage
$remainingLogs = Get-ChildItem -Path $LogDir -Filter "*.log" -Recurse
$remainingSize = ($remainingLogs | Measure-Object -Property Length -Sum).Sum / 1MB

Write-Host "[INFO] Logs restants : $($remainingLogs.Count) fichier(s), $([math]::Round($remainingSize, 2)) MB" -ForegroundColor Gray
Write-Host ""