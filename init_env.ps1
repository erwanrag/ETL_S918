# E:\Prefect\projects\ETL\init_env.ps1

Write-Host "Chargement des variables depuis .env ..." -ForegroundColor Cyan
$envFile = "$PSScriptRoot\.env"

if (Test-Path $envFile) {
    Get-Content $envFile | Where-Object { $_ -notmatch "^#" -and $_ -match "=" } | ForEach-Object {
        $name, $value = $_.Split('=', 2)
        # Nettoyer les espaces et guillemets éventuels
        $name = $name.Trim()
        $value = $value.Trim().Trim('"').Trim("'")
        
        # Définir la variable dans la session actuelle
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
    Write-Host "Variables d'environnement chargées avec succès !" -ForegroundColor Green
} else {
    Write-Host "Fichier .env introuvable : $envFile" -ForegroundColor Red
}