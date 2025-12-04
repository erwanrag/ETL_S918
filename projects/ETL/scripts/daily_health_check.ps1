# E:\Prefect\projects\ETL\scripts\daily_health_check.ps1

$date = Get-Date -Format "yyyy-MM-dd"

# Check dashboard
$result = psql -U postgres -d etl_db -t -c "SELECT * FROM etl_logs.get_daily_summary()"

# Email si échec
if ($result -match "0.*Success Rate") {
    Send-MailMessage -To "admin@example.com" -Subject "ETL Health Check FAIL" -Body $result
}

Write-Host $result