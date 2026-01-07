# =====================================================
# Stop Airflow Services via WSL2
# Run from Windows PowerShell
# =====================================================

Write-Host ""
Write-Host "Stopping Airflow Services via WSL2..." -ForegroundColor Yellow
Write-Host ""

# Get the current directory (Windows path)
$windowsPath = Get-Location
$wslPath = "/mnt/c" + ($windowsPath.Path.Substring(2) -replace '\\', '/')

# Stop Airflow in WSL2
wsl -d Ubuntu bash -c "cd '$wslPath' && chmod +x stop_airflow.sh && ./stop_airflow.sh"

Write-Host ""
Write-Host "[SUCCESS] Airflow Stopped!" -ForegroundColor Green
Write-Host ""
