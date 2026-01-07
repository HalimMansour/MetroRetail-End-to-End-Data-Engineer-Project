# =====================================================
# Start Airflow Services via WSL2
# Run from Windows PowerShell
# =====================================================

Write-Host ""
Write-Host "Starting Airflow Services via WSL2..." -ForegroundColor Green
Write-Host ""

# Get the current directory (Windows path)
$windowsPath = Get-Location
$wslPath = "/mnt/c" + ($windowsPath.Path.Substring(2) -replace '\\', '/')

# Start Airflow in WSL2
Write-Host "Starting services in WSL2..." -ForegroundColor Yellow

wsl -d Ubuntu bash -c "cd '$wslPath' && chmod +x start_airflow.sh && ./start_airflow.sh"

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "[SUCCESS] Airflow Started!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Access Airflow:" -ForegroundColor Yellow
    Write-Host "  URL: http://localhost:8080" -ForegroundColor Cyan
    Write-Host "  Username: admin" -ForegroundColor Cyan
    Write-Host "  Password: admin123" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "WSL2 terminal will remain open." -ForegroundColor Yellow
    Write-Host "To stop Airflow, press Ctrl+C or run: .\stop_airflow_wsl2.ps1" -ForegroundColor Yellow
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "ERROR: Failed to start Airflow" -ForegroundColor Red
    Write-Host "Run setup_wsl2.ps1 first if you haven't already" -ForegroundColor Yellow
    exit 1
}
