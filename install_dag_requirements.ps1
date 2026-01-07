# =====================================================
# Install DAG Requirements (run from Windows)
# =====================================================

Write-Host ""
Write-Host "Installing Airflow DAG Requirements..." -ForegroundColor Green
Write-Host ""

$wslPath = "/mnt/c" + ((Get-Location).Path.Substring(2) -replace '\\', '/')

wsl -d Ubuntu bash -c "cd '$wslPath' && chmod +x install_dag_requirements.sh && ./install_dag_requirements.sh"

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "================================================" -ForegroundColor Green
    Write-Host "[SUCCESS] All requirements installed!" -ForegroundColor Green
    Write-Host "================================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "1. Start Airflow: .\start_airflow_wsl2.ps1" -ForegroundColor White
    Write-Host "2. Open browser: http://localhost:8080" -ForegroundColor White
    Write-Host "3. Find your DAG: metro_retail_pipeline" -ForegroundColor White
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "ERROR: Installation failed" -ForegroundColor Red
    exit 1
}
