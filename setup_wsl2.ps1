# =====================================================
# Setup WSL2 for MetroRetail Data Pipeline
# Run from Windows PowerShell (Administrator)
# =====================================================

Write-Host ""
Write-Host "Setting up WSL2 for MetroRetail Project..." -ForegroundColor Green
Write-Host ""

# Check if WSL is installed
$wslCheck = wsl --list --verbose 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: WSL2 not installed or not accessible" -ForegroundColor Red
    Write-Host "Please install WSL2 first:" -ForegroundColor Yellow
    Write-Host "  1. Open PowerShell as Administrator" -ForegroundColor White
    Write-Host "  2. Run: wsl --install" -ForegroundColor White
    Write-Host "  3. Restart your computer" -ForegroundColor White
    exit 1
}

Write-Host "Checking WSL installation..." -ForegroundColor Yellow
Write-Host $wslCheck -ForegroundColor Cyan
Write-Host ""

# Get the current directory (Windows path)
$windowsPath = Get-Location
$wslPath = "/mnt/c" + ($windowsPath.Path.Substring(2) -replace '\\', '/')

Write-Host "Project path: $windowsPath" -ForegroundColor Cyan
Write-Host "WSL2 path: $wslPath" -ForegroundColor Cyan
Write-Host ""

# Run initialization in WSL2
Write-Host "Initializing Airflow in WSL2..." -ForegroundColor Yellow
Write-Host "This will take a few minutes..." -ForegroundColor Yellow
Write-Host ""

# Convert Windows path to WSL path for init script
$initScript = "$wslPath/init_airflow.sh"

# Run the bash script in WSL2
wsl -d Ubuntu bash -c "cd '$wslPath' && chmod +x init_airflow.sh && ./init_airflow.sh"

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "[SUCCESS] WSL2 Setup Complete!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "NEXT STEPS:" -ForegroundColor Yellow
    Write-Host "1. Start Airflow:" -ForegroundColor White
    Write-Host "   .\start_airflow_wsl2.ps1" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "2. Or manually in WSL2:" -ForegroundColor White
    Write-Host "   wsl" -ForegroundColor Cyan
    Write-Host "   cd /mnt/c/Work/Projects/MetroRetail" -ForegroundColor Cyan
    Write-Host "   chmod +x start_airflow.sh" -ForegroundColor Cyan
    Write-Host "   ./start_airflow.sh" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "3. Access Airflow:" -ForegroundColor White
    Write-Host "   http://localhost:8080" -ForegroundColor Cyan
    Write-Host "   Login: admin / admin123" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "ERROR: WSL2 setup failed" -ForegroundColor Red
    Write-Host "Check the output above for error details" -ForegroundColor Yellow
    exit 1
}
