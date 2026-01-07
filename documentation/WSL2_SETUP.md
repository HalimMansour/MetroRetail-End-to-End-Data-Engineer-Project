# WSL2 Setup Guide for MetroRetail Data Pipeline

This guide sets up the MetroRetail project to run on **Windows Subsystem for Linux 2 (WSL2)**, which provides native Linux environment support for Apache Airflow.

## Prerequisites

- Windows 10 version 2004 or later (or any Windows 11)
- At least 4GB RAM available for WSL2
- Administrator access to run PowerShell

## Installation Steps

### Step 1: Install WSL2

Open PowerShell as Administrator and run:

```powershell
# Enable WSL
wsl --install

# Set WSL2 as default
wsl --set-default-version 2

# Install Ubuntu 22.04 (recommended)
wsl --install Ubuntu-22.04
```

This will download and install Ubuntu. When prompted, create a username and password.

### Step 2: Verify WSL2 Installation

```powershell
# Check WSL version
wsl --list --verbose

# You should see:
# NAME           STATE           VERSION
# Ubuntu-22.04   Running         2
```

### Step 3: Initialize WSL2 Environment

From PowerShell, run the provided setup script:

```powershell
# This will set up Python, dependencies, and Airflow in WSL2
.\setup_wsl2.ps1
```

This script will:
- Update WSL2 Ubuntu packages
- Install Python 3.10+
- Install system dependencies (libpq, ODBC drivers, etc.)
- Create Python virtual environment
- Install project dependencies
- Initialize Airflow database and admin user

### Step 4: Start Airflow

Once setup completes, start Airflow with:

```powershell
# From Windows PowerShell
.\start_airflow_wsl2.ps1
```

This will:
- Open WSL2 Ubuntu terminal
- Activate virtual environment
- Start Airflow scheduler and webserver
- Open browser to http://localhost:8080

## Manual WSL2 Usage

If you prefer to manage WSL2 directly:

```powershell
# Enter WSL2 Ubuntu
wsl

# Or open distro directly
wsl --distribution Ubuntu-22.04
```

Once in WSL2:

```bash
# Navigate to project
cd /mnt/c/Work/Projects/MetroRetail

# Activate virtual environment
source .venv/bin/activate

# Initialize Airflow (one-time)
./init_airflow.sh

# Start Airflow
./start_airflow.sh
```

## File Paths in WSL2

Windows paths are accessible in WSL2 using the `/mnt` drive:

| Windows Path | WSL2 Path |
|---|---|
| `C:\Work\Projects\MetroRetail` | `/mnt/c/Work/Projects/MetroRetail` |
| `.env` file | `/mnt/c/Work/Projects/MetroRetail/.env` |
| Database backups | `/mnt/c/Work/Projects/MetroRetail/data` |

## Airflow Access

- **UI**: http://localhost:8080
- **Username**: admin
- **Password**: admin123

## Troubleshooting

### WSL2 not starting
```powershell
# Reset WSL
wsl --shutdown
wsl --unregister Ubuntu-22.04
wsl --install Ubuntu-22.04
```

### Permission denied errors
```bash
# Inside WSL, fix file permissions
chmod +x *.sh
sudo chown -R $USER:$USER .
```

### SQL Server connection issues
Ensure SQL Server is accessible from WSL2. If using local SQL Server:
```bash
# Test connection from WSL
sqlcmd -S localhost -U sa -P YourPassword
```

### Airflow port already in use
```bash
# Find and kill process using port 8080
lsof -i :8080
kill -9 <PID>
```

## VSCode Integration (Optional)

For seamless WSL2 integration in VSCode:

1. Install "Remote - WSL" extension
2. Open Command Palette (Ctrl+Shift+P)
3. Type "Remote-WSL: Reopen Folder in WSL"
4. Select the MetroRetail folder
5. VSCode will now work directly in WSL2

## Next Steps

1. Follow the installation steps above
2. Run the setup script
3. Start Airflow
4. Access http://localhost:8080 in your browser
5. Create/monitor data pipelines in Airflow UI

## Additional Resources

- [WSL2 Documentation](https://learn.microsoft.com/en-us/windows/wsl/)
- [Apache Airflow Installation](https://airflow.apache.org/docs/apache-airflow/stable/installation/)
- [WSL2 Networking Guide](https://learn.microsoft.com/en-us/windows/wsl/networking)
