# Installation & Troubleshooting Guide

## 🔧 Prerequisites Checklist

- [ ] Windows 10/11 with WSL2 enabled
- [ ] Python 3.10+ installed on Windows
- [ ] SQL Server 2019+ Express/Standard
- [ ] SQL Server Management Studio (SSMS)
- [ ] Visual C++ Redistributable
- [ ] Administrator access to Windows

---

## 📦 Step 1: WSL2 Setup

### Install WSL2 (if not already installed)

Open PowerShell as Administrator:

```powershell
# Enable WSL2
wsl --install

# Set WSL2 as default
wsl --set-default-version 2

# Install Ubuntu 22.04
wsl --install -d Ubuntu-22.04
```

Verify installation:
```powershell
wsl --list --verbose
# Output should show Ubuntu-22.04 with VERSION 2
```

---

## 🗄️ Step 2: SQL Server Setup

### Create Database and Schemas

1. Open **SQL Server Management Studio (SSMS)**
2. Connect to your SQL Server instance
3. Execute scripts in order:

```sql
-- 01_create_schemas.sql
CREATE DATABASE MetroRetailDB;
GO

USE MetroRetailDB;
GO

CREATE SCHEMA Raw;
CREATE SCHEMA Staging;
CREATE SCHEMA Silver;
CREATE SCHEMA Gold;
GO
```

4. Continue with remaining DDL scripts (02-07)

### Enable TCP/IP (for WSL2 access)

1. Open **SQL Server Configuration Manager**
2. Navigate to: SQL Server Network Configuration → Protocols for SQLEXPRESS
3. Enable **TCP/IP**
4. Right-click TCP/IP → Properties
5. Set Port: 1433 (default)
6. Restart SQL Server service

### Create Non-Admin User

```sql
-- Create login
CREATE LOGIN halim WITH PASSWORD = 'Halim@1999!';
GO

-- Create user in MetroRetailDB
USE MetroRetailDB;
CREATE USER halim FOR LOGIN halim;
GO

-- Grant permissions
ALTER ROLE db_datareader ADD MEMBER halim;
ALTER ROLE db_datawriter ADD MEMBER halim;
ALTER ROLE db_owner ADD MEMBER halim;
GO
```

---

## 🐍 Step 3: Python Environment Setup

### Create Virtual Environment

```powershell
cd C:\Work\Projects\MetroRetail

# Create virtual environment
python -m venv .venv

# Activate
.\.venv\Scripts\Activate.ps1

# Upgrade pip
python -m pip install --upgrade pip
```

### Install Dependencies

```powershell
pip install -r requirements.txt

# Install ODBC driver for WSL2 (in WSL2)
wsl -d Ubuntu bash -c "sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17"
```

---

## 🌬️ Step 4: Airflow in WSL2

### Initialize Airflow Database

```powershell
# Run from Windows PowerShell
.\init_airflow.sh

# Or manually in WSL2
wsl -d Ubuntu bash -c "cd /mnt/c/Work/Projects/MetroRetail && airflow db init"
```

### Create Admin User

```powershell
wsl -d Ubuntu bash -c "cd /mnt/c/Work/Projects/MetroRetail && airflow users create --username admin --password admin123 --firstname Admin --lastname User --role Admin --email admin@example.com"
```

---

## 🚀 Step 5: Start Services

### Start Airflow

```powershell
.\start_airflow_wsl2.ps1
```

Wait for output:
```
✓ Airflow services started!
  URL: http://172.29.83.242:8080
  Username: admin
  Password: admin123
```

### Access Airflow UI

Open browser: **http://172.29.83.242:8080**

---

## 🧪 Step 6: Test Data Pipeline

### Trigger DAG

1. Go to Airflow UI
2. Find `metro_retail_pipeline` DAG
3. Click "Trigger DAG"
4. Monitor execution in "Graph" view

### Expected Stages

1. ✅ `pull_weather_data` - Green in ~10 seconds
2. ✅ `ingest_csv_files` - Green in ~5 seconds
3. ✅ `dbt_staging` - Green in ~2 seconds
4. ✅ `dbt_silver` - Green in ~2 seconds
5. ✅ `dbt_gold` - Green in ~2 seconds
6. ✅ `data_quality_checks` - Green in ~2 seconds

---

## 🐛 Troubleshooting

### Issue 1: WSL2 Not Found

**Error:** `The term 'wsl' is not recognized`

**Solution:**
```powershell
# Check WSL installation
wsl --list --verbose

# If not installed, run as admin:
wsl --install -d Ubuntu-22.04
```

---

### Issue 2: Database Connection Failed

**Error:** `Login timeout expired` or `Cannot connect to SQL Server`

**Diagnosis:**
```powershell
# Test connectivity from WSL2
wsl -d Ubuntu bash -c "nc -zv 10.255.255.254 1433"
```

**Solutions:**
1. Verify TCP/IP enabled in SQL Server Configuration Manager
2. Check firewall allows SQL Server (port 1433)
3. Verify Windows host IP:
   ```powershell
   wsl -d Ubuntu bash -c "cat /etc/resolv.conf | grep nameserver"
   ```
4. Update `pipelines/config.py` with correct IP
5. Update `dbt/metro_dbt/profiles.yml` with correct IP

---

### Issue 3: ODBC Driver Not Found

**Error:** `Can't open lib 'ODBC Driver 17 for SQL Server'`

**Solution:**
```bash
# In WSL2
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
sudo apt-get install -y unixodbc-dev
```

---

### Issue 4: Airflow DAG Not Showing

**Error:** DAG doesn't appear in Airflow UI after 5 minutes

**Diagnosis:**
```powershell
# Check dags folder in Airflow
wsl -d Ubuntu bash -c "grep 'dags_folder' /mnt/c/Work/Projects/MetroRetail/airflow_home/airflow.cfg"

# Check if DAG file is readable
wsl -d Ubuntu bash -c "ls -la /mnt/c/Work/Projects/MetroRetail/dags/"
```

**Solution:**
1. Verify path is `/mnt/c/Work/Projects/MetroRetail/dags` (not Windows path)
2. Restart Airflow scheduler:
   ```powershell
   .\stop_airflow_wsl2.ps1
   # Wait 30 seconds
   .\start_airflow_wsl2.ps1
   ```

---

### Issue 5: CSV Ingestion Task Fails

**Error:** `File not found` or `No rows inserted`

**Diagnostic:**
```powershell
# Check if CSV files exist
wsl -d Ubuntu bash -c "ls -la /mnt/c/Work/Projects/MetroRetail/data/sample/"

# Test ingestion manually
wsl -d Ubuntu bash -c "cd /mnt/c/Work/Projects/MetroRetail && /mnt/c/Work/Projects/MetroRetail/.venv/bin/python pipelines/ingest_csv.py --all"
```

**Solutions:**
1. Verify CSV files in `data/sample/` directory
2. Check column names match expected format
3. Verify database credentials in `.env`

---

### Issue 6: dbt Models Failing

**Error:** `cannot import name...` or `connection timeout`

**Diagnostic:**
```bash
# In WSL2
cd /mnt/c/Work/Projects/MetroRetail/dbt/metro_dbt
/mnt/c/Work/Projects/MetroRetail/.venv/bin/dbt debug
```

**Solutions:**
1. Update dbt profiles:
   ```bash
   /mnt/c/Work/Projects/MetroRetail/.venv/bin/pip install --upgrade dbt-sqlserver
   ```
2. Verify profiles.yml has correct credentials
3. Test connection:
   ```bash
   /mnt/c/Work/Projects/MetroRetail/.venv/bin/dbt run --select dim_date --profiles-dir .
   ```

---

### Issue 7: Permission Denied in WSL2

**Error:** `Permission denied` when executing scripts

**Solution:**
```bash
# Make script executable
chmod +x /mnt/c/Work/Projects/MetroRetail/start_airflow.sh

# Run with bash explicitly
bash /mnt/c/Work/Projects/MetroRetail/start_airflow.sh
```

---

### Issue 8: Port Already in Use

**Error:** `Address already in use :8080`

**Solution:**
```powershell
# Kill existing process
netstat -ano | findstr :8080
taskkill /PID <PID> /F

# Or change port in airflow.cfg
# webserver_port = 8081
```

---

## 📊 Verification Commands

### Check Airflow Status

```powershell
# Check if services running
wsl -d Ubuntu bash -c "ps aux | grep airflow"

# Check logs
wsl -d Ubuntu bash -c "tail -50 /mnt/c/Work/Projects/MetroRetail/logs/scheduler.log"
```

### Check SQL Server Connection

```powershell
# Test from WSL2
wsl -d Ubuntu bash -c "python -c \"import pyodbc; print(pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=10.255.255.254;Database=MetroRetailDB;UID=halim;PWD=Halim@1999!'))\""
```

### Check dbt Installation

```bash
# In WSL2
/mnt/c/Work/Projects/MetroRetail/.venv/bin/dbt --version
```

### Check Airflow DAGs

```bash
# In WSL2
/mnt/c/Work/Projects/MetroRetail/.venv/bin/airflow dags list
```

---

## 🔄 Common Workflows

### Run Pipeline Manually

```powershell
# Start Airflow
.\start_airflow_wsl2.ps1

# Open browser: http://172.29.83.242:8080
# Click: metro_retail_pipeline DAG
# Click: Trigger DAG
# Monitor in Graph view
```

### Run dbt Specific Models

```bash
# In WSL2
cd /mnt/c/Work/Projects/MetroRetail/dbt/metro_dbt

# Run all staging models
/mnt/c/Work/Projects/MetroRetail/.venv/bin/dbt run --select staging.*

# Run specific model
/mnt/c/Work/Projects/MetroRetail/.venv/bin/dbt run --select dim_customer

# Run with tests
/mnt/c/Work/Projects/MetroRetail/.venv/bin/dbt run --select dim_customer --tests
```

### Inspect Raw Data

```sql
-- SQL Server
SELECT TOP 10 * FROM Raw.erp_products;
SELECT TOP 10 * FROM Raw.pos_transactions_header;
```

### Check Data Quality

```sql
-- Count records in each layer
SELECT 
  'Raw' AS Layer, 
  COUNT(*) AS RecordCount 
FROM Raw.pos_transactions_header
UNION ALL
SELECT 
  'Staging',
  COUNT(*)
FROM Staging.stg_pos_transactions_header_clean
UNION ALL
SELECT
  'Gold',
  COUNT(*)
FROM Gold.fact_sales;
```

---

## 🎯 First-Time Setup Checklist

- [ ] WSL2 installed and Ubuntu running
- [ ] SQL Server running with MetroRetailDB
- [ ] Non-admin user created (halim)
- [ ] TCP/IP enabled in SQL Server
- [ ] Python virtual environment created
- [ ] Dependencies installed
- [ ] ODBC Driver 17 installed in WSL2
- [ ] Airflow initialized
- [ ] Airflow admin user created
- [ ] Airflow services started
- [ ] DAG visible in Airflow UI
- [ ] First DAG run completed successfully
- [ ] Data verified in SQL Server

---

## 📚 Additional Resources

- **Airflow Docs:** https://airflow.apache.org
- **dbt Docs:** https://docs.getdbt.com
- **SQL Server Docs:** https://learn.microsoft.com/en-us/sql
- **WSL2 Setup:** https://docs.microsoft.com/en-us/windows/wsl
- **Open-Meteo API:** https://open-meteo.com

---

**Last Updated:** January 7, 2026
**Support Email:** your-email@example.com
