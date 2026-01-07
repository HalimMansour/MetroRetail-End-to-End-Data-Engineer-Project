# Quick Reference Guide

## 📍 Key Commands

### Start/Stop Services

```powershell
# Start Airflow in WSL2
.\start_airflow_wsl2.ps1

# Stop Airflow
.\stop_airflow_wsl2.ps1

# Access Airflow UI
# http://172.29.83.242:8080
# Username: admin | Password: admin123
```

### Activate Python Environment

```powershell
# Windows PowerShell
.\.venv\Scripts\Activate.ps1

# Deactivate
deactivate
```

### dbt Commands

```bash
# Parse models (validate syntax)
dbt parse

# Run all models
dbt run

# Run specific layer
dbt run --select staging.*
dbt run --select silver.*
dbt run --select gold.*

# Run specific model
dbt run --select dim_customer

# Test models
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Airflow Commands

```bash
# List all DAGs
airflow dags list

# Trigger DAG
airflow dags trigger metro_retail_pipeline

# View DAG details
airflow dags show metro_retail_pipeline

# View task logs
airflow tasks logs metro_retail_pipeline pull_weather_data
```

### SQL Server Queries

```sql
-- Check data in each layer
SELECT COUNT(*) FROM Raw.erp_products;
SELECT COUNT(*) FROM Staging.stg_erp_products_clean;
SELECT COUNT(*) FROM Silver.*_clean;
SELECT COUNT(*) FROM Gold.fact_sales;

-- View recent transactions
SELECT TOP 100 * FROM Gold.fact_sales 
ORDER BY transaction_id DESC;

-- Check data quality
SELECT 
  'Products' AS TableName,
  COUNT(*) AS RecordCount,
  COUNT(DISTINCT product_id) AS UniqueProducts
FROM Raw.erp_products;
```

---

## 📂 Important Directories

| Path | Purpose |
|------|---------|
| `dags/` | Airflow DAG definitions |
| `pipelines/` | Python ingestion scripts |
| `dbt/metro_dbt/` | dbt project (models, tests, macros) |
| `data/sample/` | Sample CSV data files |
| `sqlserver/` | SQL Server DDL scripts |
| `airflow_home/` | Airflow configuration |
| `logs/` | Pipeline execution logs |

---

## 🔐 Configuration Files

### `.env` - Database Credentials
```
SQL_SERVER=10.255.255.254\SQLEXPRESS
SQL_DATABASE=MetroRetailDB
SQL_USERNAME=halim
SQL_PASSWORD=Halim@1999!
```

### `dbt/metro_dbt/profiles.yml` - dbt Config
```yaml
metro_dbt:
  target: dev
  outputs:
    dev:
      type: sqlserver
      server: 10.255.255.254
      database: MetroRetailDB
      schema: Staging
      authentication: sql
      user: halim
      password: Halim@1999!
```

### `airflow_home/airflow.cfg` - Airflow Config
```
[core]
dags_folder = /mnt/c/Work/Projects/MetroRetail/dags
executor = SequentialExecutor

[database]
sql_alchemy_conn = sqlite:////mnt/c/Work/Projects/MetroRetail/airflow_home/airflow.db
```

---

## 🚀 Typical Workflow

### 1. Development
```bash
# Activate venv
.\.venv\Scripts\Activate.ps1

# Edit dbt models
code dbt/metro_dbt/models/gold/fact_sales.sql

# Test locally
cd dbt/metro_dbt
dbt run --select fact_sales
dbt test --select fact_sales
```

### 2. Deploy
```powershell
# Start Airflow
.\start_airflow_wsl2.ps1

# Go to http://172.29.83.242:8080
# Trigger metro_retail_pipeline DAG
# Monitor execution
```

### 3. Verify
```sql
-- SQL Server
SELECT COUNT(*) FROM Gold.fact_sales;
SELECT TOP 5 * FROM Gold.fact_sales;
```

---

## 📊 Pipeline Stages & Time

| Stage | Task | Duration | Status |
|-------|------|----------|--------|
| 1 | pull_weather_data | ~10s | ✅ |
| 2 | ingest_csv_files | ~5s | ✅ |
| 3 | dbt_staging | ~2s | ✅ |
| 4 | dbt_silver | ~2s | ✅ |
| 5 | dbt_gold | ~2s | ✅ |
| 6 | data_quality_checks | ~2s | ✅ |
| **Total** | | **~25s** | **🟢 Success** |

---

## 🔍 Log Locations

```
logs/scheduler.log          # Airflow scheduler
logs/webserver.log          # Airflow webserver
airflow_home/logs/          # Task-specific logs

# Example: View last 50 lines of scheduler
wsl -d Ubuntu bash -c "tail -50 logs/scheduler.log"
```

---

## 🆘 Quick Troubleshooting

| Issue | Quick Fix |
|-------|-----------|
| DAG not visible | Restart Airflow scheduler (stop + start) |
| DB connection failed | Check TCP/IP enabled + verify IP in config |
| CSV not found | Verify files in `data/sample/` directory |
| dbt import error | `pip install --upgrade dbt-sqlserver dbt-fabric` |
| Permission denied | `chmod +x script.sh` in WSL2 |
| Port 8080 in use | `taskkill /PID <PID> /F` or change port |

---

## 📞 Support Resources

**Airflow Issues:**
- Docs: https://airflow.apache.org
- GitHub: https://github.com/apache/airflow
- Slack: https://apache-airflow.slack.com

**dbt Issues:**
- Docs: https://docs.getdbt.com
- Slack: https://community.getdbt.com

**SQL Server Issues:**
- Docs: https://learn.microsoft.com/en-us/sql
- Stack Overflow: tag `sql-server`

---

## 📝 File Tree

```
MetroRetail/
├── README.md                    # Main readme
├── PROJECT_OVERVIEW.md          # This file
├── ARCHITECTURE.md              # System design
├── INSTALLATION.md              # Setup guide
├── QUICK_REFERENCE.md           # Quick commands
├── .env.sample                  # Config template
├── requirements.txt             # Python packages
├── dags/
│   ├── metro_retail_pipeline.py # Main DAG
│   ├── config/
│   │   └── dag_config.py        # DAG settings
│   └── utils/
│       └── __init__.py
├── pipelines/
│   ├── pull_weather_data.py     # Weather fetch
│   ├── ingest_csv.py            # CSV loader
│   ├── db_utils.py              # DB utilities
│   ├── config.py                # DB config
│   └── schema.py                # Data models
├── dbt/metro_dbt/
│   ├── dbt_project.yml          # dbt config
│   ├── profiles.yml             # dbt profiles
│   ├── models/
│   │   ├── staging/             # Staging layer
│   │   ├── silver/              # Silver layer
│   │   └── gold/                # Gold layer
│   ├── macros/                  # dbt macros
│   └── tests/                   # dbt tests
├── sqlserver/
│   ├── 01_create_schemas.sql    # Schemas
│   ├── 02_create_raw_tables_ddl.sql
│   ├── ...
│   └── 07_create_gold_tables_ddl.sql
├── data/
│   └── sample/                  # Sample CSVs
│       ├── erp_products.csv
│       ├── erp_stores.csv
│       ├── crm_customers.csv
│       ├── mkt_promotions.csv
│       ├── pos_transactions_*.csv
│       └── api_weather.csv
├── airflow_home/
│   ├── airflow.cfg              # Airflow config
│   └── webserver_config.py      # UI config
├── logs/                        # Pipeline logs
├── start_airflow_wsl2.ps1       # Start script
├── stop_airflow_wsl2.ps1        # Stop script
└── init_airflow.sh              # Init script
```

---

## ⚡ Performance Tips

1. **Run dbt selectively:** Only run models you changed
   ```bash
   dbt run --select fact_sales
   ```

2. **Use dbt test in CI/CD:** Test automatically
   ```bash
   dbt test
   ```

3. **Rebuild docs locally:** Before pushing
   ```bash
   dbt docs generate
   ```

4. **Check dependencies:** Graph models
   ```bash
   dbt docs serve
   ```

5. **Monitor data quality:** Add tests
   ```sql
   -- tests/fact_sales_not_null.sql
   select * from {{ ref('fact_sales') }}
   where transaction_id is null
   ```

---

## 🎓 Learning Path

1. **Understand Data Flow**
   - Read: PROJECT_OVERVIEW.md
   - Run: First DAG pipeline

2. **Learn Architecture**
   - Read: ARCHITECTURE.md
   - Query: SQL Server schemas

3. **Explore Code**
   - Read: DAG definition (dags/metro_retail_pipeline.py)
   - Read: dbt models (dbt/metro_dbt/models/)

4. **Customize**
   - Add new CSV sources
   - Create new dbt models
   - Add tests & macros

5. **Deploy**
   - Set up production database
   - Configure backups
   - Monitor execution

---

**Last Updated:** January 7, 2026
