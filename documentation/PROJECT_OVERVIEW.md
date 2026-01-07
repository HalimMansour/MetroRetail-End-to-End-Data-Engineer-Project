# MetroRetail Data Pipeline - Project Overview

A comprehensive data pipeline for retail analytics using Apache Airflow, dbt, and SQL Server.

## 🏗️ Architecture

```
Raw Layer (CSV) → Staging Layer (dbt) → Silver Layer (dbt) → Gold Layer (dbt) → Analytics
```

### Key Components

- **Airflow**: Orchestration & scheduling
- **dbt**: SQL transformation framework
- **SQL Server**: Data warehouse
- **Python**: Data ingestion & utilities

---

## 📁 Project Structure

```
MetroRetail/
├── dags/                          # Airflow DAGs
│   ├── metro_retail_pipeline.py   # Main pipeline DAG
│   └── config/
│       └── dag_config.py          # DAG configuration
│
├── pipelines/                     # Python ingestion scripts
│   ├── pull_weather_data.py       # Fetch weather from API
│   ├── ingest_csv.py              # Load CSV to Raw layer
│   ├── config.py                  # Database config
│   ├── db_utils.py                # Database utilities
│   └── schema.py                  # Data models
│
├── dbt/metro_dbt/                 # dbt project
│   ├── models/
│   │   ├── staging/               # Staging models (cleanse)
│   │   ├── silver/                # Silver models (aggregate)
│   │   └── gold/                  # Gold models (analytics-ready)
│   ├── macros/                    # dbt macros
│   ├── profiles.yml               # dbt configuration
│   └── dbt_project.yml            # dbt project config
│
├── sqlserver/                     # SQL Server DDL
│   ├── 01_create_schemas.sql      # Create Raw, Staging, Silver, Gold schemas
│   ├── 02_create_raw_tables_ddl.sql
│   ├── 03_test_load.sql
│   ├── 04_create_staging_tables_ddl.sql
│   ├── 05_stging_layer_template.sql
│   ├── 06_create_silver_tables_ddl.sql
│   ├── 07_create_gold_tables_ddl.sql
│   └── validation_checklist/
│       └── master_staging_validation.sql
│
├── data/
│   └── sample/                    # Sample CSV files
│       ├── erp_products.csv
│       ├── erp_stores.csv
│       ├── erp_inventory.csv
│       ├── crm_customers.csv
│       ├── mkt_promotions.csv
│       ├── pos_transactions_header.csv
│       ├── pos_transactions_lines.csv
│       └── api_weather.csv
│
├── airflow_home/                  # Airflow configuration
│   ├── airflow.cfg                # Airflow settings
│   └── dags/                      # Symbolic link to dags/
│
├── logs/                          # Pipeline logs
│
├── requirements.txt               # Python dependencies
├── .env.sample                    # Environment variables template
├── README.md                      # Quick start guide
├── WSL2_SETUP.md                  # WSL2 setup instructions
├── start_airflow_wsl2.ps1         # Start Airflow in WSL2
├── stop_airflow_wsl2.ps1          # Stop Airflow in WSL2
└── init_airflow.sh                # Initialize Airflow environment
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- SQL Server Express or Standard
- WSL2 (recommended for Airflow on Windows)
- Windows ODBC Driver 17 for SQL Server

### 1. Setup Environment

```powershell
# Clone/download the project
cd C:\Work\Projects\MetroRetail

# Create virtual environment
python -m venv .venv

# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Database

Update `.env` with your SQL Server details:
```
SQL_SERVER=10.255.255.254\SQLEXPRESS
SQL_DATABASE=MetroRetailDB
SQL_USERNAME=halim
SQL_PASSWORD=Halim@1999!
```

Run SQL Server setup scripts:
```powershell
# Execute scripts in order (in SQL Server Management Studio)
sqlserver/01_create_schemas.sql
sqlserver/02_create_raw_tables_ddl.sql
sqlserver/04_create_staging_tables_ddl.sql
sqlserver/06_create_silver_tables_ddl.sql
sqlserver/07_create_gold_tables_ddl.sql
```

### 3. Start Airflow (WSL2)

```powershell
# Start Airflow in WSL2 Ubuntu
.\start_airflow_wsl2.ps1

# Access Airflow UI at: http://172.29.83.242:8080
# Username: admin
# Password: admin123
```

### 4. Run the Pipeline

- Go to Airflow UI
- Find `metro_retail_pipeline` DAG
- Click "Trigger DAG" or manually trigger tasks

---

## 📊 Pipeline Stages

### Stage 1: Pull Weather Data
- Fetches weather from Open-Meteo API
- Saves to `data/sample/api_weather.csv`
- ~28,000 rows of historical weather

### Stage 2: Ingest CSV Files
- Loads all CSV files to SQL Server Raw schema
- Files processed:
  - `erp_products.csv` → Raw.erp_products
  - `erp_stores.csv` → Raw.erp_stores
  - `erp_inventory.csv` → Raw.erp_inventory
  - `crm_customers.csv` → Raw.crm_customers
  - `mkt_promotions.csv` → Raw.mkt_promotions
  - `pos_transactions_header.csv` → Raw.pos_transactions_header
  - `pos_transactions_lines.csv` → Raw.pos_transactions_lines
  - `api_weather.csv` → Raw.api_weather

### Stage 3-5: dbt Transformations
- **Staging**: Data cleansing, standardization, type conversion
- **Silver**: Business logic, aggregations, joins
- **Gold**: Star schema for analytics (facts & dimensions)

### Stage 6: Data Quality Checks
- Validates row counts
- Confirms all layers processed successfully

---

## 📚 Key Files

### DAG Configuration
**[dags/config/dag_config.py](dags/config/dag_config.py)**
- DAG schedule interval
- Default arguments
- Paths to dbt, pipelines, data

### Database Utilities
**[pipelines/db_utils.py](pipelines/db_utils.py)**
- Connection management
- Bulk insert operations
- Manifest tracking

### dbt Models
- **Staging**: Clean raw data, fix data types
- **Silver**: Aggregate, join related tables
- **Gold**: Create fact/dimension tables for BI tools

### SQL Server
- Raw schema: Load staging area
- Staging schema: Cleansed data
- Silver schema: Business-ready aggregations
- Gold schema: Star schema analytics layer

---

## 🔧 Configuration

### Environment Variables (.env)
```
SQL_SERVER=10.255.255.254\SQLEXPRESS
SQL_DATABASE=MetroRetailDB
SQL_USERNAME=halim
SQL_PASSWORD=your_password
```

### Airflow Settings (airflow_home/airflow.cfg)
```
executor = SequentialExecutor
dags_folder = /mnt/c/Work/Projects/MetroRetail/dags
database = sqlite
```

### dbt Profiles (dbt/metro_dbt/profiles.yml)
```yaml
metro_dbt:
  target: dev
  outputs:
    dev:
      type: sqlserver
      server: 10.255.255.254
      database: MetroRetailDB
      schema: Staging
```

---

## 📊 Data Model

### Staging Layer (SQL Server)
- Clean, standardized versions of raw data
- Data type conversions
- Null handling
- Deduplication

### Silver Layer
- Business logic applied
- Aggregations calculated
- Related data joined
- Performance optimized

### Gold Layer (Star Schema)
**Fact Tables:**
- `fact_sales`: Transaction details with amounts, quantities
- `fact_inventory_snapshot`: Inventory levels over time

**Dimension Tables:**
- `dim_customer`: Customer attributes
- `dim_product`: Product catalog
- `dim_store`: Store locations
- `dim_date`: Date dimension (time intelligence)
- `dim_promotion`: Promotion details
- `dim_weather`: Weather conditions by date/location
- `bridge_promotion_product`: Many-to-many relationship

---

## 🛠️ Troubleshooting

### Issue: DAG not appearing in Airflow
**Solution:** Check dags_folder path in airflow.cfg is correct for WSL2

### Issue: Database connection timeout
**Solution:** WSL2 can't reach Windows SQL Server directly. Use Windows host IP (10.255.255.254) or enable TCP/IP in SQL Server

### Issue: dbt models failing
**Solution:** Verify dbt/metro_dbt/profiles.yml has correct server, database, and credentials

### Issue: CSV ingestion fails
**Solution:** Ensure ODBC Driver 17 is installed in WSL2:
```bash
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

---

## 📈 Monitoring

### Airflow UI
- Check DAG run history: http://172.29.83.242:8080/dags/metro_retail_pipeline
- View task logs: Click task → Logs tab
- Check task status: Green = Success, Red = Failed

### Logs
```
logs/scheduler.log        # Scheduler logs
logs/webserver.log        # Webserver logs
airflow_home/logs/        # Task logs
```

---

## 🎯 Best Practices

1. **Always use WSL2** for Airflow on Windows
2. **Run dbt commands** from dbt/metro_dbt/ directory
3. **Test CSV files** before large ingestions
4. **Monitor dbt model lineage** in dbt/metro_dbt/target/graph.gpickle
5. **Validate data** after each stage completes
6. **Keep .env secure** - don't commit to Git

---

## 📝 Next Steps

1. ✅ Verify all tasks turn green in Airflow
2. ✅ Query Gold schema tables from SQL Server
3. ✅ Connect BI tool (Power BI, Tableau) to Gold schema
4. ✅ Add custom dbt models for your analytics needs
5. ✅ Schedule pipeline with cron or Airflow scheduler

---

## 📞 Support

For issues or questions:
1. Check logs in `logs/` directory
2. Review dbt documentation: https://docs.getdbt.com
3. Check Airflow documentation: https://airflow.apache.org
4. Verify SQL Server connectivity with SSMS

---

**Last Updated:** January 7, 2026
