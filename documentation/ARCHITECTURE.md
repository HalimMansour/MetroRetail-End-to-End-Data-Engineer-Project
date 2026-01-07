# Architecture & Design Document

## 🏛️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA PIPELINE FLOW                          │
└─────────────────────────────────────────────────────────────────┘

SOURCES
├─ ERP System (Inventory, Products, Stores)
├─ CRM System (Customers)
├─ Marketing (Promotions)
├─ POS System (Transactions)
└─ Weather API (External data)
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    AIRFLOW ORCHESTRATION                        │
│  (metro_retail_pipeline.py - WSL2 Ubuntu)                       │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    RAW LAYER (Stage 1-2)                        │
│   • Pull Weather Data (API → CSV)                               │
│   • Ingest CSV Files (Python)                                   │
│   • Target: SQL Server Raw Schema                               │
│   Files: pipelines/{pull_weather_data.py, ingest_csv.py}       │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│         STAGING LAYER (Stage 3 - dbt Staging Models)            │
│   • Data Cleansing (remove nulls, handle outliers)              │
│   • Type Conversion (string → numeric)                          │
│   • Standardization (date formats, naming)                      │
│   • Deduplication (remove duplicates)                           │
│   Target: SQL Server Staging Schema                             │
│   Models: dbt/metro_dbt/models/staging/stg_*.sql               │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│        SILVER LAYER (Stage 4 - dbt Silver Models)               │
│   • Business Logic Applied                                      │
│   • Aggregations (sum, count, avg)                              │
│   • Joins (relate dimensions)                                   │
│   • Calculations (margins, rates)                               │
│   Target: SQL Server Silver Schema                              │
│   Models: dbt/metro_dbt/models/silver/*_clean.sql              │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│        GOLD LAYER (Stage 5 - dbt Gold Models)                   │
│   • Star Schema (Facts + Dimensions)                            │
│   • Analytics-Ready Tables                                      │
│   • Optimized for BI Tools                                      │
│   • Fact Tables: fact_sales, fact_inventory_snapshot            │
│   • Dimensions: customers, products, stores, dates, etc.        │
│   Target: SQL Server Gold Schema                                │
│   Models: dbt/metro_dbt/models/gold/dim_*, fact_*              │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│              ANALYTICS & REPORTING (Stage 6)                    │
│   • Data Quality Checks                                         │
│   • BI Tool Connection (Power BI, Tableau)                      │
│   • Ad-hoc Queries                                              │
│   • Dashboards & Reports                                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🗄️ Database Schema Design

### Raw Schema
**Purpose:** Direct copy of source data, minimal transformation

```sql
Raw.erp_products          -- ERP product master
Raw.erp_stores            -- ERP store master
Raw.erp_inventory         -- ERP inventory levels
Raw.crm_customers         -- CRM customer data
Raw.mkt_promotions        -- Marketing promotions
Raw.pos_transactions_header
Raw.pos_transactions_lines
Raw.api_weather           -- Weather data from API
```

**Characteristics:**
- 1:1 mapping with source files
- All columns preserved
- Minimal validation
- Load timestamp tracking

### Staging Schema
**Purpose:** Cleansed, standardized data ready for business logic

```sql
Staging.stg_erp_products_clean
Staging.stg_erp_stores_clean
Staging.stg_erp_inventory_clean
Staging.stg_crm_customers_clean
Staging.stg_mkt_promotions_clean
Staging.stg_pos_transactions_header_clean
Staging.stg_pos_transactions_lines_clean
Staging.stg_api_weather_clean
```

**Transformations:**
- Remove nulls
- Convert data types
- Standardize formats (dates, phone numbers)
- Remove duplicates
- Validate ranges

### Silver Schema
**Purpose:** Aggregated, business-logic applied tables

```sql
Silver.customer_summary         -- Customer metrics
Silver.product_summary          -- Product performance
Silver.store_summary            -- Store KPIs
Silver.transaction_summary      -- Transaction aggregates
```

**Features:**
- Business metrics calculated
- Relationships established
- Performance optimized
- Audit columns added

### Gold Schema (Star Schema)
**Purpose:** Analytics-ready dimensional model

**Fact Tables:**
```sql
Gold.fact_sales
  - transaction_id (PK)
  - date_id (FK → dim_date)
  - store_id (FK → dim_store)
  - customer_id (FK → dim_customer)
  - product_id (FK → dim_product)
  - promotion_id (FK → dim_promotion)
  - quantity_sold
  - total_amount
  - discount_amount
  - margin_amount
```

```sql
Gold.fact_inventory_snapshot
  - inventory_date (PK)
  - store_id (PK, FK → dim_store)
  - product_id (PK, FK → dim_product)
  - quantity_on_hand
  - reorder_level
  - stock_value
```

**Dimension Tables:**
```sql
Gold.dim_date              -- Date dimensions (day, month, year, week, quarter)
Gold.dim_customer          -- Customer attributes
Gold.dim_product           -- Product information
Gold.dim_store             -- Store details
Gold.dim_promotion         -- Promotion details
Gold.dim_weather           -- Weather conditions
Gold.bridge_promotion_product  -- Many-to-many relationship
```

---

## 🔄 ETL Process Details

### Stage 1: Pull Weather Data (Python)
**File:** `pipelines/pull_weather_data.py`

**Steps:**
1. Load store coordinates from `erp_stores.csv`
2. Call Open-Meteo API for each store
3. Fetch daily weather (2023-present)
4. Generate CSV: `data/sample/api_weather.csv`

**Output:** 
- ~28,000 rows
- Columns: date, store_id, temperature, precipitation, weather_code

### Stage 2: Ingest CSV Files (Python)
**File:** `pipelines/ingest_csv.py`

**Steps:**
1. Read each CSV file
2. Add metadata columns (batch_id, source_file)
3. Insert to Raw schema
4. Track manifest entry

**Files:**
```
data/sample/
├─ erp_products.csv          (100 products)
├─ erp_stores.csv            (50 stores)
├─ erp_inventory.csv         (5,000 records)
├─ crm_customers.csv         (10,000 customers)
├─ mkt_promotions.csv        (1,000 promotions)
├─ pos_transactions_header.csv (50,000 transactions)
├─ pos_transactions_lines.csv  (150,000 line items)
└─ api_weather.csv           (28,678 weather records)
```

### Stage 3: Staging (dbt)
**Command:** `dbt run --select staging.*`

**Models:**
```
stg_erp_products            → Clean product data
stg_erp_stores              → Clean store data
stg_crm_customers           → Clean customer data
stg_mkt_promotions          → Clean promotions
stg_pos_transactions_*      → Clean transactions
stg_api_weather             → Clean weather
```

**Transformations:**
```sql
-- Example: stg_erp_products.sql
SELECT
  CAST(product_id AS INT) AS product_id,
  UPPER(TRIM(product_name)) AS product_name,
  CAST(unit_price AS NUMERIC(10,2)) AS unit_price,
  CASE 
    WHEN category IS NULL THEN 'UNCATEGORIZED'
    ELSE category 
  END AS category,
  CURRENT_TIMESTAMP AS load_timestamp
FROM
  Raw.erp_products
WHERE
  product_id IS NOT NULL
```

### Stage 4: Silver (dbt)
**Command:** `dbt run --select silver.*`

**Models:**
```
api_weather_clean       → Weather by store/date
crm_customers_clean     → Customer demographics
erp_inventory_clean     → Inventory details
erp_products_clean      → Product catalog
erp_stores_clean        → Store master
mkt_promotions_clean    → Promotion rules
pos_transactions_*_clean → Transaction details
```

### Stage 5: Gold (dbt)
**Command:** `dbt run --select gold.*`

**Star Schema Models:**
```
fact_sales              → Sales transactions
fact_inventory_snapshot → Inventory tracking
dim_date                → Date dimension
dim_customer            → Customer dimension
dim_product             → Product dimension
dim_store               → Store dimension
dim_promotion           → Promotion dimension
dim_weather             → Weather dimension
bridge_promotion_product → Many-to-many bridge
```

---

## 🔌 Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Orchestration** | Apache Airflow | 2.8.0+ | Workflow scheduling |
| **Transformation** | dbt | 1.11.2 | SQL transformations |
| **Database** | SQL Server | 2019+ | Data warehouse |
| **Python** | Python | 3.10+ | Data ingestion |
| **Driver** | ODBC Driver 17 | 17.x | DB connectivity |
| **API** | Open-Meteo | Free | Weather data |
| **Environment** | WSL2 Ubuntu | 22.04 | Linux runtime |

---

## 📊 Data Volumes

| Source | Records | Frequency | Update Method |
|--------|---------|-----------|----------------|
| Products | 100 | Weekly | Full refresh |
| Stores | 50 | Monthly | Full refresh |
| Customers | 10,000 | Daily | Incremental |
| Promotions | 1,000 | Daily | Incremental |
| Transactions | 50,000+ | Daily | Incremental |
| Weather | ~30,000 | Daily | Append |

---

## 🔐 Security Considerations

1. **Environment Variables:** Database credentials in `.env` (never commit)
2. **SQL Authentication:** Non-admin account with schema-level permissions
3. **Network:** WSL2 isolated, SQL Server accessed via IP
4. **Logs:** Contains no sensitive data (sanitized)
5. **Data Access:** Windows integrated security for Windows clients

---

## ⚙️ Performance Optimization

### Indexing Strategy
- Cluster index on fact tables (date, store)
- Non-clustered on foreign keys
- Date dimension for time-based queries

### Partitioning
- Fact tables partitioned by year/month
- Reduces query scope
- Enables faster bulk inserts

### Statistics
- Auto-update enabled
- Rebuilt nightly after major loads
- Improves query plans

### Query Patterns
- Star join optimization
- Column store indexes on fact tables
- Pre-aggregated Silver layer queries

---

## 🚨 Error Handling & Recovery

### Airflow Retry Logic
```python
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
```

### dbt Error Handling
- Pre-execute tests validation
- Singular tests for data quality
- Generic tests for completeness

### Data Validation
- Row count checks
- Null ratio validation
- Data type enforcement
- Referential integrity

---

## 📈 Scalability

### Current Design Supports:
- **Up to 10M rows** in fact tables
- **Hourly execution** without bottlenecks
- **Parallel dbt runs** (8 threads)
- **CSV files up to 500MB**

### For Larger Scale:
- Implement partitioning
- Use Incremental models in dbt
- Consider Columnstore indexes
- Move to Airflow distributed setup

---

## 🔄 Deployment Pipeline

```
Development
    ↓
Staging (copy of prod data)
    ↓
Production (live data)
```

**Promotion Strategy:**
1. Test in dev with sample data
2. Validate in staging with full data
3. Deploy to production with approval
4. Monitor for 48 hours
5. Keep rollback plan ready

---

**Last Updated:** January 7, 2026
