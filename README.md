# 🛒 MetroRetail

MetroRetail is an end-to-end retail analytics & data engineering project that delivers a complete data pipeline using **Apache Airflow**, **dbt**, **SQL Server**, and **Power BI**.

---

## 🏗️ Architecture

<img width="1251" height="858" alt="Data Architecture Final" src="https://github.com/user-attachments/assets/184e6402-d8fa-46a0-8655-e9dbf669c8e9" />

## 🧰 Tech Stack

- **Apache Airflow** – Orchestration  
- **dbt** – Data transformations  
- **SQL Server** – Data warehouse  
- **Python** – Data ingestion  
- **Power BI** – Analytics & dashboards  

---

## 📁 Project Structure

MetroRetail/
├── dags/ # Airflow DAGs
├── pipelines/ # Python ingestion scripts
├── dbt/metro_dbt/ # dbt models (staging, silver, gold)
├── sqlserver/ # SQL Server DDL scripts
├── data/sample/ # Sample CSV files
├── Report/
│ └── MetroRetail.pbix # Power BI report
├── airflow_home/
├── logs/
├── requirements.txt
├── .env.sample
└── README.md

yaml
Copy code

---

## 🚀 Quick Start

### Prerequisites

- Python 3.10+  
- SQL Server  
- WSL2 (recommended for Airflow)  
- ODBC Driver 17  

### Setup
```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 🔄 Pipeline Overview

1. Weather API ingestion  
2. CSV ingestion → Raw layer  
3. Staging layer (clean & standardize)  
4. Silver layer (business logic & aggregations)  
5. Gold layer (star schema)  
6. Data quality validation  

---

## 📊 Data Model (Gold Layer)

**Fact Tables**  
- fact_sales  
- fact_inventory_snapshot  

**Dimension Tables**  
- dim_customer  
- dim_product  
- dim_store  
- dim_date  
- dim_promotion  
- dim_weather  

---

## 📈 Power BI

- **File:** `Report/MetroRetail.pbix`  
- **Source:** SQL Server (Gold schema)  
- **Model:** Star schema  
- **DAX:** Measures for KPIs & time intelligence  

**Dashboards:**  
<img width="1448" height="812" alt="1" src="https://github.com/user-attachments/assets/976f5e59-c9b2-4c55-9918-c5aaf9baf7d4" />
<img width="1448" height="810" alt="2" src="https://github.com/user-attachments/assets/913e0137-6e1c-4a2c-92b4-edd078acec4a" />
<img width="1445" height="811" alt="3" src="https://github.com/user-attachments/assets/536d85bd-be22-4a8f-b52f-566f197ad5f2" />
<img width="1447" height="812" alt="4" src="https://github.com/user-attachments/assets/6632c38a-475b-4d0e-ad75-c2919d6c4bd5" />
<img width="1443" height="813" alt="5" src="https://github.com/user-attachments/assets/cf8b9121-9d77-4e83-b044-7e110fe594eb" />

---

## ▶️ Run Airflow

```powershell
.\start_airflow_wsl2.ps1
```
<img width="1486" height="633" alt="image" src="https://github.com/user-attachments/assets/49e31db0-21e1-4d4a-90b6-bbf2d99f170c" />



