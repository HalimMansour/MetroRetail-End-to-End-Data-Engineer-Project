"""
Configuration module for MetroRetail data pipelines
Centralized settings for database connections and file paths
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SAMPLE_DIR = DATA_DIR / "sample"
RAW_DIR = DATA_DIR / "raw"
LOGS_DIR = PROJECT_ROOT / "logs"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# SQL Server connection settings
# For WSL2: SQL Server runs on Windows host
# Use the nameserver IP from /etc/resolv.conf or host.docker.internal
SQL_SERVER = os.getenv("SQL_SERVER", "10.255.255.254\\SQLEXPRESS")
SQL_DATABASE = os.getenv("SQL_DATABASE", "MetroRetailDB")
SQL_USERNAME = os.getenv("SQL_USERNAME", "halim")   
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "Halim@1999!")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
SQL_TRUSTED_CONNECTION = os.getenv("SQL_TRUSTED_CONNECTION", "no")

# Build connection string
if SQL_TRUSTED_CONNECTION.lower() == "yes":
    # Windows Authentication
    CONNECTION_STRING = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"Trusted_Connection=yes;"
    )
else:
    # SQL Authentication
    CONNECTION_STRING = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
    )

# Source system mappings
SOURCE_SYSTEMS = {
    "pos": "POS",
    "erp": "ERP",
    "crm": "CRM",
    "mkt": "MKT",
    "api": "API"
}

# File to table mappings
FILE_TABLE_MAP = {
    "pos_transactions_header.csv": {
        "table": "Raw.pos_transactions_header",
        "source": "POS",
        "entity": "transactions_header"
    },
    "pos_transactions_lines.csv": {
        "table": "Raw.pos_transactions_lines",
        "source": "POS",
        "entity": "transactions_lines"
    },
    "erp_products.csv": {
        "table": "Raw.erp_products",
        "source": "ERP",
        "entity": "products"
    },
    "erp_stores.csv": {
        "table": "Raw.erp_stores",
        "source": "ERP",
        "entity": "stores"
    },
    "erp_inventory.csv": {
        "table": "Raw.erp_inventory",
        "source": "ERP",
        "entity": "inventory"
    },
    "crm_customers.csv": {
        "table": "Raw.crm_customers",
        "source": "CRM",
        "entity": "customers"
    },
    "mkt_promotions.csv": {
        "table": "Raw.mkt_promotions",
        "source": "MKT",
        "entity": "promotions"
    },
    "api_weather.csv": {
        "table": "Raw.api_weather",
        "source": "API",
        "entity": "weather"
    }
}

# Logging configuration
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")