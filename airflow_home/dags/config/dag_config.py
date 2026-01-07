"""DAG Configuration for MetroRetail Pipeline"""

from datetime import datetime, timedelta
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "metro_dbt"
PIPELINES_DIR = PROJECT_ROOT / "pipelines"

# Default DAG arguments
DEFAULT_DAG_ARGS = {
    'owner': 'metroretail',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['alerts@metroretail.com'],
    'email_on_failure': False,  # Set to True if you configure email
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Schedule: Daily at 2 AM
SCHEDULE_INTERVAL = '0 2 * * *'

# Data quality thresholds
DQ_THRESHOLDS = {
    'min_rows_gold_fact': 100,  # Minimum rows in fact table
}