"""
Generic CSV Ingestion Script
Loads CSV files from data/sample/ into Raw layer tables
Handles manifest tracking and error logging
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import sys
from typing import Optional , Dict

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent))

from config import SAMPLE_DIR, FILE_TABLE_MAP
from db_utils import DatabaseManager, generate_batch_id

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def ingest_csv_file(file_name: str, truncate: bool = False) -> bool:
    """
    Ingest a single CSV file into Raw layer
    
    Args:
        file_name: Name of CSV file (e.g., 'erp_products.csv')
        truncate: Whether to truncate target table before insert
    
    Returns:
        True if successful, False otherwise
    """
    logger.info("=" * 70)
    logger.info(f"Starting ingestion: {file_name}")
    logger.info("=" * 70)
    
    # Validate file exists
    file_path = SAMPLE_DIR / file_name
    if not file_path.exists():
        logger.error(f"✗ File not found: {file_path}")
        return False
    
    # Get table mapping
    if file_name not in FILE_TABLE_MAP:
        logger.error(f"✗ No table mapping for: {file_name}")
        return False
    
    mapping = FILE_TABLE_MAP[file_name]
    target_table = mapping['table']
    source_system = mapping['source']
    entity_name = mapping['entity']
    
    # Generate batch ID
    batch_id = generate_batch_id(source_system.lower(), entity_name)
    logger.info(f"Batch ID: {batch_id}")
    logger.info(f"Target table: {target_table}")
    
    try:
        # Read CSV file
        logger.info(f"Reading CSV: {file_path}")
        df = pd.read_csv(file_path, low_memory=False)
        logger.info(f"  Rows read: {len(df):,}")
        logger.info(f"  Columns: {', '.join(df.columns.tolist())}")
        
        # Add metadata columns
        df['Batch_ID'] = batch_id
        df['Source_File'] = file_name
        
        # Special handling for weather data - rename Store_ID to Retail_Location_ID
        if file_name == 'api_weather.csv' and 'Store_ID' in df.columns:
            df = df.rename(columns={'Store_ID': 'Retail_Location_ID'})
        
        logger.info(f"✓ Successfully processed {file_name}")
        logger.info(f"  Ready for insertion: {len(df):,} rows")
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to process {file_name}: {e}")
        return False
        
        logger.info("=" * 70)
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Processing failed: {e}")
        return False
    
    finally:
        logger.info("=" * 70)


def ingest_all_csv_files(truncate: bool = False) -> Dict[str, bool]:
    """
    Ingest all CSV files defined in FILE_TABLE_MAP
    
    Args:
        truncate: Whether to truncate tables before insert
    
    Returns:
        Dictionary of file_name: success_status
    """
    logger.info("\n" + "=" * 70)
    logger.info("BULK INGESTION - ALL CSV FILES")
    logger.info("=" * 70)
    
    results = {}
    
    for file_name in FILE_TABLE_MAP.keys():
        success = ingest_csv_file(file_name, truncate=truncate)
        results[file_name] = success
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("INGESTION SUMMARY")
    logger.info("=" * 70)
    
    successful = sum(1 for v in results.values() if v)
    failed = len(results) - successful
    
    for file_name, success in results.items():
        status = "[OK] SUCCESS" if success else "[ERROR] FAILED"
        logger.info(f"  {status}: {file_name}")
    
    logger.info("-" * 70)
    logger.info(f"Total files: {len(results)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info("=" * 70)
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Ingest CSV files into Raw layer')
    parser.add_argument('--file', type=str, help='Specific file to ingest (e.g., erp_products.csv)')
    parser.add_argument('--all', action='store_true', help='Ingest all files')
    parser.add_argument('--truncate', action='store_true', help='Truncate tables before insert')
    
    args = parser.parse_args()
    
    if args.file:
        # Ingest single file
        success = ingest_csv_file(args.file, truncate=args.truncate)
        sys.exit(0 if success else 1)
    
    elif args.all:
        # Ingest all files
        results = ingest_all_csv_files(truncate=args.truncate)
        all_success = all(results.values())
        sys.exit(0 if all_success else 1)
    
    else:
        parser.print_help()
        sys.exit(1)