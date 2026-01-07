"""
Database utility functions for MetroRetail pipelines
Handles connections, manifest tracking, and bulk inserts
FIXED: Removed Unicode characters for Windows compatibility
"""

import pyodbc
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any
import logging
from config import CONNECTION_STRING

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.conn_str = CONNECTION_STRING
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = pyodbc.connect(self.conn_str)
            self.cursor = self.conn.cursor()
            logger.info("[OK] Database connection established")
            return True
        except Exception as e:
            logger.error(f"[ERROR] Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def start_manifest(self, batch_id: str, source_system: str, 
                       entity_name: str, source_file: str) -> bool:
        """
        Create manifest entry at start of ingestion
        """
        try:
            sql = """
                INSERT INTO Raw.Ingestion_Manifest 
                (Batch_ID, Source_System, Entity_Name, Source_File, 
                 Row_Count, Load_Start_TS, Load_Status)
                VALUES (?, ?, ?, ?, 0, GETDATE(), 'STARTED')
            """
            self.cursor.execute(sql, (batch_id, source_system, entity_name, source_file))
            self.conn.commit()
            logger.info(f"[OK] Manifest entry created: {batch_id}")
            return True
        except Exception as e:
            logger.error(f"[ERROR] Failed to create manifest entry: {e}")
            return False
    
    def complete_manifest(self, batch_id: str, row_count: int, 
                          status: str = 'COMPLETED', 
                          error_message: Optional[str] = None) -> bool:
        """
        Update manifest entry on completion/failure
        """
        try:
            sql = """
                UPDATE Raw.Ingestion_Manifest
                SET Row_Count = ?,
                    Load_End_TS = GETDATE(),
                    Load_Status = ?,
                    Error_Message = ?
                WHERE Batch_ID = ?
            """
            self.cursor.execute(sql, (row_count, status, error_message, batch_id))
            self.conn.commit()
            logger.info(f"[OK] Manifest updated: {batch_id} - {status}")
            return True
        except Exception as e:
            logger.error(f"[ERROR] Failed to update manifest: {e}")
            return False
    
    def truncate_table(self, table_name: str) -> bool:
        """
        Truncate a table (use with caution!)
        """
        try:
            sql = f"TRUNCATE TABLE {table_name}"
            self.cursor.execute(sql)
            self.conn.commit()
            logger.info(f"[OK] Table truncated: {table_name}")
            return True
        except Exception as e:
            logger.error(f"[ERROR] Failed to truncate table: {e}")
            return False
    
    def bulk_insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                              batch_size: int = 1000) -> int:
        """
        Bulk insert DataFrame into SQL Server table
        Returns number of rows inserted
        """
        if df.empty:
            logger.warning("Empty DataFrame - nothing to insert")
            return 0
        
        try:
            # Convert all columns to strings to match VARCHAR schema
            df_str = df.astype(str)
            
            # Replace pandas 'nan' strings with None for NULL values
            df_str = df_str.replace('nan', None)
            df_str = df_str.replace('None', None)
            
            # Get column names from DataFrame
            columns = df_str.columns.tolist()
            placeholders = ','.join(['?' for _ in columns])
            columns_str = ','.join([f"[{col}]" for col in columns])
            
            sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples
            data = [tuple(row) for row in df_str.values]
            
            # Batch insert
            total_inserted = 0
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                self.cursor.executemany(sql, batch)
                self.conn.commit()
                total_inserted += len(batch)
                
                if (i + batch_size) % 10000 == 0:
                    logger.info(f"  Inserted {total_inserted:,} rows...")
            
            logger.info(f"[OK] Bulk insert completed: {total_inserted:,} rows into {table_name}")
            return total_inserted
            
        except Exception as e:
            logger.error(f"[ERROR] Bulk insert failed: {e}")
            self.conn.rollback()
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute SELECT query and return DataFrame
        """
        try:
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as e:
            logger.error(f"[ERROR] Query execution failed: {e}")
            return pd.DataFrame()
    
    def get_table_count(self, table_name: str) -> int:
        """
        Get row count for a table
        """
        try:
            sql = f"SELECT COUNT(*) as cnt FROM {table_name}"
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            return result.cnt if result else 0
        except Exception as e:
            logger.error(f"[ERROR] Failed to get table count: {e}")
            return -1


def generate_batch_id(source: str, entity: str) -> str:
    """
    Generate unique batch ID
    Format: {source}_{entity}_{YYYYMMDD}_{HHMMSS}
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{source}_{entity}_{timestamp}"


def validate_connection() -> bool:
    """
    Test database connection
    """
    db = DatabaseManager()
    if db.connect():
        logger.info("[OK] Database connection test successful")
        db.disconnect()
        return True
    return False