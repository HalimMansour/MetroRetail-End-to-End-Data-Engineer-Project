import pyodbc
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Connection string for Windows Authentication
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER=localhost\\SQLEXPRESS;"
    f"DATABASE=MetroRetailDB;"
    f"Trusted_Connection=yes;"
)

try:
    # Connect to SQL Server
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Read and execute the schema creation script
    with open('sqlserver/01_create_schemas.sql', 'r') as f:
        sql_script = f.read()
    
    # Split by GO statements and execute each batch
    batches = sql_script.split('GO')
    for batch in batches:
        if batch.strip():
            cursor.execute(batch)
    
    conn.commit()
    print("✅ Schemas created successfully!")
    
    # Verify schemas exist
    cursor.execute("""
        SELECT name 
        FROM sys.schemas 
        WHERE name IN ('Raw', 'Staging', 'Silver', 'Gold')
        ORDER BY name
    """)
    
    print("\n📋 Available schemas:")
    for row in cursor.fetchall():
        print(f"  - {row[0]}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")