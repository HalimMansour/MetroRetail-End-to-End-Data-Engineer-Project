-- Check manifest for all batches
SELECT 
    Source_System,
    Entity_Name,
    Row_Count,
    Load_Status,
    DATEDIFF(SECOND, Load_Start_TS, Load_End_TS) as Duration_Seconds,
    Load_Start_TS
FROM Raw.Ingestion_Manifest
ORDER BY Load_Start_TS DESC;

-- Check row counts per table
SELECT 'pos_transactions_header' as table_name, COUNT(*) as row_count FROM Raw.pos_transactions_header
UNION ALL
SELECT 'pos_transactions_lines', COUNT(*) FROM Raw.pos_transactions_lines
UNION ALL
SELECT 'erp_products', COUNT(*) FROM Raw.erp_products
UNION ALL
SELECT 'erp_stores', COUNT(*) FROM Raw.erp_stores
UNION ALL
SELECT 'erp_inventory', COUNT(*) FROM Raw.erp_inventory
UNION ALL
SELECT 'crm_customers', COUNT(*) FROM Raw.crm_customers
UNION ALL
SELECT 'mkt_promotions', COUNT(*) FROM Raw.mkt_promotions
UNION ALL
SELECT 'api_weather', COUNT(*) FROM Raw.api_weather;

-- Sample dirty data (transactions with currency symbols)
SELECT TOP 5 
    Transaction_ID,
    Total_Amount,        -- Should have "$" or "," in some rows
    Total_Discount,      -- Should have "NA" or NULL in some rows
    Payment_Method       -- Should have casing issues
FROM Raw.pos_transactions_header;

-- Sample weather data with missing temperatures
SELECT TOP 10
    Weather_Date,
    Retail_Location_ID,  -- Intentional key mismatch
    Temperature_C,       -- Should have NULL values (~2%)
    Weather_Condition
FROM Raw.api_weather
WHERE Temperature_C IS NULL OR Temperature_C = 'None';