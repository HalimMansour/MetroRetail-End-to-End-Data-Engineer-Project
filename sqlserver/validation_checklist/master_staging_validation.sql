-- =====================================================
-- MASTER STAGING VALIDATION SCRIPT
-- Validates ALL 8 staging tables after dbt run
-- Run after: dbt run --select staging
-- =====================================================

USE MetroRetailDB;
GO

SET NOCOUNT ON;

PRINT '';
PRINT '========================================';
PRINT 'METRORETAIL STAGING VALIDATION';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '========================================';
PRINT '';

-- =====================================================
-- SECTION 1: ROW COUNT SUMMARY (All Tables)
-- =====================================================
PRINT '========================================';
PRINT '1. ROW COUNT SUMMARY - RAW vs STAGING';
PRINT '========================================';
PRINT '';

DECLARE @validation_results TABLE (
    Table_Name VARCHAR(100),
    Raw_Count INT,
    Staging_Count INT,
    Match_Status VARCHAR(20),
    Valid_Count INT,
    Valid_Pct DECIMAL(5,2)
);

-- POS Transactions Header
INSERT INTO @validation_results
SELECT 
    'pos_transactions_header',
    (SELECT COUNT(*) FROM Raw.pos_transactions_header),
    (SELECT COUNT(*) FROM Staging.stg_pos_transactions_header),
    CASE 
        WHEN (SELECT COUNT(*) FROM Raw.pos_transactions_header) = 
             (SELECT COUNT(*) FROM Staging.stg_pos_transactions_header) 
        THEN 'MATCH' ELSE 'MISMATCH' 
    END,
    (SELECT SUM(DQ_Is_Valid) FROM Staging.stg_pos_transactions_header),
    (SELECT CAST(SUM(DQ_Is_Valid) * 100.0 / COUNT(*) AS DECIMAL(5,2)) 
     FROM Staging.stg_pos_transactions_header);

-- POS Transactions Lines
INSERT INTO @validation_results
SELECT 
    'pos_transactions_lines',
    (SELECT COUNT(*) FROM Raw.pos_transactions_lines),
    (SELECT COUNT(*) FROM Staging.stg_pos_transactions_lines),
    CASE 
        WHEN (SELECT COUNT(*) FROM Raw.pos_transactions_lines) = 
             (SELECT COUNT(*) FROM Staging.stg_pos_transactions_lines) 
        THEN 'MATCH' ELSE 'MISMATCH' 
    END,
    (SELECT SUM(DQ_Is_Valid) FROM Staging.stg_pos_transactions_lines),
    (SELECT CAST(SUM(DQ_Is_Valid) * 100.0 / COUNT(*) AS DECIMAL(5,2)) 
     FROM Staging.stg_pos_transactions_lines);

-- ERP Products
INSERT INTO @validation_results
SELECT 
    'erp_products',
    (SELECT COUNT(*) FROM Raw.erp_products),
    (SELECT COUNT(*) FROM Staging.stg_erp_products),
    CASE 
        WHEN (SELECT COUNT(*) FROM Raw.erp_products) = 
             (SELECT COUNT(*) FROM Staging.stg_erp_products) 
        THEN 'MATCH' ELSE 'MISMATCH' 
    END,
    (SELECT SUM(DQ_Is_Valid) FROM Staging.stg_erp_products),
    (SELECT CAST(SUM(DQ_Is_Valid) * 100.0 / COUNT(*) AS DECIMAL(5,2)) 
     FROM Staging.stg_erp_products);

-- ERP Stores
INSERT INTO @validation_results
SELECT 
    'erp_stores',
    (SELECT COUNT(*) FROM Raw.erp_stores),
    (SELECT COUNT(*) FROM Staging.stg_erp_stores),
    CASE 
        WHEN (SELECT COUNT(*) FROM Raw.erp_stores) = 
             (SELECT COUNT(*) FROM Staging.stg_erp_stores) 
        THEN 'MATCH' ELSE 'MISMATCH' 
    END,
    (SELECT SUM(DQ_Is_Valid) FROM Staging.stg_erp_stores),
    (SELECT CAST(SUM(DQ_Is_Valid) * 100.0 / COUNT(*) AS DECIMAL(5,2)) 
     FROM Staging.stg_erp_stores);

-- ERP Inventory
INSERT INTO @validation_results
SELECT 
    'erp_inventory',
    (SELECT COUNT(*) FROM Raw.erp_inventory),
    (SELECT COUNT(*) FROM Staging.stg_erp_inventory),
    CASE 
        WHEN (SELECT COUNT(*) FROM Raw.erp_inventory) = 
             (SELECT COUNT(*) FROM Staging.stg_erp_inventory) 
        THEN 'MATCH' ELSE 'MISMATCH' 
    END,
    (SELECT SUM(DQ_Is_Valid) FROM Staging.stg_erp_inventory),
    (SELECT CAST(SUM(DQ_Is_Valid) * 100.0 / COUNT(*) AS DECIMAL(5,2)) 
     FROM Staging.stg_erp_inventory);

-- CRM Customers
INSERT INTO @validation_results
SELECT 
    'crm_customers',
    (SELECT COUNT(*) FROM Raw.crm_customers),
    (SELECT COUNT(*) FROM Staging.stg_crm_customers),
    CASE 
        WHEN (SELECT COUNT(*) FROM Raw.crm_customers) = 
             (SELECT COUNT(*) FROM Staging.stg_crm_customers) 
        THEN 'MATCH' ELSE 'MISMATCH' 
    END,
    (SELECT SUM(DQ_Is_Valid) FROM Staging.stg_crm_customers),
    (SELECT CAST(SUM(DQ_Is_Valid) * 100.0 / COUNT(*) AS DECIMAL(5,2)) 
     FROM Staging.stg_crm_customers);

-- MKT Promotions
INSERT INTO @validation_results
SELECT 
    'mkt_promotions',
    (SELECT COUNT(*) FROM Raw.mkt_promotions),
    (SELECT COUNT(*) FROM Staging.stg_mkt_promotions),
    CASE 
        WHEN (SELECT COUNT(*) FROM Raw.mkt_promotions) = 
             (SELECT COUNT(*) FROM Staging.stg_mkt_promotions) 
        THEN 'MATCH' ELSE 'MISMATCH' 
    END,
    (SELECT SUM(DQ_Is_Valid) FROM Staging.stg_mkt_promotions),
    (SELECT CAST(SUM(DQ_Is_Valid) * 100.0 / COUNT(*) AS DECIMAL(5,2)) 
     FROM Staging.stg_mkt_promotions);

-- API Weather
INSERT INTO @validation_results
SELECT 
    'api_weather',
    (SELECT COUNT(*) FROM Raw.api_weather),
    (SELECT COUNT(*) FROM Staging.stg_api_weather),
    CASE 
        WHEN (SELECT COUNT(*) FROM Raw.api_weather) = 
             (SELECT COUNT(*) FROM Staging.stg_api_weather) 
        THEN 'MATCH' ELSE 'MISMATCH' 
    END,
    (SELECT SUM(DQ_Is_Valid) FROM Staging.stg_api_weather),
    (SELECT CAST(SUM(DQ_Is_Valid) * 100.0 / COUNT(*) AS DECIMAL(5,2)) 
     FROM Staging.stg_api_weather);

-- Display results
SELECT 
    Table_Name,
    FORMAT(Raw_Count, 'N0') AS Raw_Rows,
    FORMAT(Staging_Count, 'N0') AS Staging_Rows,
    Match_Status,
    FORMAT(Valid_Count, 'N0') AS Valid_Rows,
    CAST(Valid_Pct AS VARCHAR) + '%' AS Valid_Pct
FROM @validation_results
ORDER BY Staging_Count DESC;

PRINT '';

-- Summary stats
SELECT 
    FORMAT(SUM(Raw_Count), 'N0') AS Total_Raw_Rows,
    FORMAT(SUM(Staging_Count), 'N0') AS Total_Staging_Rows,
    FORMAT(SUM(Valid_Count), 'N0') AS Total_Valid_Rows,
    CAST(SUM(Valid_Count) * 100.0 / SUM(Staging_Count) AS DECIMAL(5,2)) AS Overall_Valid_Pct
FROM @validation_results;

PRINT '';

-- =====================================================
-- SECTION 2: DATA QUALITY SCORECARD
-- =====================================================
PRINT '========================================';
PRINT '2. DATA QUALITY SCORECARD BY TABLE';
PRINT '========================================';
PRINT '';

-- POS Transactions Header
PRINT '--- pos_transactions_header ---';
SELECT 
    'Walk-In Customers' AS Metric,
    FORMAT(SUM(Is_Walk_In), 'N0') AS Count,
    CAST(SUM(Is_Walk_In) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS Pct
FROM Staging.stg_pos_transactions_header
UNION ALL
SELECT 
    'Has Discount',
    FORMAT(SUM(Has_Discount), 'N0'),
    CAST(SUM(Has_Discount) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_pos_transactions_header
UNION ALL
SELECT 
    'Invalid Payment Method',
    FORMAT(COUNT(*) - SUM(DQ_Payment_Method_Valid), 'N0'),
    CAST((COUNT(*) - SUM(DQ_Payment_Method_Valid)) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_pos_transactions_header;
PRINT '';

-- POS Transactions Lines
PRINT '--- pos_transactions_lines ---';
SELECT 
    'Negative Quantities (Returns)' AS Metric,
    FORMAT(SUM(Is_Negative_Quantity), 'N0') AS Count,
    CAST(SUM(Is_Negative_Quantity) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS Pct
FROM Staging.stg_pos_transactions_lines
UNION ALL
SELECT 
    'Has Discount',
    FORMAT(SUM(Has_Discount), 'N0'),
    CAST(SUM(Has_Discount) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_pos_transactions_lines
UNION ALL
SELECT 
    'Has Promotion',
    FORMAT(SUM(Has_Promotion), 'N0'),
    CAST(SUM(Has_Promotion) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_pos_transactions_lines
UNION ALL
SELECT 
    'Pricing Issues (Cost > Price)',
    FORMAT(SUM(Has_Price_Issue), 'N0'),
    CAST(SUM(Has_Price_Issue) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_pos_transactions_lines;
PRINT '';

-- ERP Products
PRINT '--- erp_products ---';
SELECT 
    'Pricing Issues (Cost > Price)' AS Metric,
    FORMAT(SUM(Has_Price_Issue), 'N0') AS Count,
    CAST(SUM(Has_Price_Issue) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS Pct
FROM Staging.stg_erp_products
UNION ALL
SELECT 
    'Missing Category',
    FORMAT(COUNT(*) - SUM(DQ_Category_Valid), 'N0'),
    CAST((COUNT(*) - SUM(DQ_Category_Valid)) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_erp_products;
PRINT '';

-- ERP Stores
PRINT '--- erp_stores ---';
SELECT 
    'Multiple Managers' AS Metric,
    FORMAT(SUM(Has_Multiple_Managers), 'N0') AS Count,
    CAST(SUM(Has_Multiple_Managers) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS Pct
FROM Staging.stg_erp_stores;
PRINT '';

-- ERP Inventory
PRINT '--- erp_inventory ---';
SELECT 
    'Negative Quantities' AS Metric,
    FORMAT(SUM(Is_Negative_Quantity), 'N0') AS Count,
    CAST(SUM(Is_Negative_Quantity) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS Pct
FROM Staging.stg_erp_inventory
UNION ALL
SELECT 
    'Outlier Quantities (>10K)',
    FORMAT(SUM(Is_Outlier_Quantity), 'N0'),
    CAST(SUM(Is_Outlier_Quantity) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_erp_inventory
UNION ALL
SELECT 
    'Below Reorder Level',
    FORMAT(SUM(Is_Below_Reorder_Level), 'N0'),
    CAST(SUM(Is_Below_Reorder_Level) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_erp_inventory;
PRINT '';

-- CRM Customers
PRINT '--- crm_customers ---';
SELECT 
    'Missing Email' AS Metric,
    FORMAT(SUM(Has_Missing_Email), 'N0') AS Count,
    CAST(SUM(Has_Missing_Email) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS Pct
FROM Staging.stg_crm_customers
UNION ALL
SELECT 
    'Missing Phone',
    FORMAT(SUM(Has_Missing_Phone), 'N0'),
    CAST(SUM(Has_Missing_Phone) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_crm_customers
UNION ALL
SELECT 
    'Invalid Gender',
    FORMAT(COUNT(*) - SUM(DQ_Gender_Valid), 'N0'),
    CAST((COUNT(*) - SUM(DQ_Gender_Valid)) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_crm_customers;
PRINT '';

-- MKT Promotions
PRINT '--- mkt_promotions ---';
SELECT 
    'Invalid Date Range (End < Start)' AS Metric,
    FORMAT(SUM(Has_Invalid_Date_Range), 'N0') AS Count,
    CAST(SUM(Has_Invalid_Date_Range) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS Pct
FROM Staging.stg_mkt_promotions
UNION ALL
SELECT 
    'Multiple SKUs',
    FORMAT(SUM(Has_Multiple_SKUs), 'N0'),
    CAST(SUM(Has_Multiple_SKUs) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_mkt_promotions;
PRINT '';

-- API Weather
PRINT '--- api_weather ---';
SELECT 
    'Missing Temperature' AS Metric,
    FORMAT(SUM(Has_Missing_Temperature), 'N0') AS Count,
    CAST(SUM(Has_Missing_Temperature) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS Pct
FROM Staging.stg_api_weather
UNION ALL
SELECT 
    'Extreme Temperatures',
    FORMAT(SUM(Is_Extreme_Temperature), 'N0'),
    CAST(SUM(Is_Extreme_Temperature) * 100.0 / COUNT(*) AS DECIMAL(5,2))
FROM Staging.stg_api_weather;
PRINT '';

-- =====================================================
-- SECTION 3: BUSINESS METRICS
-- =====================================================
PRINT '========================================';
PRINT '3. KEY BUSINESS METRICS';
PRINT '========================================';
PRINT '';

-- Total Sales
PRINT '--- Total Sales Summary ---';
SELECT 
    FORMAT(COUNT(DISTINCT Transaction_ID), 'N0') AS Total_Transactions,
    FORMAT(SUM(Total_Amount), 'C2') AS Total_Sales,
    FORMAT(AVG(Total_Amount), 'C2') AS Avg_Transaction_Value,
    FORMAT(SUM(ISNULL(Total_Discount, 0)), 'C2') AS Total_Discounts
FROM Staging.stg_pos_transactions_header
WHERE DQ_Is_Valid = 1;
PRINT '';

-- Product Count
PRINT '--- Product Portfolio ---';
SELECT 
    FORMAT(COUNT(*), 'N0') AS Total_Products,
    FORMAT(COUNT(DISTINCT Category), 'N0') AS Unique_Categories,
    FORMAT(COUNT(DISTINCT Sub_Category), 'N0') AS Unique_SubCategories,
    FORMAT(AVG(Price), 'C2') AS Avg_Price
FROM Staging.stg_erp_products
WHERE DQ_Is_Valid = 1;
PRINT '';

-- Store Count
PRINT '--- Store Network ---';
SELECT 
    FORMAT(COUNT(*), 'N0') AS Total_Stores,
    FORMAT(COUNT(DISTINCT City), 'N0') AS Unique_Cities,
    FORMAT(COUNT(DISTINCT Region), 'N0') AS Unique_Regions,
    FORMAT(AVG(Store_Area_sqm), 'N2') AS Avg_Store_Area_sqm
FROM Staging.stg_erp_stores
WHERE DQ_Is_Valid = 1;
PRINT '';

-- Customer Count
PRINT '--- Customer Base ---';
SELECT 
    FORMAT(COUNT(*), 'N0') AS Total_Customers,
    FORMAT(COUNT(DISTINCT City), 'N0') AS Unique_Cities,
    FORMAT(AVG(DATEDIFF(YEAR, Birthdate, GETDATE())), 'N0') AS Avg_Age
FROM Staging.stg_crm_customers
WHERE DQ_Is_Valid = 1 AND Birthdate IS NOT NULL;
PRINT '';

-- =====================================================
-- SECTION 4: DATE RANGE COVERAGE
-- =====================================================
PRINT '========================================';
PRINT '4. DATE RANGE COVERAGE';
PRINT '========================================';
PRINT '';

-- Transactions
SELECT 
    'Transactions' AS Dataset,
    MIN(Transaction_Date) AS Start_Date,
    MAX(Transaction_Date) AS End_Date,
    DATEDIFF(DAY, MIN(Transaction_Date), MAX(Transaction_Date)) + 1 AS Total_Days
FROM Staging.stg_pos_transactions_header
WHERE DQ_Is_Valid = 1

UNION ALL

-- Weather
SELECT 
    'Weather',
    MIN(Weather_Date),
    MAX(Weather_Date),
    DATEDIFF(DAY, MIN(Weather_Date), MAX(Weather_Date)) + 1
FROM Staging.stg_api_weather
WHERE DQ_Is_Valid = 1

UNION ALL

-- Inventory
SELECT 
    'Inventory',
    MIN(Snapshot_Date),
    MAX(Snapshot_Date),
    DATEDIFF(DAY, MIN(Snapshot_Date), MAX(Snapshot_Date)) + 1
FROM Staging.stg_erp_inventory
WHERE DQ_Is_Valid = 1

UNION ALL

-- Promotions
SELECT 
    'Promotions',
    MIN(Start_Date),
    MAX(End_Date),
    DATEDIFF(DAY, MIN(Start_Date), MAX(End_Date)) + 1
FROM Staging.stg_mkt_promotions
WHERE DQ_Is_Valid = 1;

PRINT '';

-- =====================================================
-- SECTION 5: TOP ISSUES TO ADDRESS IN SILVER
-- =====================================================
PRINT '========================================';
PRINT '5. TOP ISSUES FOR SILVER LAYER';
PRINT '========================================';
PRINT '';

PRINT '1. Duplicate Product SKUs';
SELECT TOP 5
    Product_SKU,
    COUNT(*) AS Duplicate_Count
FROM Staging.stg_erp_products
GROUP BY Product_SKU
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;
PRINT '';

PRINT '2. Invalid Category Names (Typos)';
SELECT DISTINCT Category
FROM Staging.stg_erp_products
WHERE Category LIKE '%ELEC%'  -- Electroncs typo
   OR Category LIKE '%BEVER%';  -- Beverage vs Beverages
PRINT '';

PRINT '3. Store ID Mismatch (Lines vs Header)';
SELECT TOP 5
    h.Transaction_ID,
    h.Store_ID AS Header_Store_ID,
    l.Store_ID AS Line_Store_ID
FROM Staging.stg_pos_transactions_header h
JOIN Staging.stg_pos_transactions_lines l ON h.Transaction_ID = l.Transaction_ID
WHERE h.Store_ID <> l.Store_ID
  AND h.DQ_Is_Valid = 1 
  AND l.DQ_Is_Valid = 1;
PRINT '';

PRINT '4. Overlapping Promotions';
WITH promo_overlaps AS (
    SELECT 
        p1.Promotion_ID AS Promo1,
        p2.Promotion_ID AS Promo2,
        p1.Start_Date,
        p1.End_Date
    FROM Staging.stg_mkt_promotions p1
    JOIN Staging.stg_mkt_promotions p2 
        ON p1.Promotion_ID < p2.Promotion_ID
        AND p1.Start_Date <= p2.End_Date
        AND p1.End_Date >= p2.Start_Date
    WHERE p1.DQ_Is_Valid = 1 AND p2.DQ_Is_Valid = 1
)
SELECT COUNT(*) AS Overlapping_Promo_Pairs
FROM promo_overlaps;
PRINT '';

-- =====================================================
-- SECTION 6: FINAL SUMMARY
-- =====================================================
PRINT '========================================';
PRINT '6. VALIDATION SUMMARY';
PRINT '========================================';
PRINT '';

DECLARE @total_rows INT, @valid_rows INT, @invalid_rows INT;

SELECT 
    @total_rows = SUM(Staging_Count),
    @valid_rows = SUM(Valid_Count),
    @invalid_rows = SUM(Staging_Count) - SUM(Valid_Count)
FROM @validation_results;

PRINT 'Total Rows Processed: ' + FORMAT(@total_rows, 'N0');
PRINT 'Valid Rows: ' + FORMAT(@valid_rows, 'N0') + ' (' + 
      CAST(CAST(@valid_rows * 100.0 / @total_rows AS DECIMAL(5,2)) AS VARCHAR) + '%)';
PRINT 'Invalid Rows: ' + FORMAT(@invalid_rows, 'N0') + ' (' + 
      CAST(CAST(@invalid_rows * 100.0 / @total_rows AS DECIMAL(5,2)) AS VARCHAR) + '%)';

PRINT '';

-- Check if any table has 0 rows
IF EXISTS (SELECT 1 FROM @validation_results WHERE Staging_Count = 0)
BEGIN
    PRINT 'WARNING: The following tables have 0 rows:';
    SELECT Table_Name 
    FROM @validation_results 
    WHERE Staging_Count = 0;
    PRINT '';
END

-- Check if any table has mismatched counts
IF EXISTS (SELECT 1 FROM @validation_results WHERE Match_Status = 'MISMATCH')
BEGIN
    PRINT 'WARNING: The following tables have row count mismatches:';
    SELECT 
        Table_Name,
        FORMAT(Raw_Count, 'N0') AS Raw_Rows,
        FORMAT(Staging_Count, 'N0') AS Staging_Rows
    FROM @validation_results 
    WHERE Match_Status = 'MISMATCH';
    PRINT '';
END

-- Overall status
IF @valid_rows * 100.0 / @total_rows >= 95
    PRINT 'STATUS: EXCELLENT - Staging layer ready for Silver transformations';
ELSE IF @valid_rows * 100.0 / @total_rows >= 85
    PRINT 'STATUS: GOOD - Minor issues to address in Silver layer';
ELSE
    PRINT 'STATUS: NEEDS ATTENTION - Review data quality issues';

PRINT '';
PRINT '========================================';
PRINT 'VALIDATION COMPLETE';
PRINT 'Execution Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '========================================';

SET NOCOUNT OFF;