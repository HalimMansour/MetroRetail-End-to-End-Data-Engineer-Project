{{
    config(
        materialized = 'table',
        schema = 'Silver',
        alias = 'erp_inventory_clean',
        full_refresh = true,
        transient = false,
    )
}}

/*
    Silver Model: ERP Inventory Clean
    Source: Staging.stg_erp_inventory
    Target: Silver.erp_inventory_clean

    Silver Layer Transformations:
    ✅ Keep only latest snapshot per Product_SKU + Store_ID
    ✅ Exclude negative quantities (data errors)
    ✅ Flag outlier quantities (>10,000 units)
    ✅ Flag items below reorder level
    ✅ Validate FK references (Product_SKU, Store_ID exist)
    ✅ Calculate composite DQ Score (0-100)
    ✅ Only keep valid records
    
    Business Rules:
    - One snapshot per Product_SKU + Store_ID (latest by Snapshot_Date)
    - Quantity_On_Hand must be >= 0 (negatives are data errors)
    - Reorder_Level must be >= 0
    - Outlier threshold: Quantity_On_Hand > 10,000
    
    Data Quality:
    - Exclude rows with Is_Negative_Quantity = 1
    - Flag but keep rows with Is_Outlier_Quantity = 1
    - Flag items below reorder level for replenishment
*/

WITH source AS (
    SELECT *
    FROM {{ ref('stg_erp_inventory') }}
    WHERE DQ_Is_Valid = 1  -- Only valid rows from Staging
        AND Is_Negative_Quantity = 0  -- Exclude negative quantities (data errors)
),

-- Step 1: Get latest snapshot per Product_SKU + Store_ID
latest_snapshot AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Product_SKU, Store_ID 
            ORDER BY Snapshot_Date DESC, Load_Timestamp DESC
        ) AS row_num
    FROM source
),

latest_only AS (
    SELECT *
    FROM latest_snapshot
    WHERE row_num = 1
),

-- Step 2: Validate foreign key references
with_fk_validation AS (
    SELECT
        inv.*,
        
        -- Check if Product_SKU exists in products
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM {{ ref('stg_erp_products') }} p 
                WHERE p.Product_SKU = inv.Product_SKU 
                    AND p.DQ_Is_Valid = 1
            ) THEN 1 ELSE 0 
        END AS Product_Exists,
        
        -- Check if Store_ID exists in stores
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM {{ ref('stg_erp_stores') }} s 
                WHERE s.Store_ID = inv.Store_ID 
                    AND s.DQ_Is_Valid = 1
            ) THEN 1 ELSE 0 
        END AS Store_Exists
        
    FROM latest_only inv
),

-- Step 3: Calculate composite DQ Score (0-100)
with_dq_score AS (
    SELECT
        Inventory_ID,
        Product_SKU,
        Store_ID,
        Snapshot_Date,
        Quantity_On_Hand,
        Reorder_Level,
        
        -- Business Flags (from Staging)
        Is_Below_Reorder_Level,
        Is_Negative_Quantity,  -- Should be 0 (filtered out)
        Is_Outlier_Quantity,
        
        -- FK Validation Flags
        Product_Exists,
        Store_Exists,
        
        -- Data Quality Score (0-100)
        -- Weighted based on criticality of each field
        (
            (DQ_Inventory_ID_Valid * 15) +       -- 15% - Inventory ID
            (DQ_Product_SKU_Valid * 25) +        -- 25% - Product SKU is critical
            (DQ_Store_ID_Valid * 20) +           -- 20% - Store ID is critical
            (DQ_Snapshot_Date_Valid * 15) +      -- 15% - Snapshot Date
            (DQ_Quantity_On_Hand_Valid * 15) +   -- 15% - Quantity is critical
            (DQ_Reorder_Level_Valid * 10)        -- 10% - Reorder Level
        ) AS DQ_Score,
        
        -- Overall validity (all critical checks must pass)
        CASE 
            WHEN DQ_Inventory_ID_Valid = 1 
                AND DQ_Product_SKU_Valid = 1
                AND DQ_Store_ID_Valid = 1
                AND DQ_Snapshot_Date_Valid = 1
                AND DQ_Quantity_On_Hand_Valid = 1
                AND DQ_Reorder_Level_Valid = 1
                AND Is_Negative_Quantity = 0
                AND Product_Exists = 1
                AND Store_Exists = 1
            THEN 1 ELSE 0 
        END AS Is_Valid,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM with_fk_validation
),

-- Step 4: Final select with all required columns
final AS (
    SELECT
        Inventory_ID,
        Product_SKU,
        Store_ID,
        Snapshot_Date,
        Quantity_On_Hand,
        Reorder_Level,
        
        -- Business Flags
        Is_Below_Reorder_Level,
        Is_Negative_Quantity,
        Is_Outlier_Quantity,
        1 AS Is_Latest_Snapshot,  -- All rows are latest by design
        Is_Valid,
        
        -- Data Quality Score
        DQ_Score,
        
        -- Metadata
        Batch_ID,
        Source_File,
        GETDATE() AS Created_TS,
        GETDATE() AS Updated_TS
        
    FROM with_dq_score
    WHERE Is_Valid = 1  -- Only keep fully valid records in Silver
)

SELECT * FROM final

/*
    Output Schema:
    - Inventory_ID: VARCHAR(50) - Business key
    - Product_SKU: VARCHAR(50) - FK to products
    - Store_ID: VARCHAR(50) - FK to stores
    - Snapshot_Date: DATE - Snapshot date
    - Quantity_On_Hand: INT - Must be >= 0
    - Reorder_Level: INT - Must be >= 0
    - Is_Below_Reorder_Level: BIT - Quantity < Reorder_Level
    - Is_Negative_Quantity: BIT - Should be 0 (filtered out)
    - Is_Outlier_Quantity: BIT - Quantity > 10,000
    - Is_Latest_Snapshot: BIT - Always 1 (latest per SKU+Store)
    - Is_Valid: BIT
    - DQ_Score: INT (0-100)
    - Batch_ID: VARCHAR(100)
    - Source_File: VARCHAR(255)
    - Created_TS: DATETIME2
    - Updated_TS: DATETIME2
    
    Note: Inventory_SK (surrogate key) is auto-generated by SQL Server IDENTITY
    
    Business Logic:
    - One row per Product_SKU + Store_ID (latest snapshot)
    - Negative quantities are excluded (data errors)
    - Outlier quantities are flagged but kept
    - Items below reorder level are flagged for replenishment
    
    Sample Output (based on your data):
    - Inventory_ID: INV00000801
    - Product_SKU: P0129
    - Store_ID: S009
    - Snapshot_Date: 2024-01-01
    - Quantity_On_Hand: 51
    - Reorder_Level: 9
    - Is_Below_Reorder_Level: 0 (51 > 9)
    - Is_Outlier_Quantity: 0 (51 < 10000)
*/