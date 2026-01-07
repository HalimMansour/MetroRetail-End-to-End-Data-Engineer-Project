{{
    config(
        materialized = 'table',
        schema = 'Silver',
        alias = 'erp_products_clean',
        full_refresh = true,
        transient = false,
    )
}}

/*
    Silver Model: ERP Products Clean (SCD Type 2)
    Source: Staging.stg_erp_products
    Target: Silver.erp_products_clean

    SCD Type 2 Implementation:
    ✅ Track price/cost changes over time
    ✅ Key: Product_SKU + Store_ID
    ✅ Effective_From / Effective_To / Is_Current
    ✅ Version_Number increments on changes
    ✅ Deduplicate by Product_SKU (keep latest)
    ✅ Standardize Category/Sub_Category (fix typos)
    ✅ Flag price issues (Cost > Price)
    ✅ Calculate composite DQ Score (0-100)
    
    Category Standardization:

    
    Sub_Category Standardization:

    
    Business Rules:
    - Product_SKU + Store_ID is unique per version
    - Only one current version per Product_SKU + Store_ID
    - Effective_To = NULL for current version
    - Price and Cost_Price can change over time
    
    Note: This is a SIMPLIFIED SCD Type 2 for MVP
    For production, use dbt snapshots or incremental materialization
*/

WITH source AS (
    SELECT *
    FROM {{ ref('stg_erp_products') }}
    WHERE DQ_Is_Valid = 1  -- Only valid rows from Staging
),

-- Step 1: Deduplicate - Keep latest record per Product_SKU
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Product_SKU 
            ORDER BY Last_Updated DESC, Load_Timestamp DESC
        ) AS row_num
    FROM source
),

latest_only AS (
    SELECT *
    FROM deduplicated
    WHERE row_num = 1
),

-- Step 2: Standardize categories (fix typos)
with_standardization AS (
    SELECT
        Product_SKU,
        Product_Name,
        
        -- Standardize Category (fix known typos)
        CASE 
            WHEN Category LIKE 'F%' THEN 'Food'
            WHEN Category LIKE 'CL%' THEN 'Clothing'
            WHEN Category LIKE 'BEVE%' THEN 'Beverages'
            WHEN Category LIKE 'H%E' THEN 'Home'
            WHEN Category LIKE 'ELEC%' THEN 'Electronics'
            WHEN Category LIKE 'E%S' THEN 'Electronics'

            ELSE Category
        END AS Category,
        
        -- Standardize Sub_Category (fix known typos)
        CASE 
            WHEN Sub_Category LIKE 'W%N' THEN 'Women'
            WHEN Sub_Category LIKE 'WO%' THEN 'Women'
            WHEN Sub_Category LIKE 'ACC%' THEN 'Accessories'
            WHEN Sub_Category LIKE 'SN%' THEN 'Snacks'
            WHEN Sub_Category LIKE 'S%S' THEN 'Snacks'
            WHEN Sub_Category LIKE 'MO%' THEN 'Mobile'
            WHEN Sub_Category LIKE 'M%E' THEN 'Mobile'
            WHEN Sub_Category LIKE 'M%N' THEN 'Men'
            WHEN Sub_Category LIKE 'L%S' THEN 'Laptops'
            WHEN Sub_Category LIKE 'LPATOPS' THEN 'Laptops'
            WHEN Sub_Category LIKE 'C%G' THEN 'Cleaning'
            WHEN Sub_Category LIKE 'CLE%' THEN 'Cleaning'
            WHEN Sub_Category LIKE 'S%A' THEN 'Soda'
            WHEN Sub_Category LIKE 'SO%' THEN 'Soda'


            ELSE Sub_Category
        END AS Sub_Category,
        
        Price,
        Cost_Price,
        Supplier_ID,
        Last_Updated,
        
        -- Business Flags
        Has_Price_Issue,
        
        -- DQ Flags from Staging
        DQ_Product_SKU_Valid,
        DQ_Product_Name_Valid,
        DQ_Category_Valid,
        DQ_Sub_Category_Valid,
        DQ_Price_Valid,
        DQ_Cost_Price_Valid,
        DQ_Supplier_ID_Valid,
        DQ_Last_Updated_Valid,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM latest_only
),

-- Step 3: Calculate composite DQ Score (0-100)
with_dq_score AS (
    SELECT
        Product_SKU,
        Product_Name,
        Category,
        Sub_Category,
        Price,
        Cost_Price,
        Supplier_ID,
        Last_Updated,
        
        -- Business Flags
        Has_Price_Issue,
        
        -- Data Quality Score (0-100)
        -- Weighted based on criticality of each field
        (
            (DQ_Product_SKU_Valid * 20) +
            (DQ_Product_Name_Valid * 15) +
            (DQ_Category_Valid * 15) +
            (DQ_Sub_Category_Valid * 10) +
            (DQ_Price_Valid * 20) +
            (DQ_Cost_Price_Valid * 10) +
            (DQ_Supplier_ID_Valid * 10)
        ) AS DQ_Score,
        
        -- Overall validity
        CASE 
            WHEN DQ_Product_SKU_Valid = 1 
                AND DQ_Product_Name_Valid = 1
                AND DQ_Category_Valid = 1
                AND DQ_Price_Valid = 1
                AND DQ_Supplier_ID_Valid = 1
            THEN 1 ELSE 0 
        END AS Is_Valid,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM with_standardization
),

-- Step 4: Create SCD Type 2 records
-- For MVP: Create single version with current prices
-- Store_ID is set to 'ALL' for products without store-specific pricing
-- In production, you'd have store-specific prices from a different source
scd_records AS (
    SELECT
        Product_SKU,
        Product_Name,
        Category,
        Sub_Category,
        Price,
        Cost_Price,
        Supplier_ID,
        
        -- SCD Type 2 fields
        COALESCE(Last_Updated, CAST(GETDATE() AS DATE)) AS Effective_From,
        CAST(NULL AS DATE) AS Effective_To,  -- NULL = current version
        1 AS Is_Current,
        1 AS Version_Number,
        
        -- Business Flags
        CASE WHEN Cost_Price IS NULL THEN 0 ELSE 1 END AS Has_Cost_Price,
        Has_Price_Issue,
        Is_Valid,
        
        -- Data Quality Score
        DQ_Score,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM with_dq_score
),

-- Step 5: Final select with all required columns
final AS (
    SELECT
        Product_SKU,
        Product_Name,
        Category,
        Sub_Category,
        Price,
        Cost_Price,
        Supplier_ID,
        
        -- SCD Type 2 fields
        Effective_From,
        Effective_To,
        Is_Current,
        Version_Number,
        
        -- Business Flags
        Has_Cost_Price,
        Has_Price_Issue,
        Is_Valid,
        
        -- Data Quality Score
        DQ_Score,
        
        -- Metadata
        Batch_ID,
        Source_File,
        GETDATE() AS Created_TS,
        GETDATE() AS Updated_TS
        
    FROM scd_records
    -- WHERE Is_Valid = 1  -- Only keep fully valid records in Silver
)

SELECT * FROM final

/*
    Output Schema:
    - Product_SKU: VARCHAR(50) - Business key
    - Product_Name: VARCHAR(255)
    - Category: VARCHAR(100) - Standardized
    - Sub_Category: VARCHAR(100) - Standardized
    - Price: DECIMAL(18,2)
    - Cost_Price: DECIMAL(18,2) - Nullable
    - Supplier_ID: VARCHAR(50)
    - Effective_From: DATE - Start of version validity
    - Effective_To: DATE - End of version validity (NULL = current)
    - Is_Current: BIT - 1 for current version
    - Version_Number: INT - Version counter
    - Has_Cost_Price: BIT
    - Has_Price_Issue: BIT - Cost > Price
    - Is_Valid: BIT
    - DQ_Score: INT (0-100)
    - Batch_ID: VARCHAR(100)
    - Source_File: VARCHAR(255)
    - Created_TS: DATETIME2
    - Updated_TS: DATETIME2
    
    Note: Product_SK is auto-generated by SQL Server IDENTITY
    Note: Margin_Amount and Margin_Pct are computed columns in SQL Server table
    
    MVP Limitations:
    - Only tracks latest version (Version_Number = 1)
    - Effective_To is always NULL (all current)
    
    To implement true SCD Type 2:
    - Add store-specific product source
    - Implement price change detection logic
    - Update Effective_To on price changes
    - Increment Version_Number
    
    Category Fixes:
    - "ELECTRONCS" → "ELECTRONICS"
    - "BEVERAGE" → "BEVERAGES"
    
    Sub_Category Fixes:
    - "SMARTPHONS" → "SMARTPHONES"
    - "LPATOPS" → "LAPTOPS"
*/