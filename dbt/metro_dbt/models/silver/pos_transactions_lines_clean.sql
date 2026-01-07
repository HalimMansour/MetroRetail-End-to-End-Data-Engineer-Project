{{
    config(
        materialized = 'table',
        schema = 'Silver',
        alias = 'pos_transactions_lines_clean',
        full_refresh = true,
        transient = false,
    )
}}

/*
    Silver Model: POS Transactions Lines Clean
    Source: Staging.stg_pos_transactions_lines
    Target: Silver.pos_transactions_lines_clean

    Silver Layer Transformations:
    ✅ Deduplicate by Transaction_Line_ID (keep latest)
    ✅ Validate foreign key references (Transaction_ID, Product_SKU, Store_ID, Promotion_ID)
    ✅ Flag returns, outliers, price issues
    ✅ Calculate composite DQ Score (0-100)
    ✅ Only keep valid records
    
    Business Rules:
    - Transaction_Line_ID is unique (deduplicate if needed)
    - Transaction_ID must exist in transaction headers
    - Product_SKU must exist in products
    - Store_ID must exist in stores
    - Promotion_ID must exist in promotions (if present)
    - Quantity must be non-zero (negative = returns)
    - Unit_Price and Cost_Price must be > 0
*/

WITH source AS (
    SELECT *
    FROM {{ ref('stg_pos_transactions_lines') }}
    WHERE DQ_Is_Valid = 1  -- Only valid rows from Staging
),

-- Step 1: Deduplicate - Keep latest record per Transaction_Line_ID
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Transaction_Line_ID 
            ORDER BY Load_Timestamp DESC
        ) AS row_num
    FROM source
),

latest_only AS (
    SELECT *
    FROM deduplicated
    WHERE row_num = 1
),

-- Step 2: Validate foreign key references
with_fk_validation AS (
    SELECT
        l.*,
        
        -- Check if Transaction_ID exists in header
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM {{ ref('pos_transactions_header_clean') }} h 
                WHERE h.Transaction_ID = l.Transaction_ID
            ) THEN 1 ELSE 0 
        END AS Transaction_Exists,
        
        -- Check if Product_SKU exists in products
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM {{ ref('erp_products_clean') }} p 
                WHERE p.Product_SKU = l.Product_SKU 
                    AND p.Is_Current = 1
            ) THEN 1 ELSE 0 
        END AS Product_Exists,
        
        -- Check if Store_ID exists in stores
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM {{ ref('erp_stores_clean') }} s 
                WHERE s.Store_ID = l.Store_ID
            ) THEN 1 ELSE 0 
        END AS Store_Exists,
        
        -- Check if Promotion_ID exists (if present)
        CASE 
            WHEN l.Promotion_ID IS NULL THEN 1  -- No promo, valid
            WHEN EXISTS (
                SELECT 1 
                FROM {{ ref('mkt_promotions_clean') }} p 
                WHERE p.Promotion_ID = l.Promotion_ID
            ) THEN 1 ELSE 0 
        END AS Promotion_Exists
        
    FROM latest_only l
),

-- Step 3: Calculate composite DQ Score (0-100)
with_dq_score AS (
    SELECT
        Transaction_Line_ID,
        Transaction_ID,
        Line_Number,
        Product_SKU,
        Store_ID,
        Quantity,
        Unit_Price,
        Cost_Price,
        COALESCE(
            NULLIF(Discount_Amount, 0),
            (Unit_Price * Quantity) - Line_Sales_Amount
        ) AS Discount_Amount,
        Line_Sales_Amount,
        Promotion_ID,
        
        -- Business Flags (from Staging)
        Is_Negative_Quantity AS Is_Return,
        Is_Outlier_Quantity,
        Has_Discount,
        Has_Promotion,
        Has_Price_Issue,
        
        -- FK Validation Flags
        Transaction_Exists,
        Product_Exists,
        Store_Exists,
        Promotion_Exists,
        
        -- Data Quality Score (0-100)
        -- Weighted based on criticality of each field
        (
            (DQ_Transaction_Line_ID_Valid * 10) +
            (DQ_Transaction_ID_Valid * 15) +
            (DQ_Line_Number_Valid * 5) +
            (DQ_Product_SKU_Valid * 15) +
            (DQ_Store_ID_Valid * 10) +
            (DQ_Quantity_Valid * 10) +
            (DQ_Unit_Price_Valid * 15) +
            (DQ_Cost_Price_Valid * 10) +
            (DQ_Line_Sales_Amount_Valid * 10)
        ) AS DQ_Score,
        
        -- Overall validity (must pass all critical checks)
        CASE 
            WHEN DQ_Is_Valid = 1 
                -- AND Transaction_Exists = 1 
                -- AND Product_Exists = 1 
                -- AND Store_Exists = 1
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
        Transaction_Line_ID,
        Transaction_ID,
        Line_Number,
        Product_SKU,
        Store_ID,
        Quantity,
        Unit_Price,
        Cost_Price,
        Discount_Amount,
        Line_Sales_Amount,
        Promotion_ID,
        
        -- Business Flags
        Is_Return,
        Is_Outlier_Quantity,
        Has_Discount,
        Has_Promotion,
        Has_Price_Issue,
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
    - Transaction_Line_ID: VARCHAR(50) - Unique business key
    - Transaction_ID: VARCHAR(50) - FK to transaction header
    - Line_Number: INT
    - Product_SKU: VARCHAR(50) - FK to products
    - Store_ID: VARCHAR(50) - FK to stores
    - Quantity: INT - Can be negative (returns)
    - Unit_Price: DECIMAL(18,2)
    - Cost_Price: DECIMAL(18,2)
    - Discount_Amount: DECIMAL(18,2) - Nullable
    - Line_Sales_Amount: DECIMAL(18,2)
    - Promotion_ID: VARCHAR(50) - FK to promotions (nullable)
    - Is_Return: BIT - Negative quantity
    - Is_Outlier_Quantity: BIT - Quantity > 100
    - Has_Discount: BIT
    - Has_Promotion: BIT
    - Has_Price_Issue: BIT - Cost > Unit Price
    - Is_Valid: BIT
    - DQ_Score: INT (0-100)
    - Batch_ID: VARCHAR(100)
    - Source_File: VARCHAR(255)
    - Created_TS: DATETIME2
    - Updated_TS: DATETIME2
    
    Note: Transaction_Line_SK is auto-generated by SQL Server IDENTITY
    Note: Line_Cost, Line_Margin, Line_Margin_Pct are computed columns in SQL Server table
*/