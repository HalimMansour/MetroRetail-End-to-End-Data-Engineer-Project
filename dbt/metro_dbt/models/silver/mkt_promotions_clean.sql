{{
    config(
        materialized = 'table',
        schema = 'Silver',
        alias = 'mkt_promotions_clean',
        full_refresh = true,
        transient = false,
    )
}}

/*
    Silver Model: Marketing Promotions Clean
    Source: Staging.stg_mkt_promotions
    Target: Silver.mkt_promotions_clean
+
    Silver Layer Transformations:
    ✅ Deduplicate by Promotion_ID (keep latest)
    ✅ Standardize Promo_Type (DISCOUNT/BOGO/BUNDLE/REDUCTION)
    ✅ Validate date ranges (End_Date >= Start_Date)
    ✅ Calculate promotion duration
    ✅ Keep Eligible_SKUs as-is (parsing deferred to Gold)
    ✅ Calculate composite DQ Score (0-100)
    ✅ Only keep valid records
    
    Promo_Type Standardization:
    - "Discount", "discount", "DISCOUNT%" → "DISCOUNT"
    - "BOGO", "Buy One Get One" → "BOGO"
    - "Bundle", "bundle deal" → "BUNDLE"
    - "Reduction", "price reduction" → "REDUCTION"
    - Others → "OTHER"
    
    Business Rules:
    - Promotion_ID is unique (deduplicate if needed)
    - End_Date must be >= Start_Date
    - Promo_Cost must be >= 0
    - Eligible_SKUs remains pipe-separated (e.g., "SKU001|SKU002|SKU003")
*/

WITH source AS (
    SELECT *
    FROM {{ ref('stg_mkt_promotions') }}
    WHERE DQ_Is_Valid = 1  -- Only valid rows from Staging
        AND DQ_Date_Range_Valid = 1  -- Must have valid date range
),

-- Step 1: Deduplicate - Keep latest record per Promotion_ID
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Promotion_ID 
            ORDER BY Load_Timestamp DESC
        ) AS row_num
    FROM source
),

latest_only AS (
    SELECT *
    FROM deduplicated
    WHERE row_num = 1
),

-- Step 2: Standardize Promo_Type
with_standardization AS (
    SELECT
        Promotion_ID,
        Promo_Name,
        
        -- Standardize Promo_Type (already uppercase from Staging)
        CASE 
            WHEN Promo_Type LIKE '%DISCOUNT%' THEN 'DISCOUNT'
            WHEN Promo_Type LIKE '%BOGO%' THEN 'BOGO'
            WHEN Promo_Type LIKE '%BUY%ONE%GET%' THEN 'BOGO'
            WHEN Promo_Type LIKE '%BUNDLE%' THEN 'BUNDLE'
            WHEN Promo_Type LIKE '%REDUCTION%' THEN 'REDUCTION'
            WHEN Promo_Type LIKE '%PRICE%CUT%' THEN 'REDUCTION'
            WHEN Promo_Type LIKE '%SALE%' THEN 'DISCOUNT'
            WHEN Promo_Type LIKE '%OFF%' THEN 'DISCOUNT'
            ELSE 'OTHER'
        END AS Promo_Type,
        
        Start_Date,
        End_Date,
        Promo_Cost,
        Eligible_SKUs,
        
        -- Carry forward DQ flags from Staging
        DQ_Promotion_ID_Valid,
        DQ_Promo_Name_Valid,
        DQ_Promo_Type_Valid,
        DQ_Start_Date_Valid,
        DQ_End_Date_Valid,
        DQ_Promo_Cost_Valid,
        DQ_Date_Range_Valid,
        Has_Invalid_Date_Range,
        Has_Multiple_SKUs,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM latest_only
),

-- Step 3: Calculate composite DQ Score (0-100)
with_dq_score AS (
    SELECT
        Promotion_ID,
        Promo_Name,
        Promo_Type,
        Start_Date,
        End_Date,
        Promo_Cost,
        Eligible_SKUs,
        
        -- Business Flags
        Has_Multiple_SKUs,
        Has_Invalid_Date_Range,
        
        -- Data Quality Score (0-100)
        -- Weighted based on criticality of each field
        (
            (DQ_Promotion_ID_Valid * 20) +       -- 20% - Promotion ID is critical
            (DQ_Promo_Name_Valid * 15) +         -- 15% - Promo Name is important
            (DQ_Promo_Type_Valid * 15) +         -- 15% - Promo Type is important
            (DQ_Start_Date_Valid * 15) +         -- 15% - Start Date is critical
            (DQ_End_Date_Valid * 15) +           -- 15% - End Date is critical
            (DQ_Promo_Cost_Valid * 10) +         -- 10% - Promo Cost is important
            (DQ_Date_Range_Valid * 10)           -- 10% - Date range validity
        ) AS DQ_Score,
        
        -- Overall validity (all critical checks must pass)
        CASE 
            WHEN DQ_Promotion_ID_Valid = 1 
                AND DQ_Promo_Name_Valid = 1
                AND DQ_Start_Date_Valid = 1
                AND DQ_End_Date_Valid = 1
                AND DQ_Date_Range_Valid = 1
                AND DQ_Promo_Cost_Valid = 1
            THEN 1 ELSE 0 
        END AS Is_Valid,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM with_standardization
),

-- Step 4: Final select with all required columns
final AS (
    SELECT
        Promotion_ID,
        Promo_Name,
        Promo_Type,
        Start_Date,
        End_Date,
        Promo_Cost,
        COALESCE(UPPER(LTRIM(RTRIM(Eligible_SKUs))), 'N/A') AS Eligible_SKUs,
        
        -- Business Flags
        Has_Multiple_SKUs,
        Has_Invalid_Date_Range,
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
    - Promotion_ID: VARCHAR(50) - Unique business key
    - Promo_Name: VARCHAR(255)
    - Promo_Type: VARCHAR(50) - Standardized (DISCOUNT/BOGO/BUNDLE/REDUCTION/OTHER)
    - Start_Date: DATE
    - End_Date: DATE
    - Promo_Cost: DECIMAL(18,2) - Must be >= 0
    - Eligible_SKUs: NVARCHAR(MAX) - Pipe-separated list (parsing in Gold)
    - Has_Multiple_SKUs: BIT
    - Has_Invalid_Date_Range: BIT - Should be 0 (filtered out)
    - Is_Valid: BIT
    - DQ_Score: INT (0-100)
    - Batch_ID: VARCHAR(100)
    - Source_File: VARCHAR(255)
    - Created_TS: DATETIME2
    - Updated_TS: DATETIME2
    
    Note: Promotion_SK (surrogate key) is auto-generated by SQL Server IDENTITY
    Note: Promo_Duration_Days and Is_Active are computed columns in SQL Server table
    
    Sample Eligible_SKUs:
    - "SKU001|SKU002|SKU003" (pipe-separated)
    - NULL (store-level promo, no specific SKUs)
    
    Promo_Type Mapping:
    - "25% OFF DISCOUNT" → "DISCOUNT"
    - "BOGO SPECIAL" → "BOGO"
    - "BUNDLE DEAL" → "BUNDLE"
    - "PRICE REDUCTION" → "REDUCTION"
*/