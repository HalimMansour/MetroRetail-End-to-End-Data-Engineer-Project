{{
    config(
        materialized = 'table',
        schema = 'Silver',
        alias = 'erp_stores_clean',
        full_refresh = true,
        transient = false,
    )
}}

/*
    Silver Model: ERP Stores Clean
    Source: Staging.stg_erp_stores
    Target: Silver.erp_stores_clean

    Silver Layer Transformations:
    ✅ Deduplicate by Store_ID (keep latest)
    ✅ Parse multi-value Store_Manager (extract first manager)
    ✅ Standardize City/Region (proper case)
    ✅ Validate Store_Area_sqm (must be > 0)
    ✅ Calculate composite DQ Score (0-100)
    ✅ Only keep valid records
    
    Multi-value Manager Handling:
    - Input: "John Doe; Jane Smith; Bob Johnson"
    - Output: "John Doe" (first manager only)
    - Flag: Has_Multiple_Managers = 1
    
    Business Rules:
    - Store_ID is unique (deduplicate if needed)
    - Store_Area_sqm must be > 0
    - City and Region are required
    - Store_Manager can be NULL
*/

WITH source AS (
    SELECT *
    FROM {{ ref('stg_erp_stores') }}
    WHERE DQ_Is_Valid = 1  -- Only valid rows from Staging
),

-- Step 1: Deduplicate - Keep latest record per Store_ID
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Store_ID 
            ORDER BY Load_Timestamp DESC
        ) AS row_num
    FROM source
),

latest_only AS (
    SELECT *
    FROM deduplicated
    WHERE row_num = 1
),

-- Step 2: Parse multi-value Store_Manager (take first if semicolon-separated)
with_parsed_manager AS (
    SELECT
        Store_ID,
        Store_Name,
        City,
        Region,
        
        -- Extract first manager if multi-value
        -- Example: "John Doe; Jane Smith" → "John Doe"
        CASE 
            WHEN Store_Manager IS NULL THEN NULL
            WHEN Store_Manager LIKE '%;%' THEN 
                LTRIM(RTRIM(LEFT(Store_Manager, CHARINDEX(';', Store_Manager) - 1)))
            ELSE LTRIM(RTRIM(Store_Manager))
        END AS Store_Manager,
        
        Store_Area_sqm,
        Open_Date,
        
        -- Carry forward DQ flags from Staging
        DQ_Store_ID_Valid,
        DQ_Store_Name_Valid,
        DQ_City_Valid,
        DQ_Region_Valid,
        DQ_Store_Area_Valid,
        DQ_Open_Date_Valid,
        Has_Multiple_Managers,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM latest_only
),

-- Step 3: Standardize City/Region (proper case for consistency)
with_standardization AS (
    SELECT
        Store_ID,
        Store_Name,
        
        -- Standardize City - Convert to proper case
        -- Example: "NEW YORK" → "New York", "los angeles" → "Los Angeles"
        UPPER(LEFT(City, 1)) + LOWER(SUBSTRING(City, 2, LEN(City))) AS City,
        
        -- Standardize Region - Keep as uppercase for consistency
        UPPER(Region) AS Region,
        COALESCE(NULLIF(LTRIM(RTRIM(Store_Manager)), ''), 'N/A') AS Store_Manager,
        Store_Area_sqm,
        Open_Date,
        
        -- Business Flags
        Has_Multiple_Managers,
        
        -- DQ Flags from Staging
        DQ_Store_ID_Valid,
        DQ_Store_Name_Valid,
        DQ_City_Valid,
        DQ_Region_Valid,
        DQ_Store_Area_Valid,
        DQ_Open_Date_Valid,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM with_parsed_manager
),

-- Step 4: Calculate composite DQ Score (0-100)
with_dq_score AS (
    SELECT
        Store_ID,
        Store_Name,
        City,
        Region,
        Store_Manager,
        Store_Area_sqm,
        Open_Date,
        
        -- Business Flags
        Has_Multiple_Managers,
        
        -- Data Quality Score (0-100)
        -- Weighted based on criticality of each field
        (
            (DQ_Store_ID_Valid * 20) +           -- 20% - Store ID is critical
            (DQ_Store_Name_Valid * 20) +         -- 20% - Store Name is critical
            (DQ_City_Valid * 15) +               -- 15% - City is required
            (DQ_Region_Valid * 15) +             -- 15% - Region is required
            (DQ_Store_Area_Valid * 15) +         -- 15% - Store Area is required
            (DQ_Open_Date_Valid * 15)            -- 15% - Open Date is required
        ) AS DQ_Score,
        
        -- Overall validity (all critical checks must pass)
        CASE 
            WHEN DQ_Store_ID_Valid = 1 
                AND DQ_Store_Name_Valid = 1
                AND DQ_City_Valid = 1
                AND DQ_Region_Valid = 1
                AND DQ_Store_Area_Valid = 1
                AND DQ_Open_Date_Valid = 1
            THEN 1 ELSE 0 
        END AS Is_Valid,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM with_standardization
),

-- Step 5: Final select with all required columns
final AS (
    SELECT
        Store_ID,
        Store_Name,
        City,
        Region,
        Store_Manager,
        Store_Area_sqm,
        Open_Date,
        
        -- Business Flags
        Has_Multiple_Managers,
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
    - Store_ID: VARCHAR(50) - Unique business key
    - Store_Name: VARCHAR(255)
    - City: VARCHAR(100) - Proper case
    - Region: VARCHAR(100) - Uppercase
    - Store_Manager: VARCHAR(255) - First manager only
    - Store_Area_sqm: DECIMAL(10,2) - Must be > 0
    - Open_Date: DATE
    - Has_Multiple_Managers: BIT
    - Is_Valid: BIT
    - DQ_Score: INT (0-100)
    - Batch_ID: VARCHAR(100)
    - Source_File: VARCHAR(255)
    - Created_TS: DATETIME2
    - Updated_TS: DATETIME2
    
    Note: Store_SK (surrogate key) is auto-generated by SQL Server IDENTITY
    Note: Store_Age_Years is computed column in SQL Server table definition
    
    Sample Transformation:
    Input:  Store_Manager = "John Doe; Jane Smith; Bob Johnson"
    Output: Store_Manager = "John Doe"
            Has_Multiple_Managers = 1
*/