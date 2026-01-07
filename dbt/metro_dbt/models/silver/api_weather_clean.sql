{{
    config(
        materialized = 'table',
        schema = 'Silver',
        alias = 'api_weather_clean',
        full_refresh = true,
        transient = false,
    )
}}

/*
    Silver Model: API Weather Clean
    Source: Staging.stg_api_weather
    Target: Silver.api_weather_clean

    Silver Layer Transformations:
    ✅ Map Retail_Location_ID → Store_ID (resolve intentional key mismatch)
    ✅ Deduplicate by Weather_Date + Store_ID (keep latest)
    ✅ Standardize Weather_Condition (group similar conditions)
    ✅ Calculate composite DQ Score (0-100)
    ✅ Flag extreme temperatures and missing values
    ✅ Only keep valid records
    
    Key Mapping Rules:
    - Retail_Location_ID format: "LOC_S001"
    - Store_ID format: "S001"
    - Mapping: Remove "LOC_" prefix
    
    Business Rules:
    - Temperature_C can be NULL (~2% expected)
    - Extreme temps: < -20°C or > 45°C
    - Precipitation must be >= 0
*/

WITH source AS (
    SELECT *
    FROM {{ ref('stg_api_weather') }}
    WHERE DQ_Is_Valid = 1  -- Only valid rows from Staging
),

-- Step 1: Map Retail_Location_ID → Store_ID (resolve key mismatch)
with_store_mapping AS (
    SELECT
        Weather_Date,
        
        -- Map Retail_Location_ID to Store_ID
        -- Remove "LOC_" prefix if present
        CASE 
            WHEN Retail_Location_ID LIKE 'LOC_%' THEN 
                REPLACE(Retail_Location_ID, 'LOC_', '')
            ELSE Retail_Location_ID
        END AS Store_ID,
        
        Temperature_C,
        Precipitation_mm,
        Weather_Condition,
        
        -- Carry forward DQ flags from Staging
        DQ_Weather_Date_Valid,
        DQ_Retail_Location_ID_Valid,
        DQ_Temperature_Valid,
        DQ_Precipitation_Valid,
        DQ_Weather_Condition_Valid,
        Has_Missing_Temperature,
        Is_Extreme_Temperature,
        
        -- Metadata
        Batch_ID,
        Source_File,
        Load_Timestamp
        
    FROM source
),

-- Step 2: Deduplicate - Keep latest record per Weather_Date + Store_ID
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Weather_Date, Store_ID 
            ORDER BY Load_Timestamp DESC
        ) AS row_num
    FROM with_store_mapping
),

latest_only AS (
    SELECT *
    FROM deduplicated
    WHERE row_num = 1
),

-- Step 3: Standardize Weather_Condition (group similar conditions)
with_standardization AS (
    SELECT
        Weather_Date,
        Store_ID,
        Temperature_C,
        Precipitation_mm,
        
        -- Standardize Weather_Condition (already uppercase from Staging)
        CASE 
            WHEN Weather_Condition LIKE '%CLEAR%' THEN 'CLEAR'
            WHEN Weather_Condition LIKE '%MAIN%CLEAR%' THEN 'CLEAR'
            WHEN Weather_Condition LIKE '%PARTLY%CLOUD%' THEN 'PARTLY CLOUDY'
            WHEN Weather_Condition LIKE '%CLOUD%' THEN 'CLOUDY'
            WHEN Weather_Condition LIKE '%OVERCAST%' THEN 'CLOUDY'
            WHEN Weather_Condition LIKE '%RAIN%' THEN 'RAINY'
            WHEN Weather_Condition LIKE '%DRIZZLE%' THEN 'RAINY'
            WHEN Weather_Condition LIKE '%SNOW%' THEN 'SNOWY'
            WHEN Weather_Condition LIKE '%STORM%' THEN 'STORMY'
            WHEN Weather_Condition LIKE '%THUNDER%' THEN 'STORMY'
            WHEN Weather_Condition LIKE '%FOG%' THEN 'FOGGY'
            ELSE Weather_Condition
        END AS Weather_Condition,
        
        -- Business Flags
        Has_Missing_Temperature,
        Is_Extreme_Temperature,
        
        -- DQ Flags from Staging
        DQ_Weather_Date_Valid,
        DQ_Retail_Location_ID_Valid,
        DQ_Temperature_Valid,
        DQ_Precipitation_Valid,
        DQ_Weather_Condition_Valid,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM latest_only
),

-- Step 4: Calculate composite DQ Score (0-100)
with_dq_score AS (
    SELECT
        Weather_Date,
        Store_ID,
        Temperature_C,
        Precipitation_mm,
        Weather_Condition,
        
        -- Business Flags
        Has_Missing_Temperature,
        Is_Extreme_Temperature,
        
        -- Data Quality Score (0-100)
        -- Weighted based on criticality of each field
        (
            (DQ_Weather_Date_Valid * 25) +           -- 25% - Date is critical
            (DQ_Retail_Location_ID_Valid * 25) +     -- 25% - Location is critical
            (DQ_Temperature_Valid * 20) +            -- 20% - Temp can be null (~2%)
            (DQ_Precipitation_Valid * 15) +          -- 15% - Precip must be present
            (DQ_Weather_Condition_Valid * 15)        -- 15% - Condition must be present
        ) AS DQ_Score,
        
        -- Overall validity (all critical checks must pass)
        CASE 
            WHEN DQ_Weather_Date_Valid = 1 
                AND DQ_Retail_Location_ID_Valid = 1
                AND DQ_Weather_Condition_Valid = 1
                AND DQ_Precipitation_Valid = 1
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
        Weather_Date,
        Store_ID,
        Temperature_C,
        Precipitation_mm,
        Weather_Condition,
        
        -- Business Flags
        Has_Missing_Temperature,
        Is_Extreme_Temperature,
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
    - Weather_Date: DATE
    - Store_ID: VARCHAR(50) - Mapped from Retail_Location_ID
    - Temperature_C: DECIMAL(5,2) - Can be NULL (~2%)
    - Precipitation_mm: DECIMAL(5,2)
    - Weather_Condition: VARCHAR(100) - Standardized
    - Has_Missing_Temperature: BIT
    - Is_Extreme_Temperature: BIT
    - Is_Valid: BIT
    - DQ_Score: INT (0-100)
    - Batch_ID: VARCHAR(100)
    - Source_File: VARCHAR(255)
    - Created_TS: DATETIME2
    - Updated_TS: DATETIME2
    
    Note: Weather_SK (surrogate key) is auto-generated by SQL Server IDENTITY
    Note: Is_Rainy is computed column in SQL Server table definition
*/