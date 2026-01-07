{{
    config(
        materialized = 'table',
        schema = 'Staging',
        full_refresh = true,
        transient = false,
        
    )
}}

/*
    Staging Model: API Weather
    Source: raw.api_weather
    Target: Staging.stg_api_weather

    Staging Layer Transformations:
    - Column renaming: None needed
    - Type casting: DATE (Weather_Date), INT (Retail_Location_ID), DECIMAL (Temperature, Precipitation)
    - Text normalization: UPPER (Weather_Condition)
    
    NOT in Staging (deferred to Silver):
    - Retail_Location_ID → Store_ID mapping (intentional key mismatch)
    - Missing Temperature_C imputation (~2% nulls)
    - Weather_Condition standardization
    - Outlier detection (extreme temperatures)
    
    Data Issues:
    - Retail_Location_ID doesn't match Store_ID exactly (intentional)
    - Temperature_C has ~2% missing values
    - Some extreme temperatures (e.g., < -20°C, > 45°C)
*/

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'api_weather') }}
),

cleaned AS (
    SELECT
        Raw_ID,
        
        -- Weather Date
        TRY_CAST(Weather_Date AS DATE) AS Weather_Date,
        
        -- Retail Location ID (this is Store_ID from source, but renamed)
        UPPER(LTRIM(RTRIM(Retail_Location_ID))) AS Retail_Location_ID,
        
        -- Temperature - Has ~2% nulls intentionally
        TRY_CAST(Temperature_C AS DECIMAL(5,2)) AS Temperature_C,
        
        -- Precipitation
        TRY_CAST(Precipitation_mm AS DECIMAL(5,2)) AS Precipitation_mm,
        
        -- Weather Condition - Normalize
        UPPER(NULLIF(LTRIM(RTRIM(Weather_Condition)), '')) AS Weather_Condition,
        
        -- Metadata
        Batch_ID,
        Source_File,
        Load_Timestamp
        
    FROM source
),

with_dq_flags AS (
    SELECT
        *,
        
        -- Data Quality Flags
        CASE 
            WHEN Weather_Date IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Weather_Date_Valid,
        
        CASE 
            WHEN Retail_Location_ID IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Retail_Location_ID_Valid,
        
        CASE 
            WHEN Temperature_C IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Temperature_Valid,
        
        CASE 
            WHEN Precipitation_mm IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Precipitation_Valid,
        
        CASE 
            WHEN Weather_Condition IS NOT NULL AND LEN(Weather_Condition) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Weather_Condition_Valid,
        
        -- Business Flags
        CASE 
            WHEN Temperature_C IS NULL 
            THEN 1 ELSE 0 
        END AS Has_Missing_Temperature,
        
        CASE 
            WHEN Temperature_C IS NOT NULL 
                AND (Temperature_C < -20 OR Temperature_C > 45) 
            THEN 1 ELSE 0 
        END AS Is_Extreme_Temperature,
        
        -- Overall Validity
        CASE 
            WHEN Weather_Date IS NOT NULL
                AND Retail_Location_ID IS NOT NULL
                AND Weather_Condition IS NOT NULL AND LEN(Weather_Condition) > 0
            THEN 1 ELSE 0 
        END AS DQ_Is_Valid
        
    FROM cleaned
),

final AS (
    SELECT *
    FROM with_dq_flags
)

SELECT * FROM final

