{{
    config(
        materialized = 'table',
        schema = 'Staging',
        full_refresh = true,
        transient = false,
        
    )
}}

/*
    Staging Model: ERP Stores
    Source: raw.erp_stores
    Target: Staging.stg_erp_stores

    Staging Layer Transformations:
    - Column renaming: None needed
    - Type casting: INT (Store_ID), DECIMAL (Store_Area_sqm), DATE (Open_Date)
    - Text normalization: Trim and clean
    
    NOT in Staging (deferred to Silver):
    - Multi-value Store_Manager parsing (e.g., "John Doe; Jane Smith")
    - City/Region standardization
    - Store_Area_sqm validation (must be > 0)
    
    Data Issues:
    - Store_Manager contains multiple managers separated by semicolons
    - Store_Area_sqm has commas (e.g., "1,200.50")
    - Mixed case in City, Region
    - Some Store_IDs may be non-numeric (validation needed)
*/

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'erp_stores') }}
),

cleaned AS (
    SELECT
        Raw_ID,
        
        -- Business Key -
        LTRIM(RTRIM(Store_ID)) AS Store_ID,
        
        -- Store Name
        NULLIF(LTRIM(RTRIM(Store_Name)), '') AS Store_Name,
        
        -- City
        NULLIF(LTRIM(RTRIM(City)), '') AS City,
        
        -- Region
        NULLIF(LTRIM(RTRIM(Region)), '') AS Region,
        
        -- Store Manager - Keep as is, defer multi-value parsing to Silver
        NULLIF(LTRIM(RTRIM(Store_Manager)), '') AS Store_Manager,
        
        -- Store Area - Remove commas
        TRY_CAST(
            REPLACE(REPLACE(Store_Area_sqm, ',', ''), ' ', '')
            AS DECIMAL(10,2)
        ) AS Store_Area_sqm,
        
        -- Open Date
        TRY_CAST(Open_Date AS DATE) AS Open_Date,
        
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
            WHEN Store_ID IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Store_ID_Valid,
        
        CASE 
            WHEN Store_Name IS NOT NULL AND LEN(Store_Name) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Store_Name_Valid,
        
        CASE 
            WHEN City IS NOT NULL AND LEN(City) > 0 
            THEN 1 ELSE 0 
        END AS DQ_City_Valid,
        
        CASE 
            WHEN Region IS NOT NULL AND LEN(Region) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Region_Valid,
        
        CASE 
            WHEN Store_Area_sqm IS NOT NULL AND Store_Area_sqm > 0 
            THEN 1 ELSE 0 
        END AS DQ_Store_Area_Valid,
        
        CASE 
            WHEN Open_Date IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Open_Date_Valid,
        
        -- Business Flags
        CASE 
            WHEN Store_Manager IS NOT NULL AND Store_Manager LIKE '%;%' 
            THEN 1 ELSE 0 
        END AS Has_Multiple_Managers,
        
        -- Overall Validity
        CASE 
            WHEN Store_ID IS NOT NULL
                AND Store_Name IS NOT NULL AND LEN(Store_Name) > 0
                AND City IS NOT NULL AND LEN(City) > 0
                AND Region IS NOT NULL AND LEN(Region) > 0
            THEN 1 ELSE 0 
        END AS DQ_Is_Valid
        
    FROM cleaned
),

final AS (
    SELECT *
    FROM with_dq_flags
)

SELECT * FROM final


