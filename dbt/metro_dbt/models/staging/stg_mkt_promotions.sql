{{
    config(
        materialized = 'table',
        schema = 'Staging',
        full_refresh = true,
        transient = false,
        
    )
}}

/*
    Staging Model: Marketing Promotions
    Source: raw.mkt_promotions
    Target: Staging.stg_mkt_promotions

    Staging Layer Transformations:
    - Column renaming: None needed
    - Type casting: DATE (Start_Date, End_Date), DECIMAL (Promo_Cost)
    - Text normalization: UPPER (Promo_Type)
    - Currency parsing
    
    NOT in Staging (deferred to Silver):
    - Overlapping promotion detection (same SKU, overlapping dates)
    - Eligible_SKUs parsing (pipe-separated list → array/table)
    - Promo_Type standardization
    - Date range validation (End_Date >= Start_Date)
    
    Data Issues:
    - Overlapping promotions (same product, overlapping date ranges)
    - Eligible_SKUs is pipe-separated list: "SKU001|SKU002|SKU003"
    - Currency symbols in Promo_Cost
    - Some promotions have End_Date < Start_Date
*/

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'mkt_promotions') }}
),

cleaned AS (
    SELECT
        Raw_ID,
        
        -- Business Key
        UPPER(LTRIM(RTRIM(Promotion_ID))) AS Promotion_ID,
        
        -- Promo Name
        NULLIF(LTRIM(RTRIM(Promo_Name)), '') AS Promo_Name,
        
        -- Promo Type - Normalize
        UPPER(NULLIF(LTRIM(RTRIM(Promo_Type)), '')) AS Promo_Type,
        
        -- Start Date
        TRY_CAST(Start_Date AS DATE) AS Start_Date,
        
        -- End Date
        TRY_CAST(End_Date AS DATE) AS End_Date,
        
        -- Promo Cost - Remove currency
        TRY_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(Promo_Cost, '$', ''), '€', ''), ',', ''), ' ', '')
            AS DECIMAL(18,2)
        ) AS Promo_Cost,
        
        -- Eligible SKUs - Keep as is, defer parsing to Silver
        Eligible_SKUs,
        
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
            WHEN Promotion_ID IS NOT NULL AND LEN(Promotion_ID) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Promotion_ID_Valid,
        
        CASE 
            WHEN Promo_Name IS NOT NULL AND LEN(Promo_Name) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Promo_Name_Valid,
        
        CASE 
            WHEN Promo_Type IS NOT NULL AND LEN(Promo_Type) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Promo_Type_Valid,
        
        CASE 
            WHEN Start_Date IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Start_Date_Valid,
        
        CASE 
            WHEN End_Date IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_End_Date_Valid,
        
        CASE 
            WHEN Promo_Cost IS NOT NULL AND Promo_Cost >= 0 
            THEN 1 ELSE 0 
        END AS DQ_Promo_Cost_Valid,
        
        CASE 
            WHEN Start_Date IS NOT NULL 
                AND End_Date IS NOT NULL 
                AND End_Date >= Start_Date 
            THEN 1 ELSE 0 
        END AS DQ_Date_Range_Valid,
        
        -- Business Flags
        CASE 
            WHEN End_Date IS NOT NULL 
                AND Start_Date IS NOT NULL 
                AND End_Date < Start_Date 
            THEN 1 ELSE 0 
        END AS Has_Invalid_Date_Range,
        
        CASE 
            WHEN Eligible_SKUs IS NOT NULL 
                AND Eligible_SKUs LIKE '%|%' 
            THEN 1 ELSE 0 
        END AS Has_Multiple_SKUs,
        
        -- Overall Validity
        CASE 
            WHEN Promotion_ID IS NOT NULL AND LEN(Promotion_ID) > 0
                AND Promo_Name IS NOT NULL AND LEN(Promo_Name) > 0
                AND Start_Date IS NOT NULL
                AND End_Date IS NOT NULL
                AND End_Date >= Start_Date
            THEN 1 ELSE 0 
        END AS DQ_Is_Valid
        
    FROM cleaned
),

final AS (
    SELECT *
    FROM with_dq_flags
)

SELECT * FROM final

