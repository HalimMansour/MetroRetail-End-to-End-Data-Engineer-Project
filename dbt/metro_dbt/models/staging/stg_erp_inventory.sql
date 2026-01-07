{{
    config(
        materialized = 'table',
        schema = 'Staging',
        full_refresh = true,
        transient = false,
        
    )
}}

/*
    Staging Model: ERP Inventory
    Source: raw.erp_inventory
    Target: Staging.stg_erp_inventory

    Staging Layer Transformations:
    - Column renaming: None needed
    - Type casting: INT (Store_ID, Quantity_On_Hand, Reorder_Level), DATE (Snapshot_Date)
    - Basic validation: Non-null checks
    
    NOT in Staging (deferred to Silver):
    - Negative Quantity_On_Hand handling (data errors)
    - Outlier detection (e.g., extremely high quantities)
    - Product_SKU existence validation
    - Store_ID existence validation
    - Snapshot_Date business logic (most recent per SKU/Store)
    
    Data Issues:
    - Negative Quantity_On_Hand (~1-2% of rows)
    - Outlier quantities (e.g., >10,000 units)
    - Some Reorder_Level = 0
*/

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'erp_inventory') }}
),

cleaned AS (
    SELECT
        Raw_ID,
        
        -- Business Key
        UPPER(LTRIM(RTRIM(Inventory_ID))) AS Inventory_ID,
        
        -- Product SKU
        UPPER(LTRIM(RTRIM(Product_SKU))) AS Product_SKU,
        
        -- Store ID
        UPPER(LTRIM(RTRIM(Store_ID))) AS Store_ID,
        
        -- Snapshot Date
        TRY_CAST(Snapshot_Date AS DATE) AS Snapshot_Date,
        
        -- Quantity On Hand (includes negatives)
        TRY_CAST(Quantity_On_Hand AS INT) AS Quantity_On_Hand,
        
        -- Reorder Level
        TRY_CAST(Reorder_Level AS INT) AS Reorder_Level,
        
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
            WHEN Inventory_ID IS NOT NULL AND LEN(Inventory_ID) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Inventory_ID_Valid,
        
        CASE 
            WHEN Product_SKU IS NOT NULL AND LEN(Product_SKU) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Product_SKU_Valid,
        
        CASE 
            WHEN Store_ID IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Store_ID_Valid,
        
        CASE 
            WHEN Snapshot_Date IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Snapshot_Date_Valid,
        
        CASE 
            WHEN Quantity_On_Hand IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Quantity_On_Hand_Valid,
        
        CASE 
            WHEN Reorder_Level IS NOT NULL AND Reorder_Level >= 0 
            THEN 1 ELSE 0 
        END AS DQ_Reorder_Level_Valid,
        
        -- Business Flags
        CASE 
            WHEN Quantity_On_Hand < 0 
            THEN 1 ELSE 0 
        END AS Is_Negative_Quantity,
        
        CASE 
            WHEN Quantity_On_Hand > 10000 
            THEN 1 ELSE 0 
        END AS Is_Outlier_Quantity,
        
        CASE 
            WHEN Quantity_On_Hand IS NOT NULL 
                AND Reorder_Level IS NOT NULL 
                AND Quantity_On_Hand < Reorder_Level 
            THEN 1 ELSE 0 
        END AS Is_Below_Reorder_Level,
        
        -- Overall Validity
        CASE 
            WHEN Inventory_ID IS NOT NULL AND LEN(Inventory_ID) > 0
                AND Product_SKU IS NOT NULL AND LEN(Product_SKU) > 0
                AND Store_ID IS NOT NULL
                AND Snapshot_Date IS NOT NULL
                AND Quantity_On_Hand IS NOT NULL
            THEN 1 ELSE 0 
        END AS DQ_Is_Valid
        
    FROM cleaned
),

final AS (
    SELECT *
    FROM with_dq_flags
)

SELECT * FROM final
