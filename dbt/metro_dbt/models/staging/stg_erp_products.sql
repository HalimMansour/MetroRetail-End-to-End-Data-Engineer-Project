{{
    config(
        materialized = 'table',
        schema = 'Staging',
        full_refresh = true,
        transient = false,
    
    )
}}

/*
    Staging Model: ERP Products
    Source: raw.erp_products
    Target: Staging.stg_erp_products

    Staging Layer Transformations:
    - Column renaming: None needed
    - Type casting: Decimals (Price, Cost_Price), Date (Last_Updated)
    - Text normalization: UPPER for Category, Sub_Category
    - Currency parsing: Remove $, commas
    
    NOT in Staging (deferred to Silver):
    - Duplicate Product_SKU handling
    - Category/Sub_Category standardization (typos: "Electroncs", "Beverage")
    - Supplier_ID referential integrity
    - Price validation (Price > Cost_Price)
    
    Data Issues:
    - Duplicate Product_SKUs (~5-10 duplicates)
    - Category typos: "Electroncs", "Beverage" vs "Beverages"
    - Sub_Category inconsistencies
    - Mixed case in text fields
    - Currency symbols in prices
*/

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'erp_products') }}
),

cleaned AS (
    SELECT
        Raw_ID,
        
        -- Business Key
        UPPER(LTRIM(RTRIM(Product_SKU))) AS Product_SKU,
        
        -- Product Name - Clean and trim
        NULLIF(LTRIM(RTRIM(Product_Name)), '') AS Product_Name,
        
        -- Category - Normalize to uppercase
        UPPER(NULLIF(LTRIM(RTRIM(Category)), '')) AS Category,
        
        -- Sub_Category - Normalize to uppercase
        UPPER(NULLIF(LTRIM(RTRIM(Sub_Category)), '')) AS Sub_Category,
        
        -- Price - Remove currency symbols
        TRY_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(Price, '$', ''), '€', ''), ',', ''), ' ', '')
            AS DECIMAL(18,2)
        ) AS Price,
        
        -- Cost_Price - Remove currency symbols
        TRY_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(Cost_Price, '$', ''), '€', ''), ',', ''), ' ', '')
            AS DECIMAL(18,2)
        ) AS Cost_Price,
        
        -- Supplier ID
        UPPER(LTRIM(RTRIM(Supplier_ID))) AS Supplier_ID,
        
        -- Last Updated - Parse date
        TRY_CAST(Last_Updated AS DATE) AS Last_Updated,
        
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
            WHEN Product_SKU IS NOT NULL AND LEN(Product_SKU) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Product_SKU_Valid,
        
        CASE 
            WHEN Product_Name IS NOT NULL AND LEN(Product_Name) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Product_Name_Valid,
        
        CASE 
            WHEN Category IS NOT NULL AND LEN(Category) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Category_Valid,
        
        CASE 
            WHEN Sub_Category IS NOT NULL AND LEN(Sub_Category) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Sub_Category_Valid,
        
        CASE 
            WHEN Price IS NOT NULL AND Price > 0 
            THEN 1 ELSE 0 
        END AS DQ_Price_Valid,
        
        CASE 
            WHEN Cost_Price IS NOT NULL AND Cost_Price >= 0 
            THEN 1 ELSE 0 
        END AS DQ_Cost_Price_Valid,
        
        CASE 
            WHEN Supplier_ID IS NOT NULL AND LEN(Supplier_ID) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Supplier_ID_Valid,
        
        CASE 
            WHEN Last_Updated IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Last_Updated_Valid,
        
        -- Business Flags
        CASE 
            WHEN Price IS NOT NULL AND Cost_Price IS NOT NULL 
                AND Cost_Price > Price 
            THEN 1 ELSE 0 
        END AS Has_Price_Issue,
        
        -- Overall Validity
        CASE 
            WHEN Product_SKU IS NOT NULL AND LEN(Product_SKU) > 0
                AND Product_Name IS NOT NULL AND LEN(Product_Name) > 0
                AND Category IS NOT NULL AND LEN(Category) > 0
                AND Price IS NOT NULL AND Price > 0
                AND Supplier_ID IS NOT NULL AND LEN(Supplier_ID) > 0
            THEN 1 ELSE 0 
        END AS DQ_Is_Valid
        
    FROM cleaned
),

final AS (
    SELECT *
    FROM with_dq_flags
)

SELECT * FROM final

