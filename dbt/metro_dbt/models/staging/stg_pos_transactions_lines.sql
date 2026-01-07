{{
    config(
        materialized = 'table',
        schema = 'Staging',
        full_refresh = true,
        transient = false,
    )
}}

/*
    Staging Model: POS Transactions Lines
    Source: raw.pos_transactions_lines
    Target: Staging.stg_pos_transactions_lines

    Staging Layer Transformations (per architecture):
    - Column renaming: None needed
    - Type casting: Integers (Line_Number, Quantity), Decimals (prices)
    - Currency parsing: Remove $, €, commas from amounts
    - Basic row validation: DQ flags
    
    NOT in Staging (deferred to Silver):
    - Referential integrity checks (Transaction_ID → header, Product_SKU → products)
    - Store_ID mismatch detection (line vs header)
    - Negative quantity handling (business logic)
    - Line_Sales_Amount recalculation/validation
    - Promotion validity checks
    
    Data Issues:
    - Mixed currency symbols ($, €) in Line_Sales_Amount
    - Negative quantities (~1-2% of rows)
    - Store_ID may differ from header Store_ID
    - Missing Discount_Amount (~12% nulls)
    - Missing Promotion_ID (most rows, expected)
    - Line_Sales_Amount sometimes has currency formatting
    - Outlier quantities (e.g., >100 units)
*/

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'pos_transactions_lines') }}
),

cleaned AS (
    SELECT
        Raw_ID,
        
        -- Business Keys
        UPPER(LTRIM(RTRIM(Transaction_Line_ID))) AS Transaction_Line_ID,
        UPPER(LTRIM(RTRIM(Transaction_ID))) AS Transaction_ID,
        
        -- Line Number (position in transaction)
        TRY_CAST(Line_Number AS INT) AS Line_Number,
        
        -- Product SKU
        UPPER(LTRIM(RTRIM(Product_SKU))) AS Product_SKU,
        
        -- Store ID (may differ from header!)
        UPPER(LTRIM(RTRIM(Store_ID))) AS Store_ID,
        
        -- Quantity (includes negatives and outliers)
        TRY_CAST(Quantity AS INT) AS Quantity,
        
        -- Unit Price - Remove currency symbols
        TRY_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Unit_Price, '$', ''), '€', ''), ',', ''), ' ', ''), '£', '')
            AS DECIMAL(18,2)
        ) AS Unit_Price,
        
        -- Cost Price
        TRY_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Cost_Price, '$', ''), '€', ''), ',', ''), ' ', ''), '£', '')
            AS DECIMAL(18,2)
        ) AS Cost_Price,
        
        -- Discount Amount - Handle NULL, empty, and "NA"
        CASE 
            WHEN Discount_Amount IS NULL THEN NULL
            WHEN UPPER(LTRIM(RTRIM(Discount_Amount))) IN ('', 'NA', 'NULL', 'N/A') THEN NULL
            ELSE TRY_CAST(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Discount_Amount, '$', ''), '€', ''), ',', ''), ' ', ''), '£', '')
                AS DECIMAL(18,2)
            )
        END AS Discount_Amount,
        
        -- Line Sales Amount - Remove currency symbols and formatting
        TRY_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Line_Sales_Amount, '$', ''), '€', ''), ',', ''), ' ', ''), '£', '')
            AS DECIMAL(18,2)
        ) AS Line_Sales_Amount,
        
        -- Promotion ID - Handle NULL, empty, and "NA"
        CASE 
            WHEN Promotion_ID IS NULL THEN NULL
            WHEN UPPER(LTRIM(RTRIM(Promotion_ID))) IN ('', 'NA', 'NULL', 'N/A') THEN NULL
            ELSE UPPER(LTRIM(RTRIM(Promotion_ID)))
        END AS Promotion_ID,
        
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
            WHEN Transaction_Line_ID IS NOT NULL AND LEN(Transaction_Line_ID) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Transaction_Line_ID_Valid,
        
        CASE 
            WHEN Transaction_ID IS NOT NULL AND LEN(Transaction_ID) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Transaction_ID_Valid,
        
        CASE 
            WHEN Line_Number IS NOT NULL AND Line_Number > 0 
            THEN 1 ELSE 0 
        END AS DQ_Line_Number_Valid,
        
        CASE 
            WHEN Product_SKU IS NOT NULL AND LEN(Product_SKU) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Product_SKU_Valid,
        
        CASE 
            WHEN Store_ID IS NOT NULL AND LEN(Store_ID) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Store_ID_Valid,
        
        -- Quantity validation (non-zero, but can be negative)
        CASE 
            WHEN Quantity IS NOT NULL AND Quantity <> 0 
            THEN 1 ELSE 0 
        END AS DQ_Quantity_Valid,
        
        -- Unit Price validation (must be > 0)
        CASE 
            WHEN Unit_Price IS NOT NULL AND Unit_Price > 0 
            THEN 1 ELSE 0 
        END AS DQ_Unit_Price_Valid,
        
        -- Cost Price validation (must be >= 0)
        CASE 
            WHEN Cost_Price IS NOT NULL AND Cost_Price >= 0 
            THEN 1 ELSE 0 
        END AS DQ_Cost_Price_Valid,
        
        -- Discount Amount validation (can be NULL, must be >= 0 if present)
        CASE 
            WHEN Discount_Amount IS NULL THEN 1  -- NULL is valid
            WHEN Discount_Amount >= 0 THEN 1
            ELSE 0 
        END AS DQ_Discount_Amount_Valid,
        
        -- Line Sales Amount validation (must be present and non-zero)
        CASE 
            WHEN Line_Sales_Amount IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Line_Sales_Amount_Valid,
        
        -- Promotion ID format (optional field)
        CASE 
            WHEN Promotion_ID IS NULL THEN 1  -- NULL is valid
            WHEN LEN(Promotion_ID) > 0 THEN 1
            ELSE 0 
        END AS DQ_Promotion_ID_Valid,
        
        -- Business Logic Flags (defer actual fixes to Silver)
        
        -- Flag negative quantities (returns/voids)
        CASE 
            WHEN Quantity < 0 THEN 1 
            ELSE 0 
        END AS Is_Negative_Quantity,
        
        -- Flag outlier quantities (>100 units)
        CASE 
            WHEN Quantity > 100 THEN 1 
            ELSE 0 
        END AS Is_Outlier_Quantity,
        
        -- Flag discounted lines
        CASE 
            WHEN Discount_Amount IS NOT NULL AND Discount_Amount > 0 THEN 1 
            ELSE 0 
        END AS Has_Discount,
        
        -- Flag promoted lines
        CASE 
            WHEN Promotion_ID IS NOT NULL THEN 1 
            ELSE 0 
        END AS Has_Promotion,
        
        -- Flag potential pricing issues (cost > unit price)
        CASE 
            WHEN Cost_Price IS NOT NULL 
                AND Unit_Price IS NOT NULL 
                AND Cost_Price > Unit_Price 
            THEN 1 ELSE 0 
        END AS Has_Price_Issue,
        
        -- Overall Validity (critical fields only)
        CASE 
            WHEN Transaction_Line_ID IS NOT NULL AND LEN(Transaction_Line_ID) > 0
                AND Transaction_ID IS NOT NULL AND LEN(Transaction_ID) > 0
                AND Line_Number IS NOT NULL AND Line_Number > 0
                AND Product_SKU IS NOT NULL AND LEN(Product_SKU) > 0
                AND Store_ID IS NOT NULL AND LEN(Store_ID) > 0
                AND Quantity IS NOT NULL AND Quantity <> 0
                AND Unit_Price IS NOT NULL AND Unit_Price > 0
                AND Cost_Price IS NOT NULL AND Cost_Price >= 0
                AND Line_Sales_Amount IS NOT NULL
            THEN 1 ELSE 0 
        END AS DQ_Is_Valid
        
    FROM cleaned
),

final AS (
    SELECT *
    FROM with_dq_flags
)

SELECT * FROM final