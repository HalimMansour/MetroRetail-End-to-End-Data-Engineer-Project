{{
    config(
        materialized = 'table',
        schema = 'Staging',
        full_refresh = true,
        transient = false,
        
    )
}}

/*
    Staging Model: POS Transactions Header
    Source: raw.pos_transactions_header
    Target: Staging.stg_pos_transactions_header

    Staging Layer Transformations (per architecture):
    - Column renaming: None needed
    - Type casting: Dates, Timestamp, Numeric
    - Date parsing: Handle M/D/YYYY and YYYY-MM-DD formats
    - Basic row validation: DQ flags
    
    NOT in Staging (deferred to Silver):
    - Deduplication of duplicate transactions
    - Referential integrity checks (Store_ID, Customer_ID existence)
    - Total_Amount validation against line items sum
    - Business logic (discount % calculation, etc.)
    
    Data Issues:
    - Mixed date formats (M/D/YYYY vs YYYY-MM-DD)
    - Timestamp parsing (M/D/YYYY HH:MM format)
    - Missing Customer_ID (~12% nulls, walk-ins)
    - "NA" strings for Customer_ID
    - Currency symbols in amounts
    - Mixed case in Payment_Method
*/

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'pos_transactions_header') }}
),

cleaned AS (
    SELECT
        Raw_ID,
        
        -- Business Key
        UPPER(LTRIM(RTRIM(Transaction_ID))) AS Transaction_ID,
        
        -- Date Fields - Handle multiple formats (M/D/YYYY or YYYY-MM-DD)
        COALESCE(
            TRY_CONVERT(DATE, Transaction_Date, 101),  -- MM/DD/YYYY
            TRY_CONVERT(DATE, Transaction_Date, 103),  -- DD/MM/YYYY
            TRY_CAST(Transaction_Date AS DATE)         -- YYYY-MM-DD
        ) AS Transaction_Date,
        
        -- Timestamp Field - Parse M/D/YYYY HH:MM format
        COALESCE(
            TRY_CONVERT(DATETIME2, Transaction_TS, 101),  -- MM/DD/YYYY HH:MM
            TRY_CONVERT(DATETIME2, Transaction_TS, 103),  -- DD/MM/YYYY HH:MM
            TRY_CAST(Transaction_TS AS DATETIME2)         -- YYYY-MM-DD HH:MM:SS
        ) AS Transaction_TS,
        
        -- Store ID
        UPPER(LTRIM(RTRIM(Store_ID))) AS Store_ID,
        
        -- Customer ID - Handle NULL, empty, and "NA" as NULL
        CASE 
            WHEN Customer_ID IS NULL THEN NULL
            WHEN UPPER(LTRIM(RTRIM(Customer_ID))) IN ('', 'NA', 'NULL', 'N/A') THEN NULL
            ELSE UPPER(LTRIM(RTRIM(Customer_ID)))
        END AS Customer_ID,
        
        -- Payment Method - Normalize to uppercase
        UPPER(LTRIM(RTRIM(Payment_Method))) AS Payment_Method,
        
        -- Numeric Fields - Remove currency symbols and commas
        TRY_CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(Total_Amount, '$', ''), ',', ''), ' ', ''), '€', '')
            AS DECIMAL(18,2)
        ) AS Total_Amount,
        
        -- Total Discount - Handle NULL, "NA", and 0
        CASE 
            WHEN Total_Discount IS NULL THEN NULL
            WHEN UPPER(LTRIM(RTRIM(Total_Discount))) IN ('NA', 'NULL', 'N/A', '') THEN NULL
            ELSE TRY_CAST(
                REPLACE(REPLACE(REPLACE(REPLACE(Total_Discount, '$', ''), ',', ''), ' ', ''), '€', '')
                AS DECIMAL(18,2)
            )
        END AS Total_Discount,
        
        -- Line Count
        TRY_CAST(Line_Count AS INT) AS Line_Count,
        
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
            WHEN Transaction_ID IS NOT NULL AND LEN(Transaction_ID) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Transaction_ID_Valid,
        
        CASE 
            WHEN Transaction_Date IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Transaction_Date_Valid,
        
        CASE 
            WHEN Transaction_TS IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Transaction_TS_Valid,
        
        CASE 
            WHEN Store_ID IS NOT NULL AND LEN(Store_ID) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Store_ID_Valid,
        
        -- Customer_ID can be NULL (walk-ins), so this just checks format if present
        CASE 
            WHEN Customer_ID IS NULL THEN 1  -- NULL is valid for walk-ins
            WHEN LEN(Customer_ID) > 0 THEN 1
            ELSE 0 
        END AS DQ_Customer_ID_Valid,
        
        -- Payment Method validation (must be in enum)
        CASE 
            WHEN Payment_Method IN ('CASH', 'CARD', 'ONLINE', 'CREDIT', 'DEBIT') 
            THEN 1 ELSE 0 
        END AS DQ_Payment_Method_Valid,
        
        -- Total Amount validation
        CASE 
            WHEN Total_Amount IS NOT NULL AND Total_Amount > 0 
            THEN 1 ELSE 0 
        END AS DQ_Total_Amount_Valid,
        
        -- Total Discount validation (can be NULL or 0)
        CASE 
            WHEN Total_Discount IS NULL THEN 1  -- NULL is valid
            WHEN Total_Discount >= 0 AND Total_Discount <= Total_Amount THEN 1
            ELSE 0 
        END AS DQ_Total_Discount_Valid,
        
        -- Line Count validation (must be >= 1)
        CASE 
            WHEN Line_Count IS NOT NULL AND Line_Count >= 1 
            THEN 1 ELSE 0 
        END AS DQ_Line_Count_Valid,
        
        -- Timestamp consistency (TS should match Date)
        CASE 
            WHEN Transaction_Date IS NOT NULL 
                AND Transaction_TS IS NOT NULL
                AND CAST(Transaction_TS AS DATE) = Transaction_Date
            THEN 1 ELSE 0 
        END AS DQ_Timestamp_Consistent,
        
        -- Flag walk-in customers (no Customer_ID)
        CASE 
            WHEN Customer_ID IS NULL THEN 1 
            ELSE 0 
        END AS Is_Walk_In,
        
        -- Flag discounted transactions
        CASE 
            WHEN Total_Discount IS NOT NULL AND Total_Discount > 0 THEN 1 
            ELSE 0 
        END AS Has_Discount,
        
        -- Overall Validity
        CASE 
            WHEN Transaction_ID IS NOT NULL AND LEN(Transaction_ID) > 0
                AND Transaction_Date IS NOT NULL
                AND Transaction_TS IS NOT NULL
                AND Store_ID IS NOT NULL AND LEN(Store_ID) > 0
                AND Payment_Method IN ('CASH', 'CARD', 'ONLINE', 'CREDIT', 'DEBIT')
                AND Total_Amount IS NOT NULL AND Total_Amount > 0
                AND Line_Count IS NOT NULL AND Line_Count >= 1
                AND CAST(Transaction_TS AS DATE) = Transaction_Date
            THEN 1 ELSE 0 
        END AS DQ_Is_Valid
        
    FROM cleaned
),

final AS (
    SELECT *
    FROM with_dq_flags
)

SELECT * FROM final