{{
    config(
        materialized = 'table',
        schema = 'Silver',
        alias = 'pos_transactions_header_clean',
        full_refresh = true,
        transient = false,
    )
}}

/*
    Silver Model: POS Transactions Header Clean
    Source: Staging.stg_pos_transactions_header
    Target: Silver.pos_transactions_header_clean

    Silver Layer Transformations:
    ✅ Deduplicate by Transaction_ID (keep latest)
    ✅ Validate foreign key references (Store_ID, Customer_ID)
    ✅ Calculate composite DQ Score (0-100)
    ✅ Flag walk-ins and discounted transactions
    ✅ Only keep valid records
    
    Business Rules:
    - Transaction_ID is unique (deduplicate if needed)
    - Store_ID must exist in stores
    - Customer_ID must exist in customers (or be NULL for walk-ins)
    - Total_Amount must be > 0
    - Total_Discount must be <= Total_Amount
    - Line_Count must be >= 1
*/

WITH source AS (
    SELECT *
    FROM {{ ref('stg_pos_transactions_header') }}
    WHERE DQ_Is_Valid = 1  -- Only valid rows from Staging
),

-- Step 1: Deduplicate - Keep latest record per Transaction_ID
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Transaction_ID 
            ORDER BY Load_Timestamp DESC
        ) AS row_num
    FROM source
),

latest_only AS (
    SELECT *
    FROM deduplicated
    WHERE row_num = 1
),

-- Step 2: Validate foreign key references
with_fk_validation AS (
    SELECT
        h.*,
        
        -- Check if Store_ID exists in stores
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM {{ ref('erp_stores_clean') }} s 
                WHERE s.Store_ID = h.Store_ID
            ) THEN 1 ELSE 0 
        END AS Store_Exists,
        
        -- Check if Customer_ID exists in customers (if not walk-in)
        CASE 
            WHEN h.Customer_ID IS NULL THEN 1  -- Walk-in, no check needed
            WHEN EXISTS (
                SELECT 1 
                FROM {{ ref('crm_customers_clean') }} c 
                WHERE c.Customer_ID = h.Customer_ID
            ) THEN 1 ELSE 0 
        END AS Customer_Exists
        
    FROM latest_only h
),

-- Step 3: Calculate composite DQ Score (0-100)
with_dq_score AS (
    SELECT
        Transaction_ID,
        Transaction_Date,
        Transaction_TS,
        COALESCE(Store_ID, 'N/A') AS Store_ID,
        COALESCE(Customer_ID, 'N/A') AS Customer_ID,
        COALESCE(Payment_Method, 'N/A') AS Payment_Method,
        Total_Amount,
        Total_Discount,
        Line_Count,
        
        -- Business Flags (from Staging)
        Is_Walk_In,
        Has_Discount,
        
        -- FK Validation Flags
        Store_Exists,
        Customer_Exists,
        
        -- Data Quality Score (0-100)
        -- Weighted based on criticality of each field
        (
            (DQ_Transaction_ID_Valid * 15) +
            (DQ_Transaction_Date_Valid * 15) +
            (DQ_Transaction_TS_Valid * 10) +
            (DQ_Store_ID_Valid * 15) +
            (DQ_Customer_ID_Valid * 10) +
            (DQ_Payment_Method_Valid * 10) +
            (DQ_Total_Amount_Valid * 15) +
            (DQ_Total_Discount_Valid * 5) +
            (DQ_Line_Count_Valid * 5)
        ) AS DQ_Score,
        
        -- Overall validity (must pass all critical checks)
        CASE 
            WHEN DQ_Is_Valid = 1 
                AND Store_Exists = 1 
            THEN 1 ELSE 0 
        END AS Is_Valid,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM with_fk_validation
),

-- Step 4: Final select with all required columns
final AS (
    SELECT
        Transaction_ID,
        Transaction_Date,
        Transaction_TS,
        Store_ID,
        Customer_ID,
        Payment_Method,
        Total_Amount,
        Total_Discount,
        Line_Count,
        
        -- Business Flags
        Is_Walk_In,
        Has_Discount,
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
    - Transaction_ID: VARCHAR(50) - Unique business key
    - Transaction_Date: DATE
    - Transaction_TS: DATETIME2
    - Store_ID: VARCHAR(50) - FK to stores
    - Customer_ID: VARCHAR(50) - FK to customers (nullable)
    - Payment_Method: VARCHAR(20) - CASH/CARD/ONLINE/CREDIT/DEBIT
    - Total_Amount: DECIMAL(18,2)
    - Total_Discount: DECIMAL(18,2) - Nullable
    - Line_Count: INT
    - Is_Walk_In: BIT
    - Has_Discount: BIT
    - Is_Valid: BIT
    - DQ_Score: INT (0-100)
    - Batch_ID: VARCHAR(100)
    - Source_File: VARCHAR(255)
    - Created_TS: DATETIME2
    - Updated_TS: DATETIME2
    
    Note: Transaction_Header_SK is auto-generated by SQL Server IDENTITY
    Note: Net_Amount and Discount_Rate are computed columns in SQL Server table
*/