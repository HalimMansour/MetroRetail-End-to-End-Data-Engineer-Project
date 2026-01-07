{{
    config(
        materialized = 'table',
        schema = 'Silver',
        alias = 'crm_customers_clean',
        full_refresh = true,
        transient = false,
    )
}}

/*
    Silver Model: CRM Customers Clean
    Source: Staging.stg_crm_customers
    Target: Silver.crm_customers_clean

    Silver Layer Transformations:
    ✅ Deduplicate by Customer_ID (keep latest)
    ✅ Standardize Gender (M/F/U)
    ✅ Mask PII (Email, Phone) for demo purposes
    ✅ Standardize Preferred_Channel
    ✅ Calculate composite DQ Score (0-100)
    ✅ Only keep valid records
    
  
    Business Rules:
    - Customer_ID is unique (deduplicate if needed)
    - Birthdate must be in past and < 120 years ago
    - Registration_Date must be <= today
    - Email and Phone can be NULL
*/

WITH source AS (
    SELECT *
    FROM {{ ref('stg_crm_customers') }}
    WHERE DQ_Is_Valid = 1  -- Only valid rows from Staging
),

-- Step 1: Deduplicate - Keep latest record per Customer_ID
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Customer_ID 
            ORDER BY Load_Timestamp DESC
        ) AS row_num
    FROM source
),

latest_only AS (
    SELECT *
    FROM deduplicated
    WHERE row_num = 1
),

-- Step 2: Standardize Gender and mask PII
with_standardization AS (
    SELECT
        UPPER(Customer_ID) AS Customer_ID,

        -- Proper case for full name (Ahmed Hassan)
        (
            SELECT STRING_AGG(
                UPPER(LEFT(value, 1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
                ' '
            ) WITHIN GROUP (ORDER BY ordinal)
            FROM STRING_SPLIT(LTRIM(RTRIM(Full_Name)), ' ', 1)
        ) AS Full_Name,

        -- Standardize Gender (M / F / O / N/A) )
        CASE
            WHEN UPPER(LTRIM(RTRIM(Gender))) IN ('F', 'FEMALE') THEN 'F'
            WHEN UPPER(LTRIM(RTRIM(Gender))) IN ('M', 'MALE') THEN 'M'
            WHEN UPPER(LTRIM(RTRIM(Gender))) IN ('O', 'OTHER') THEN 'O'
            ELSE 'N/A'
        END AS Gender,

        Birthdate,
        Registration_Date,

        -- Standardize Email
        COALESCE(NULLIF(UPPER(LTRIM(RTRIM(Email))), ''), 'N/A') AS Email,


        -- Standardize Phone Number
        CASE
            WHEN Phone_Number IS NULL THEN 'N/A'
            WHEN LTRIM(RTRIM(Phone_Number)) = '' THEN 'N/A'
            WHEN LTRIM(RTRIM(Phone_Number)) LIKE '+%' THEN LTRIM(RTRIM(Phone_Number))
            ELSE 'N/A'
        END AS Phone_Number,

        -- Standardize City
        COALESCE(
            NULLIF(
                UPPER(LEFT(LTRIM(RTRIM(City)), 1)) + 
                LOWER(SUBSTRING(LTRIM(RTRIM(City)), 2, LEN(LTRIM(RTRIM(City))))),
                ''
            ),
            'N/A'
        ) AS City,

        -- Standardize Preferred Channel
        CASE
            WHEN UPPER(Preferred_Channel) LIKE '%ONLINE%' THEN 'ONLINE'
            WHEN UPPER(Preferred_Channel) LIKE '%IN%STORE%' THEN 'IN-STORE'
            WHEN UPPER(Preferred_Channel) LIKE '%STORE%' THEN 'IN-STORE'
            WHEN UPPER(Preferred_Channel) LIKE '%MIX%' THEN 'MIXED'
            ELSE 'UNKNOWN'
        END AS Preferred_Channel,

        -- DQ flags
        DQ_Customer_ID_Valid,
        DQ_Full_Name_Valid,
        DQ_Gender_Valid,
        DQ_Birthdate_Valid,
        DQ_Registration_Date_Valid,
        DQ_Email_Valid,
        DQ_Phone_Valid,
        Has_Missing_Email,
        Has_Missing_Phone,

        -- Metadata
        Batch_ID,
        Source_File

    FROM latest_only
),

-- Step 3: Calculate composite DQ Score (0-100)
with_dq_score AS (
    SELECT
        Customer_ID,
        Full_Name,
        Gender,
        Birthdate,
        Registration_Date,
        Email,
        Phone_Number,
        City,
        Preferred_Channel,
        
        -- Business Flags
        CASE WHEN Email <> 'N/A' THEN 1 ELSE 0 END AS Has_Email,
        CASE WHEN Phone_Number <> 'N/A' THEN 1 ELSE 0 END AS Has_Phone,
        
        -- Data Quality Score (0-100)
        -- Weighted based on criticality of each field
        (
            (DQ_Customer_ID_Valid * 20) +        -- 20% - Customer ID is critical
            (DQ_Full_Name_Valid * 15) +          -- 15% - Full Name is important
            (DQ_Gender_Valid * 10) +             -- 10% - Gender is nice to have
            (DQ_Birthdate_Valid * 15) +          -- 15% - Birthdate is important
            (DQ_Registration_Date_Valid * 15) +  -- 15% - Registration Date is critical
            (DQ_Email_Valid * 15) +              -- 15% - Email is important
            (DQ_Phone_Valid * 10)                -- 10% - Phone is nice to have
        ) AS DQ_Score,
        
        -- Overall validity (all critical checks must pass)
        CASE 
            WHEN DQ_Customer_ID_Valid = 1 
            THEN 1 ELSE 0 
        END AS Is_Valid,
        
        -- Metadata
        Batch_ID,
        Source_File
        
    FROM with_standardization
),

-- Step 4: Final select with all required columns
final AS (
    SELECT
        Customer_ID,
        Full_Name,
        Gender,
        Birthdate,
        Registration_Date,
        Email,
        Phone_Number,
        City,
        Preferred_Channel,
        
        -- Business Flags
        Has_Email,
        Has_Phone,
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
    - Customer_ID: VARCHAR(50) - Unique business key
    - Full_Name: VARCHAR(255)
    - Birthdate: DATE

    - City: VARCHAR(100)
    - Preferred_Channel: VARCHAR(20) - Standardized (ONLINE/IN-STORE/MIXED/UNKNOWN)
    - Has_Email: BIT
    - Has_Phone: BIT
    - Is_Valid: BIT
    - DQ_Score: INT (0-100)
    - Batch_ID: VARCHAR(100)
    - Source_File: VARCHAR(255)
    - Created_TS: DATETIME2
    - Updated_TS: DATETIME2
    
    Note: Customer_SK (surrogate key) is auto-generated by SQL Server IDENTITY
    Note: Age and Customer_Tenure_Days are computed columns in SQL Server table
 
*/