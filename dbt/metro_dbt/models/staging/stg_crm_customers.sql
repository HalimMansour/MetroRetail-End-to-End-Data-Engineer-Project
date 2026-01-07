{{
    config(
        materialized = 'table',
        schema = 'Staging',
        full_refresh = true,
        transient = false,
    )
}}

/*
    Staging Model: CRM Customers
    Source: raw.crm_customers
    Target: Staging.stg_crm_customers

    Staging Layer Transformations:
    - Column renaming: None needed
    - Type casting: INT (Customer_ID), DATE (Birthdate, Registration_Date)
    - Text normalization: UPPER (Gender, Preferred_Channel), LOWER (Email)
    - Email/Phone cleaning
    
    NOT in Staging (deferred to Silver):
    - Email format validation (regex)
    - Phone number format standardization
    - Gender standardization ("M"/"F" vs "Male"/"Female")
    - Age calculation from Birthdate
    - Duplicate Customer_ID handling
    
    Data Issues:
    - Email nulls (~15%)
    - Phone_Number nulls (~20%)
    - Mixed case in Full_Name
    - Gender values: "M", "F", "Male", "Female", "m", "f"
    - City nulls
*/

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'crm_customers') }}
),

cleaned AS (
    SELECT
        Raw_ID,
        
        -- Business Key
        TRY_CAST(Customer_ID  AS VARCHAR(50)) AS Customer_ID,
        
        -- Full Name
        NULLIF(LTRIM(RTRIM(Full_Name)), '') AS Full_Name,
        
        -- Gender - Normalize to uppercase
        UPPER(NULLIF(LTRIM(RTRIM(Gender)), '')) AS Gender,
        
        -- Birthdate
        TRY_CAST(Birthdate AS DATE) AS Birthdate,
        
        -- Registration Date
        TRY_CAST(Registration_Date AS DATE) AS Registration_Date,
        
        -- Email - Lowercase and trim
        LOWER(NULLIF(LTRIM(RTRIM(Email)), '')) AS Email,
        
        -- Phone Number - Clean
        NULLIF(LTRIM(RTRIM(Phone_Number)), '') AS Phone_Number,
        
        -- City
        NULLIF(LTRIM(RTRIM(City)), '') AS City,
        
        -- Preferred Channel - Uppercase
        UPPER(NULLIF(LTRIM(RTRIM(Preferred_Channel)), '')) AS Preferred_Channel,
        
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
            WHEN Customer_ID IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Customer_ID_Valid,
        
        CASE 
            WHEN Full_Name IS NOT NULL AND LEN(Full_Name) > 0 
            THEN 1 ELSE 0 
        END AS DQ_Full_Name_Valid,
        
        CASE 
            WHEN Gender IN ('M', 'F', 'MALE', 'FEMALE') 
            THEN 1 ELSE 0 
        END AS DQ_Gender_Valid,
        
        CASE 
            WHEN Birthdate IS NOT NULL 
                AND Birthdate < GETDATE() 
                AND Birthdate > DATEADD(YEAR, -120, GETDATE())
            THEN 1 ELSE 0 
        END AS DQ_Birthdate_Valid,
        
        CASE 
            WHEN Registration_Date IS NOT NULL 
            THEN 1 ELSE 0 
        END AS DQ_Registration_Date_Valid,
        
        CASE 
            WHEN Email IS NOT NULL AND Email LIKE '%@%.%' 
            THEN 1 ELSE 0 
        END AS DQ_Email_Valid,
        
        CASE 
            WHEN Phone_Number IS NOT NULL AND LEN(Phone_Number) >= 10 
            THEN 1 ELSE 0 
        END AS DQ_Phone_Valid,
        
        -- Business Flags
        CASE 
            WHEN Email IS NULL 
            THEN 1 ELSE 0 
        END AS Has_Missing_Email,
        
        CASE 
            WHEN Phone_Number IS NULL 
            THEN 1 ELSE 0 
        END AS Has_Missing_Phone,
        
        -- Overall Validity
        CASE 
            WHEN Customer_ID IS NOT NULL
                AND Full_Name IS NOT NULL AND LEN(Full_Name) > 0
                AND Registration_Date IS NOT NULL
            THEN 1 ELSE 0 
        END AS DQ_Is_Valid
        
    FROM cleaned
),

final AS (
    SELECT *
    FROM with_dq_flags
)

SELECT * FROM final

