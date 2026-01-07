{{
    config(
        materialized = 'table',
        schema = 'Gold',
        alias = 'dim_date',
        full_refresh = true,
        transient = false,
    )
}}

/*
    Gold Model: Date Dimension
    Dynamic range:
    - Start: 2010-01-01
    - End:   Current year + 10 years
*/

WITH params AS (
    SELECT
        CAST('2010-01-01' AS DATE) AS Start_Date,
        DATEFROMPARTS(YEAR(GETDATE()) + 10, 12, 31) AS End_Date
),

numbers AS (
    -- Generate enough numbers for ~200 years
    SELECT TOP (80000)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM sys.objects a
    CROSS JOIN sys.objects b
),

date_base AS (
    SELECT
        DATEADD(DAY, n, p.Start_Date) AS Date_Value
    FROM numbers n
    CROSS JOIN params p
    WHERE DATEADD(DAY, n, p.Start_Date) <= p.End_Date
),

final AS (
    SELECT
        -- Surrogate Key
        CAST(FORMAT(Date_Value, 'yyyyMMdd') AS INT) AS Date_SK,

        Date_Value,

        -- Year
        YEAR(Date_Value) AS Year,

        -- Quarter
        DATEPART(QUARTER, Date_Value) AS Quarter,
        'Q' + CAST(DATEPART(QUARTER, Date_Value) AS VARCHAR(1)) AS Quarter_Name,
        DATEPART(QUARTER, Date_Value) AS Quarter_Sort,

        -- Month
        MONTH(Date_Value) AS Month,
        DATENAME(MONTH, Date_Value) AS Month_Name,
        LEFT(DATENAME(MONTH, Date_Value), 3) AS Month_Short,
        MONTH(Date_Value) AS Month_Sort,

        -- ISO Week
        DATEPART(ISO_WEEK, Date_Value) AS Week_Of_Year,

        -- Day
        DAY(Date_Value) AS Day_Of_Month,
        DATEPART(WEEKDAY, Date_Value) AS Day_Of_Week,
        DATENAME(WEEKDAY, Date_Value) AS Day_Name,
        LEFT(DATENAME(WEEKDAY, Date_Value), 3) AS Day_Short,

        -- Flags (DATEFIRST-safe)
        CASE WHEN DATENAME(WEEKDAY, Date_Value) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS Is_Weekend,
        CASE WHEN DATENAME(WEEKDAY, Date_Value) NOT IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS Is_Weekday,
        CASE WHEN DAY(Date_Value) = 1 THEN 1 ELSE 0 END AS Is_Month_Start,
        CASE WHEN Date_Value = EOMONTH(Date_Value) THEN 1 ELSE 0 END AS Is_Month_End,

        -- Holidays (extend via dim_holiday later)
        CASE
            WHEN MONTH(Date_Value) = 1 AND DAY(Date_Value) = 1 THEN 1
            WHEN MONTH(Date_Value) = 12 AND DAY(Date_Value) = 25 THEN 1
            ELSE 0
        END AS Is_Holiday,

        -- Fiscal (calendar-aligned)
        YEAR(Date_Value) AS Fiscal_Year,
        DATEPART(QUARTER, Date_Value) AS Fiscal_Quarter,

        -- Grouping
        FORMAT(Date_Value, 'yyyy-MM') AS Year_Month,
        CAST(YEAR(Date_Value) AS VARCHAR(4)) + '-Q'
            + CAST(DATEPART(QUARTER, Date_Value) AS VARCHAR(1)) AS Year_Quarter,

        GETDATE() AS Created_TS
    FROM date_base
)

SELECT *
FROM final;
