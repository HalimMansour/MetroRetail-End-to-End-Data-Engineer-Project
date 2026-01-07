-- =====================================================
-- MetroRetail - Gold Layer DDL (Dimensional Model)
-- Star schema for analytics and Power BI
-- Updated to match dbt models exactly
-- =====================================================

USE MetroRetailDB;
GO

PRINT '========================================';
PRINT 'Creating Gold Layer Tables';
PRINT '========================================';
PRINT '';

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

-- =====================================================
-- 1. Gold.dim_date - Date dimension (calendar table)
-- =====================================================
IF OBJECT_ID('Gold.dim_date', 'U') IS NOT NULL
    DROP TABLE Gold.dim_date;
GO

CREATE TABLE Gold.dim_date (
    Date_SK INT PRIMARY KEY,  -- YYYYMMDD format (e.g., 20240101)
    Date_Value DATE NOT NULL UNIQUE,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Quarter_Name VARCHAR(6) NOT NULL,  -- Q1, Q2, Q3, Q4
    Quarter_Sort INT NOT NULL,
    Month INT NOT NULL,
    Month_Name VARCHAR(20) NOT NULL,
    Month_Short VARCHAR(3) NOT NULL,  -- Jan, Feb, etc.
    Month_Sort INT NOT NULL,
    Week_Of_Year INT NOT NULL,
    Day_Of_Month INT NOT NULL,
    Day_Of_Week INT NOT NULL,
    Day_Name VARCHAR(20) NOT NULL,
    Day_Short VARCHAR(3) NOT NULL,  -- Mon, Tue, etc.
    Is_Weekend BIT NOT NULL,
    Is_Weekday BIT NOT NULL,
    Is_Month_Start BIT NOT NULL,
    Is_Month_End BIT NOT NULL,
    Is_Holiday BIT NOT NULL DEFAULT 0,
    Fiscal_Year INT NOT NULL,
    Fiscal_Quarter INT NOT NULL,
    Year_Month VARCHAR(7) NOT NULL,  -- 2024-01
    Year_Quarter VARCHAR(7) NOT NULL,  -- 2024-Q1
    Created_TS DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_Date_Value 
    ON Gold.dim_date(Date_Value);
CREATE NONCLUSTERED INDEX IX_Year_Month 
    ON Gold.dim_date(Year, Month);
GO

PRINT '✓ Created: Gold.dim_date';
GO

-- =====================================================
-- 2. Gold.dim_product - Product dimension (SCD Type 2)
-- =====================================================
IF OBJECT_ID('Gold.dim_product', 'U') IS NOT NULL
    DROP TABLE Gold.dim_product;
GO

CREATE TABLE Gold.dim_product (
    Product_Key INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate key
    Product_SKU VARCHAR(50) NOT NULL,
    Product_Name VARCHAR(255) NOT NULL,
    Category VARCHAR(100) NOT NULL,
    Sub_Category VARCHAR(100) NOT NULL,
    Price DECIMAL(18,2) NOT NULL,
    Cost_Price DECIMAL(18,2) NULL,
    Supplier_ID VARCHAR(50) NOT NULL,
    
    -- SCD Type 2 fields
    Effective_From DATE NOT NULL,
    Effective_To DATE NULL,
    Is_Current BIT NOT NULL DEFAULT 1,
    Version_Number INT NOT NULL DEFAULT 1,
    
    -- Calculated fields
    Margin_Amount AS (Price - ISNULL(Cost_Price, 0)) PERSISTED,
    Margin_Pct AS (
        CASE 
            WHEN Price > 0 
            THEN ((Price - ISNULL(Cost_Price, 0)) / Price) * 100 
            ELSE 0 
        END
    ) PERSISTED,
    
    -- Metadata
    DQ_Score INT,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE()
);
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_Product_Current 
    ON Gold.dim_product(Product_SKU, Is_Current) 
    WHERE Is_Current = 1;
CREATE NONCLUSTERED INDEX IX_Product_Category 
    ON Gold.dim_product(Category, Sub_Category);
CREATE NONCLUSTERED INDEX IX_Product_SKU
    ON Gold.dim_product(Product_SKU);
GO

PRINT '✓ Created: Gold.dim_product';
GO

-- =====================================================
-- 3. Gold.dim_store - Store dimension
-- =====================================================
IF OBJECT_ID('Gold.dim_store', 'U') IS NOT NULL
    DROP TABLE Gold.dim_store;
GO

CREATE TABLE Gold.dim_store (
    Store_Key INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate key
    Store_ID VARCHAR(50) NOT NULL UNIQUE,
    Store_Name VARCHAR(255) NOT NULL,
    City VARCHAR(100) NOT NULL,
    Region VARCHAR(100) NOT NULL,
    Store_Manager VARCHAR(255) NULL,
    Store_Area_sqm DECIMAL(10,2) NOT NULL,
    Open_Date DATE NOT NULL,
    Store_Age_Years AS (DATEDIFF(YEAR, Open_Date, GETDATE())),
    
    -- Metadata
    DQ_Score INT,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_Store_City 
    ON Gold.dim_store(City);
CREATE NONCLUSTERED INDEX IX_Store_Region 
    ON Gold.dim_store(Region);
GO

PRINT '✓ Created: Gold.dim_store';
GO

-- =====================================================
-- 4. Gold.dim_customer - Customer dimension
-- =====================================================
IF OBJECT_ID('Gold.dim_customer', 'U') IS NOT NULL
    DROP TABLE Gold.dim_customer;
GO

CREATE TABLE Gold.dim_customer (
    Customer_Key INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate key
    Customer_ID VARCHAR(50) NOT NULL UNIQUE,
    Full_Name VARCHAR(255) NOT NULL,
    Gender VARCHAR(10) NOT NULL,  -- Changed from CHAR(1) to match dbt (M/F/O/N/A)
    Birthdate DATE NOT NULL,
    Registration_Date DATE NOT NULL,
    Email VARCHAR(255) NULL,
    Phone_Number VARCHAR(50) NULL,
    City VARCHAR(100) NULL,
    Preferred_Channel VARCHAR(20) NOT NULL,
    
    -- Calculated fields
    Age AS (DATEDIFF(YEAR, Birthdate, GETDATE())),
    Customer_Tenure_Days AS (DATEDIFF(DAY, Registration_Date, GETDATE())),
    
    -- Metadata
    DQ_Score INT,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_Customer_City 
    ON Gold.dim_customer(City);
CREATE NONCLUSTERED INDEX IX_Customer_Registration 
    ON Gold.dim_customer(Registration_Date);
GO

PRINT '✓ Created: Gold.dim_customer';
GO

-- =====================================================
-- 5. Gold.dim_promotion - Promotion dimension
-- =====================================================
IF OBJECT_ID('Gold.dim_promotion', 'U') IS NOT NULL
    DROP TABLE Gold.dim_promotion;
GO

CREATE TABLE Gold.dim_promotion (
    Promotion_Key INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate key
    Promotion_ID VARCHAR(50) NOT NULL UNIQUE,
    Promo_Name VARCHAR(255) NOT NULL,
    Promo_Type VARCHAR(50) NOT NULL,
    Start_Date DATE NOT NULL,
    End_Date DATE NOT NULL,
    Promo_Cost DECIMAL(18,2) NOT NULL,
    Eligible_SKUs NVARCHAR(MAX) NULL,  -- Added to match dbt model
    Promo_Duration_Days AS (DATEDIFF(DAY, Start_Date, End_Date)) PERSISTED,
    Is_Active AS (
        CASE 
            WHEN GETDATE() BETWEEN Start_Date AND End_Date 
            THEN 1 ELSE 0 
        END
    ),
    
    -- Metadata
    DQ_Score INT,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_Promotion_Dates 
    ON Gold.dim_promotion(Start_Date, End_Date);
CREATE NONCLUSTERED INDEX IX_Promotion_Type 
    ON Gold.dim_promotion(Promo_Type);
GO

PRINT '✓ Created: Gold.dim_promotion';
GO

-- =====================================================
-- 6. Gold.dim_weather - Weather dimension (NEW)
-- =====================================================
IF OBJECT_ID('Gold.dim_weather', 'U') IS NOT NULL
    DROP TABLE Gold.dim_weather;
GO

CREATE TABLE Gold.dim_weather (
    Weather_Key INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate key
    Weather_Date DATE NOT NULL,
    Store_ID VARCHAR(50) NOT NULL,
    Temperature_C DECIMAL(5,2) NULL,  -- Can be NULL (~2%)
    Precipitation_mm DECIMAL(5,2) NOT NULL,
    Weather_Condition VARCHAR(100) NOT NULL,
    
    -- Business flags
    Has_Missing_Temperature BIT NOT NULL,
    Is_Extreme_Temperature BIT NOT NULL,
    Is_Rainy AS (CASE WHEN Precipitation_mm > 0 THEN 1 ELSE 0 END) PERSISTED,
    
    -- Metadata
    DQ_Score INT,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UX_Weather_Date_Store UNIQUE (Weather_Date, Store_ID)
);
GO

CREATE NONCLUSTERED INDEX IX_Weather_Date 
    ON Gold.dim_weather(Weather_Date);
CREATE NONCLUSTERED INDEX IX_Weather_Store 
    ON Gold.dim_weather(Store_ID);
CREATE NONCLUSTERED INDEX IX_Weather_Condition 
    ON Gold.dim_weather(Weather_Condition);
GO

PRINT '✓ Created: Gold.dim_weather';
GO

-- =====================================================
-- BRIDGE TABLES
-- =====================================================

-- =====================================================
-- 7. Gold.bridge_promotion_product - Promotion-Product bridge
-- =====================================================
IF OBJECT_ID('Gold.bridge_promotion_product', 'U') IS NOT NULL
    DROP TABLE Gold.bridge_promotion_product;
GO

CREATE TABLE Gold.bridge_promotion_product (
    Promotion_ID VARCHAR(50) NOT NULL,
    Product_SKU VARCHAR(50) NOT NULL,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    
    -- Composite Primary Key (bridge tables should NOT have surrogate keys)
    CONSTRAINT PK_bridge_promotion_product 
        PRIMARY KEY CLUSTERED (Promotion_ID, Product_SKU)
);
GO

CREATE NONCLUSTERED INDEX IX_Bridge_Product 
    ON Gold.bridge_promotion_product(Product_SKU);
GO

PRINT '✓ Created: Gold.bridge_promotion_product';
GO

-- =====================================================
-- FACT TABLES
-- =====================================================

-- =====================================================
-- 8. Gold.fact_sales - Sales fact table (grain: transaction line)
-- =====================================================
IF OBJECT_ID('Gold.fact_sales', 'U') IS NOT NULL
    DROP TABLE Gold.fact_sales;
GO

CREATE TABLE Gold.fact_sales (
    Sales_Key BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Degenerate Dimensions (transaction identifiers)
    Transaction_Line_ID VARCHAR(50) NOT NULL,
    Transaction_ID VARCHAR(50) NOT NULL,
    Line_Number INT NOT NULL,
    
    -- Foreign Keys (Natural Keys - matching dbt model)
    Transaction_Date DATE NOT NULL,  -- FK to dim_date.Date_Value
    Product_SKU VARCHAR(50) NOT NULL,  -- FK to dim_product.Product_SKU
    Store_ID VARCHAR(50) NOT NULL,  -- FK to dim_store.Store_ID
    Customer_ID VARCHAR(50) NULL,  -- FK to dim_customer.Customer_ID (NULL for walk-ins)
    Promotion_ID VARCHAR(50) NULL,  -- FK to dim_promotion.Promotion_ID (NULL if no promotion)
    
    -- Measures (additive facts)
    Quantity INT NOT NULL,
    Unit_Price DECIMAL(18,2) NOT NULL,
    Cost_Price DECIMAL(18,2) NOT NULL,
    Discount_Amount DECIMAL(18,2) NOT NULL DEFAULT 0,
    Line_Sales_Amount DECIMAL(18,2) NOT NULL,
    
    -- Calculated Measures (from dbt model)
    Line_Cost_Amount AS (CAST(Quantity AS DECIMAL(18,2)) * Cost_Price) PERSISTED,
    Line_Margin_Amount AS (Line_Sales_Amount - (CAST(Quantity AS DECIMAL(18,2)) * Cost_Price)) PERSISTED,
    Line_Margin_Pct AS (
        CASE 
            WHEN CAST(Quantity AS DECIMAL(18,2)) * Cost_Price > 0 
            THEN ((Line_Sales_Amount - (CAST(Quantity AS DECIMAL(18,2)) * Cost_Price)) 
                 / (CAST(Quantity AS DECIMAL(18,2)) * Cost_Price)) * 100
            ELSE 0 
        END
    ) PERSISTED,
    
    -- Context Attributes
    Payment_Method VARCHAR(20) NULL,
    Transaction_TS DATETIME2 NULL,
    
    -- Flags
    Is_Return BIT NOT NULL DEFAULT 0,
    Is_Outlier_Quantity BIT NOT NULL DEFAULT 0,
    Has_Discount BIT NOT NULL DEFAULT 0,
    Has_Promotion BIT NOT NULL DEFAULT 0,
    Is_Walk_In BIT NOT NULL DEFAULT 0,
    
    -- Metadata
    DQ_Score INT,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE()
);
GO

-- Clustered index on Date for time-series queries
CREATE CLUSTERED INDEX CIX_Sales_Date 
    ON Gold.fact_sales(Transaction_Date);

CREATE NONCLUSTERED INDEX IX_Sales_Product 
    ON Gold.fact_sales(Product_SKU);
CREATE NONCLUSTERED INDEX IX_Sales_Store 
    ON Gold.fact_sales(Store_ID);
CREATE NONCLUSTERED INDEX IX_Sales_Customer 
    ON Gold.fact_sales(Customer_ID) WHERE Customer_ID IS NOT NULL;
CREATE NONCLUSTERED INDEX IX_Sales_Transaction 
    ON Gold.fact_sales(Transaction_ID);
CREATE NONCLUSTERED INDEX IX_Sales_Promotion 
    ON Gold.fact_sales(Promotion_ID) WHERE Promotion_ID IS NOT NULL;
GO

PRINT '✓ Created: Gold.fact_sales';
GO

-- =====================================================
-- 9. Gold.fact_inventory_snapshot - Inventory snapshot fact
-- =====================================================
IF OBJECT_ID('Gold.fact_inventory_snapshot', 'U') IS NOT NULL
    DROP TABLE Gold.fact_inventory_snapshot;
GO

CREATE TABLE Gold.fact_inventory_snapshot (
    Inventory_Snapshot_Key BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Natural Keys (for dimension joins)
    Inventory_ID VARCHAR(50) NOT NULL,
    Product_SKU VARCHAR(50) NOT NULL,  -- FK to dim_product.Product_SKU
    Store_ID VARCHAR(50) NOT NULL,  -- FK to dim_store.Store_ID
    Snapshot_Date DATE NOT NULL,  -- FK to dim_date.Date_Value
    
    -- Measures (ADDITIVE - can sum across dimensions)
    Quantity_On_Hand INT NOT NULL,
    Reorder_Level INT NOT NULL,
    
    -- Derived Measures
    Reorder_Quantity_Needed AS (
        CASE 
            WHEN Quantity_On_Hand < Reorder_Level 
            THEN Reorder_Level - Quantity_On_Hand 
            ELSE 0 
        END
    ) PERSISTED,
    
    -- Business Flags
    Is_Below_Reorder_Level BIT NOT NULL DEFAULT 0,
    Is_Outlier_Quantity BIT NOT NULL DEFAULT 0,
    
    -- Metadata
    DQ_Score INT,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UX_Inventory_Snapshot UNIQUE (Product_SKU, Store_ID, Snapshot_Date)
);
GO

CREATE CLUSTERED INDEX CIX_Inventory_Date 
    ON Gold.fact_inventory_snapshot(Snapshot_Date);

CREATE NONCLUSTERED INDEX IX_Inventory_Product 
    ON Gold.fact_inventory_snapshot(Product_SKU);
CREATE NONCLUSTERED INDEX IX_Inventory_Store 
    ON Gold.fact_inventory_snapshot(Store_ID);
CREATE NONCLUSTERED INDEX IX_Inventory_Reorder 
    ON Gold.fact_inventory_snapshot(Is_Below_Reorder_Level) 
    WHERE Is_Below_Reorder_Level = 1;
GO

PRINT '✓ Created: Gold.fact_inventory_snapshot';
GO

-- =====================================================
-- AGGREGATE TABLES (Pre-aggregated for Power BI)
-- =====================================================

-- =====================================================
-- 10. Gold.agg_sales_daily - Daily sales aggregates
-- =====================================================
IF OBJECT_ID('Gold.agg_sales_daily', 'U') IS NOT NULL
    DROP TABLE Gold.agg_sales_daily;
GO

CREATE TABLE Gold.agg_sales_daily (
    Daily_Sales_Key INT IDENTITY(1,1) PRIMARY KEY,
    Transaction_Date DATE NOT NULL,  -- Changed to match fact_sales
    Store_ID VARCHAR(50) NOT NULL,  -- Changed to match fact_sales
    
    -- Aggregated Measures
    Total_Sales_Amount DECIMAL(18,2) NOT NULL,
    Total_Cost DECIMAL(18,2) NOT NULL,
    Total_Margin DECIMAL(18,2) NOT NULL,
    Total_Discount DECIMAL(18,2) NOT NULL,
    Transaction_Count INT NOT NULL,
    Line_Count INT NOT NULL,
    Units_Sold INT NOT NULL,
    Unique_Customers INT NOT NULL,
    
    -- Calculated Metrics
    Avg_Transaction_Value AS (
        CASE 
            WHEN Transaction_Count > 0 
            THEN Total_Sales_Amount / Transaction_Count 
            ELSE 0 
        END
    ) PERSISTED,
    Avg_Margin_Pct AS (
        CASE 
            WHEN Total_Sales_Amount > 0 
            THEN (Total_Margin / Total_Sales_Amount) * 100 
            ELSE 0 
        END
    ) PERSISTED,
    
    Created_TS DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UX_Daily_Sales UNIQUE (Transaction_Date, Store_ID)
);
GO

CREATE NONCLUSTERED INDEX IX_Daily_Sales_Date 
    ON Gold.agg_sales_daily(Transaction_Date);
CREATE NONCLUSTERED INDEX IX_Daily_Sales_Store 
    ON Gold.agg_sales_daily(Store_ID);
GO

PRINT '✓ Created: Gold.agg_sales_daily';
GO

-- =====================================================
-- 11. Gold.agg_sales_monthly - Monthly sales aggregates
-- =====================================================
IF OBJECT_ID('Gold.agg_sales_monthly', 'U') IS NOT NULL
    DROP TABLE Gold.agg_sales_monthly;
GO

CREATE TABLE Gold.agg_sales_monthly (
    Monthly_Sales_Key INT IDENTITY(1,1) PRIMARY KEY,
    Year INT NOT NULL,
    Month INT NOT NULL,
    Store_ID VARCHAR(50) NULL,  -- NULL for total across all stores
    Category VARCHAR(100) NULL,  -- NULL for total across all categories
    
    -- Aggregated Measures
    Total_Sales_Amount DECIMAL(18,2) NOT NULL,
    Total_Cost DECIMAL(18,2) NOT NULL,
    Total_Margin DECIMAL(18,2) NOT NULL,
    Total_Discount DECIMAL(18,2) NOT NULL,
    Transaction_Count INT NOT NULL,
    Units_Sold INT NOT NULL,
    Unique_Customers INT NOT NULL,
    
    -- Calculated Metrics
    Avg_Transaction_Value AS (
        CASE 
            WHEN Transaction_Count > 0 
            THEN Total_Sales_Amount / Transaction_Count 
            ELSE 0 
        END
    ) PERSISTED,
    Margin_Pct AS (
        CASE 
            WHEN Total_Sales_Amount > 0 
            THEN (Total_Margin / Total_Sales_Amount) * 100 
            ELSE 0 
        END
    ) PERSISTED,
    
    Created_TS DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_Monthly_Sales_Period 
    ON Gold.agg_sales_monthly(Year, Month);
CREATE NONCLUSTERED INDEX IX_Monthly_Sales_Store 
    ON Gold.agg_sales_monthly(Store_ID) WHERE Store_ID IS NOT NULL;
GO

PRINT '✓ Created: Gold.agg_sales_monthly';
GO

-- =====================================================
-- 12. Gold.agg_product_performance - Product performance metrics
-- =====================================================
IF OBJECT_ID('Gold.agg_product_performance', 'U') IS NOT NULL
    DROP TABLE Gold.agg_product_performance;
GO

CREATE TABLE Gold.agg_product_performance (
    Product_Performance_Key INT IDENTITY(1,1) PRIMARY KEY,
    Product_SKU VARCHAR(50) NOT NULL,  -- Changed to match fact_sales
    Store_ID VARCHAR(50) NULL,  -- NULL for total across all stores
    Year INT NOT NULL,
    Month INT NOT NULL,
    
    -- Sales Metrics
    Total_Sales_Amount DECIMAL(18,2) NOT NULL,
    Total_Cost DECIMAL(18,2) NOT NULL,
    Total_Margin DECIMAL(18,2) NOT NULL,
    Units_Sold INT NOT NULL,
    Transaction_Count INT NOT NULL,
    
    -- Performance Metrics
    Avg_Unit_Price DECIMAL(18,2) NOT NULL,
    Margin_Pct AS (
        CASE 
            WHEN Total_Sales_Amount > 0 
            THEN (Total_Margin / Total_Sales_Amount) * 100 
            ELSE 0 
        END
    ) PERSISTED,
    
    Created_TS DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_Product_Performance 
    ON Gold.agg_product_performance(Product_SKU, Year, Month);
CREATE NONCLUSTERED INDEX IX_Product_Performance_Store 
    ON Gold.agg_product_performance(Store_ID) WHERE Store_ID IS NOT NULL;
GO

PRINT '✓ Created: Gold.agg_product_performance';
GO

-- =====================================================
-- Summary
-- =====================================================
PRINT '';
PRINT '========================================';
PRINT 'Gold Layer DDL Execution Complete!';
PRINT '========================================';
PRINT '';
PRINT 'Created Dimension Tables:';
PRINT '  1. Gold.dim_date (Date dimension)';
PRINT '  2. Gold.dim_product (Product SCD Type 2)';
PRINT '  3. Gold.dim_store (Store dimension)';
PRINT '  4. Gold.dim_customer (Customer dimension)';
PRINT '  5. Gold.dim_promotion (Promotion dimension)';
PRINT '  6. Gold.dim_weather (Weather dimension) [NEW]';
PRINT '';
PRINT 'Created Bridge Tables:';
PRINT '  7. Gold.bridge_promotion_product (Promotion-Product many-to-many)';
PRINT '';
PRINT 'Created Fact Tables:';
PRINT '  8. Gold.fact_sales (Transaction line grain)';
PRINT '  9. Gold.fact_inventory_snapshot (Inventory snapshot) [NEW]';
PRINT '';
PRINT 'Created Aggregate Tables:';
PRINT '  10. Gold.agg_sales_daily (Daily aggregates)';
PRINT '  11. Gold.agg_sales_monthly (Monthly aggregates)';
PRINT '  12. Gold.agg_product_performance (Product metrics)';
PRINT '';
PRINT 'Key Changes from Original:';
PRINT '  ✓ Added dim_weather (weather dimension)';
PRINT '  ✓ Added fact_inventory_snapshot (inventory fact)';
PRINT '  ✓ Changed fact_sales to use natural keys (matches dbt)';
PRINT '  ✓ Added Eligible_SKUs to dim_promotion';
PRINT '  ✓ Updated Customer.Gender from CHAR(1) to VARCHAR(10)';
PRINT '  ✓ Added Payment_Method and Transaction_TS to fact_sales';
PRINT '  ✓ Updated aggregate tables to use natural keys';
PRINT '';
PRINT 'Star Schema Structure:';
PRINT '  ✓ 6 Dimension tables';
PRINT '  ✓ 1 Bridge table';
PRINT '  ✓ 2 Fact tables';
PRINT '  ✓ 3 Aggregate tables';
PRINT '  ✓ Proper indexing for analytics';
PRINT '  ✓ Computed columns for metrics';
PRINT '';
PRINT 'Next: Run dbt Gold models to populate these tables';
PRINT '========================================';
GO