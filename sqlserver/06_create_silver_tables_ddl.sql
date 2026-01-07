-- =====================================================
-- MetroRetail - Silver Layer DDL (WITH SOURCE PREFIXES)
-- Table naming: {source_prefix}_{entity}_clean
-- =====================================================

USE MetroRetailDB;
GO

PRINT '========================================';
PRINT 'Creating Silver Layer Tables';
PRINT '========================================';
PRINT '';

-- =====================================================
-- 1. Silver.pos_transactions_header_clean
-- =====================================================
IF OBJECT_ID('Silver.pos_transactions_header_clean', 'U') IS NOT NULL
    DROP TABLE Silver.pos_transactions_header_clean;
GO

CREATE TABLE Silver.pos_transactions_header_clean (
    Transaction_Header_SK BIGINT IDENTITY(1,1) PRIMARY KEY,
    Transaction_ID VARCHAR(50) NOT NULL,
    Transaction_Date DATE NOT NULL,
    Transaction_TS DATETIME2 NOT NULL,
    Store_ID VARCHAR(50) NOT NULL,
    Customer_ID VARCHAR(50) NULL,
    Payment_Method VARCHAR(20) NOT NULL,
    Total_Amount DECIMAL(18,2) NOT NULL,
    Total_Discount DECIMAL(18,2) NULL,
    Line_Count INT NOT NULL,
    
    -- Calculated Fields
    Net_Amount AS (Total_Amount - ISNULL(Total_Discount, 0)) PERSISTED,
    Discount_Rate AS (
        CASE 
            WHEN Total_Amount > 0 
            THEN (ISNULL(Total_Discount, 0) / Total_Amount) * 100 
            ELSE 0 
        END
    ) PERSISTED,
    
    -- Business Flags
    Is_Walk_In BIT NOT NULL DEFAULT 0,
    Has_Discount BIT NOT NULL DEFAULT 0,
    Is_Valid BIT NOT NULL DEFAULT 1,
    DQ_Score INT,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT CHK_POS_Transaction_Amount CHECK (Total_Amount >= 0),
    CONSTRAINT CHK_POS_Transaction_Discount CHECK (Total_Discount IS NULL OR Total_Discount >= 0),
    CONSTRAINT CHK_POS_Transaction_Lines CHECK (Line_Count > 0)
);
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_POS_Transaction_ID 
    ON Silver.pos_transactions_header_clean(Transaction_ID);
CREATE NONCLUSTERED INDEX IX_POS_Transaction_Date 
    ON Silver.pos_transactions_header_clean(Transaction_Date);
CREATE NONCLUSTERED INDEX IX_POS_Store_ID 
    ON Silver.pos_transactions_header_clean(Store_ID);
CREATE NONCLUSTERED INDEX IX_POS_Customer_ID 
    ON Silver.pos_transactions_header_clean(Customer_ID) WHERE Customer_ID IS NOT NULL;
GO

PRINT '✓ Created: Silver.pos_transactions_header_clean';
GO

-- =====================================================
-- 2. Silver.pos_transactions_lines_clean
-- =====================================================
IF OBJECT_ID('Silver.pos_transactions_lines_clean', 'U') IS NOT NULL
    DROP TABLE Silver.pos_transactions_lines_clean;
GO

CREATE TABLE Silver.pos_transactions_lines_clean (
    Transaction_Line_SK BIGINT IDENTITY(1,1) PRIMARY KEY,
    Transaction_Line_ID VARCHAR(50) NOT NULL,
    Transaction_ID VARCHAR(50) NOT NULL,
    Line_Number INT NOT NULL,
    Product_SKU VARCHAR(50) NOT NULL,
    Store_ID VARCHAR(50) NOT NULL,
    Quantity INT NOT NULL,
    Unit_Price DECIMAL(18,2) NOT NULL,
    Cost_Price DECIMAL(18,2) NOT NULL,
    Discount_Amount DECIMAL(18,2) NULL,
    Line_Sales_Amount DECIMAL(18,2) NOT NULL,
    Promotion_ID VARCHAR(50) NULL,
    
    -- Calculated Fields
    Line_Cost AS (Quantity * Cost_Price) PERSISTED,
    Line_Margin AS (Line_Sales_Amount - (Quantity * Cost_Price)) PERSISTED,
    Line_Margin_Pct AS (
        CASE 
            WHEN Line_Sales_Amount > 0 
            THEN ((Line_Sales_Amount - (Quantity * Cost_Price)) / Line_Sales_Amount) * 100 
            ELSE 0 
        END
    ) PERSISTED,
    
    -- Business Flags
    Is_Return BIT NOT NULL DEFAULT 0,
    Is_Outlier_Qty BIT NOT NULL DEFAULT 0,
    Has_Discount BIT NOT NULL DEFAULT 0,
    Has_Promotion BIT NOT NULL DEFAULT 0,
    Has_Price_Issue BIT NOT NULL DEFAULT 0,
    Is_Valid BIT NOT NULL DEFAULT 1,
    DQ_Score INT,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT CHK_POS_Line_Quantity CHECK (Quantity <> 0),
    CONSTRAINT CHK_POS_Line_Unit_Price CHECK (Unit_Price > 0),
    CONSTRAINT CHK_POS_Line_Cost_Price CHECK (Cost_Price >= 0)
);
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_POS_Transaction_Line_ID 
    ON Silver.pos_transactions_lines_clean(Transaction_Line_ID);
CREATE NONCLUSTERED INDEX IX_POS_Line_Transaction_ID 
    ON Silver.pos_transactions_lines_clean(Transaction_ID);
CREATE NONCLUSTERED INDEX IX_POS_Line_Product_SKU 
    ON Silver.pos_transactions_lines_clean(Product_SKU);
CREATE NONCLUSTERED INDEX IX_POS_Line_Store_ID 
    ON Silver.pos_transactions_lines_clean(Store_ID);
CREATE NONCLUSTERED INDEX IX_POS_Line_Promotion_ID 
    ON Silver.pos_transactions_lines_clean(Promotion_ID) WHERE Promotion_ID IS NOT NULL;
GO

PRINT '✓ Created: Silver.pos_transactions_lines_clean';
GO

-- =====================================================
-- 3. Silver.erp_products_clean (SCD Type 2)
-- =====================================================
IF OBJECT_ID('Silver.erp_products_clean', 'U') IS NOT NULL
    DROP TABLE Silver.erp_products_clean;
GO

CREATE TABLE Silver.erp_products_clean (
    Product_SK BIGINT IDENTITY(1,1) PRIMARY KEY,
    Product_SKU VARCHAR(50) NOT NULL,
    Store_ID VARCHAR(50) NOT NULL,
    Product_Name VARCHAR(255) NOT NULL,
    Category VARCHAR(100) NOT NULL,
    Sub_Category VARCHAR(100) NOT NULL,
    Price DECIMAL(18,2) NOT NULL,
    Cost_Price DECIMAL(18,2) NULL,
    Supplier_ID VARCHAR(50) NOT NULL,
    
    -- SCD Type 2 Columns
    Effective_From DATE NOT NULL,
    Effective_To DATE NULL,
    Is_Current BIT NOT NULL DEFAULT 1,
    Version_Number INT NOT NULL DEFAULT 1,
    
    -- Calculated Fields
    Margin_Amount AS (Price - ISNULL(Cost_Price, 0)) PERSISTED,
    Margin_Pct AS (
        CASE 
            WHEN Price > 0 
            THEN ((Price - ISNULL(Cost_Price, 0)) / Price) * 100 
            ELSE 0 
        END
    ) PERSISTED,
    
    -- Business Flags
    Has_Cost_Price BIT NOT NULL DEFAULT 1,
    Has_Price_Issue BIT NOT NULL DEFAULT 0,
    Is_Valid BIT NOT NULL DEFAULT 1,
    DQ_Score INT,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT CHK_ERP_Product_Price CHECK (Price >= 0),
    CONSTRAINT CHK_ERP_Product_Cost_Price CHECK (Cost_Price IS NULL OR Cost_Price >= 0),
    CONSTRAINT CHK_ERP_Product_Effective_Dates CHECK (Effective_To IS NULL OR Effective_To >= Effective_From)
);
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_ERP_Product_Current 
    ON Silver.erp_products_clean(Product_SKU, Store_ID, Is_Current) 
    WHERE Is_Current = 1;
CREATE NONCLUSTERED INDEX IX_ERP_Product_SKU 
    ON Silver.erp_products_clean(Product_SKU);
CREATE NONCLUSTERED INDEX IX_ERP_Product_Category 
    ON Silver.erp_products_clean(Category);
CREATE NONCLUSTERED INDEX IX_ERP_Product_Effective_Dates 
    ON Silver.erp_products_clean(Effective_From, Effective_To);
GO

PRINT '✓ Created: Silver.erp_products_clean (SCD Type 2)';
GO

-- =====================================================
-- 4. Silver.erp_stores_clean
-- =====================================================
IF OBJECT_ID('Silver.erp_stores_clean', 'U') IS NOT NULL
    DROP TABLE Silver.erp_stores_clean;
GO

CREATE TABLE Silver.erp_stores_clean (
    Store_SK INT IDENTITY(1,1) PRIMARY KEY,
    Store_ID VARCHAR(50) NOT NULL UNIQUE,
    Store_Name VARCHAR(255) NOT NULL,
    City VARCHAR(100) NOT NULL,
    Region VARCHAR(100) NOT NULL,
    Store_Manager VARCHAR(255) NULL,
    Store_Area_sqm DECIMAL(10,2) NOT NULL,
    Open_Date DATE NOT NULL,
    
    -- Calculated Field (NOT PERSISTED - uses GETDATE)
    Store_Age_Years AS (DATEDIFF(YEAR, Open_Date, GETDATE())),
    
    -- Business Flags
    Has_Multiple_Managers BIT NOT NULL DEFAULT 0,
    Is_Valid BIT NOT NULL DEFAULT 1,
    DQ_Score INT,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT CHK_ERP_Store_Area CHECK (Store_Area_sqm > 0),
    CONSTRAINT CHK_ERP_Store_Open_Date CHECK (Open_Date <= GETDATE())
);
GO

CREATE NONCLUSTERED INDEX IX_ERP_Store_City 
    ON Silver.erp_stores_clean(City);
CREATE NONCLUSTERED INDEX IX_ERP_Store_Region 
    ON Silver.erp_stores_clean(Region);
GO

PRINT '✓ Created: Silver.erp_stores_clean';
GO

-- =====================================================
-- 5. Silver.crm_customers_clean
-- =====================================================
IF OBJECT_ID('Silver.crm_customers_clean', 'U') IS NOT NULL
    DROP TABLE Silver.crm_customers_clean;
GO

CREATE TABLE Silver.crm_customers_clean (
    Customer_SK BIGINT IDENTITY(1,1) PRIMARY KEY,
    Customer_ID VARCHAR(50) NOT NULL UNIQUE,
    Full_Name VARCHAR(255) NOT NULL,
    Gender CHAR(1) NOT NULL,
    Birthdate DATE NOT NULL,
    Registration_Date DATE NOT NULL,
    Email_Masked VARCHAR(255) NULL,
    Phone_Masked VARCHAR(50) NULL,
    City VARCHAR(100) NULL,
    Preferred_Channel VARCHAR(20) NOT NULL,
    
    -- Calculated Fields (NOT PERSISTED - use GETDATE)
    Age AS (DATEDIFF(YEAR, Birthdate, GETDATE())),
    Customer_Tenure_Days AS (DATEDIFF(DAY, Registration_Date, GETDATE())),
    
    -- Business Flags
    Has_Email BIT NOT NULL DEFAULT 0,
    Has_Phone BIT NOT NULL DEFAULT 0,
    Is_Valid BIT NOT NULL DEFAULT 1,
    DQ_Score INT,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT CHK_CRM_Customer_Gender CHECK (Gender IN ('M', 'F', 'U')),
    CONSTRAINT CHK_CRM_Customer_Age CHECK (Birthdate < GETDATE() AND Birthdate > DATEADD(YEAR, -120, GETDATE())),
    CONSTRAINT CHK_CRM_Customer_Registration CHECK (Registration_Date <= GETDATE())
);
GO

CREATE NONCLUSTERED INDEX IX_CRM_Customer_City 
    ON Silver.crm_customers_clean(City);
CREATE NONCLUSTERED INDEX IX_CRM_Customer_Registration_Date 
    ON Silver.crm_customers_clean(Registration_Date);
GO

PRINT '✓ Created: Silver.crm_customers_clean';
GO

-- =====================================================
-- 6. Silver.mkt_promotions_clean
-- =====================================================
IF OBJECT_ID('Silver.mkt_promotions_clean', 'U') IS NOT NULL
    DROP TABLE Silver.mkt_promotions_clean;
GO

CREATE TABLE Silver.mkt_promotions_clean (
    Promotion_SK INT IDENTITY(1,1) PRIMARY KEY,
    Promotion_ID VARCHAR(50) NOT NULL UNIQUE,
    Promo_Name VARCHAR(255) NOT NULL,
    Promo_Type VARCHAR(50) NOT NULL,
    Start_Date DATE NOT NULL,
    End_Date DATE NOT NULL,
    Promo_Cost DECIMAL(18,2) NOT NULL,
    Eligible_SKUs NVARCHAR(MAX) NULL,
    
    -- Calculated Fields
    Promo_Duration_Days AS (DATEDIFF(DAY, Start_Date, End_Date)) PERSISTED,
    Is_Active AS (
        CASE 
            WHEN GETDATE() BETWEEN Start_Date AND End_Date 
            THEN 1 ELSE 0 
        END
    ),
    
    -- Business Flags
    Has_Multiple_SKUs BIT NOT NULL DEFAULT 0,
    Has_Invalid_Date_Range BIT NOT NULL DEFAULT 0,
    Is_Valid BIT NOT NULL DEFAULT 1,
    DQ_Score INT,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT CHK_MKT_Promo_Dates CHECK (End_Date >= Start_Date),
    CONSTRAINT CHK_MKT_Promo_Cost CHECK (Promo_Cost >= 0)
);
GO

CREATE NONCLUSTERED INDEX IX_MKT_Promotion_Dates 
    ON Silver.mkt_promotions_clean(Start_Date, End_Date);
CREATE NONCLUSTERED INDEX IX_MKT_Promotion_Type 
    ON Silver.mkt_promotions_clean(Promo_Type);
GO

PRINT '✓ Created: Silver.mkt_promotions_clean';
GO

-- =====================================================
-- 7. Silver.erp_inventory_clean
-- =====================================================
IF OBJECT_ID('Silver.erp_inventory_clean', 'U') IS NOT NULL
    DROP TABLE Silver.erp_inventory_clean;
GO

CREATE TABLE Silver.erp_inventory_clean (
    Inventory_SK BIGINT IDENTITY(1,1) PRIMARY KEY,
    Inventory_ID VARCHAR(50) NOT NULL,
    Product_SKU VARCHAR(50) NOT NULL,
    Store_ID VARCHAR(50) NOT NULL,
    Snapshot_Date DATE NOT NULL,
    Quantity_On_Hand INT NOT NULL,
    Reorder_Level INT NOT NULL,
    
    -- Business Flags
    Is_Below_Reorder_Level BIT NOT NULL DEFAULT 0,
    Is_Negative_Quantity BIT NOT NULL DEFAULT 0,
    Is_Outlier_Quantity BIT NOT NULL DEFAULT 0,
    Is_Latest_Snapshot BIT NOT NULL DEFAULT 1,
    Is_Valid BIT NOT NULL DEFAULT 1,
    DQ_Score INT,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT CHK_ERP_Inventory_Reorder CHECK (Reorder_Level >= 0)
);
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_ERP_Inventory_Latest 
    ON Silver.erp_inventory_clean(Product_SKU, Store_ID, Is_Latest_Snapshot) 
    WHERE Is_Latest_Snapshot = 1;
CREATE NONCLUSTERED INDEX IX_ERP_Inventory_Product 
    ON Silver.erp_inventory_clean(Product_SKU);
CREATE NONCLUSTERED INDEX IX_ERP_Inventory_Store 
    ON Silver.erp_inventory_clean(Store_ID);
CREATE NONCLUSTERED INDEX IX_ERP_Inventory_Snapshot_Date 
    ON Silver.erp_inventory_clean(Snapshot_Date);
GO

PRINT '✓ Created: Silver.erp_inventory_clean';
GO

-- =====================================================
-- 8. Silver.api_weather_clean
-- =====================================================
IF OBJECT_ID('Silver.api_weather_clean', 'U') IS NOT NULL
    DROP TABLE Silver.api_weather_clean;
GO

CREATE TABLE Silver.api_weather_clean (
    Weather_SK BIGINT IDENTITY(1,1) PRIMARY KEY,
    Weather_Date DATE NOT NULL,
    Store_ID VARCHAR(50) NOT NULL,
    Temperature_C DECIMAL(5,2) NULL,
    Precipitation_mm DECIMAL(5,2) NOT NULL,
    Weather_Condition VARCHAR(100) NOT NULL,
    
    -- Business Flags
    Has_Missing_Temperature BIT NOT NULL DEFAULT 0,
    Is_Extreme_Temperature BIT NOT NULL DEFAULT 0,
    Is_Rainy AS (CASE WHEN Precipitation_mm > 0 THEN 1 ELSE 0 END) PERSISTED,
    Is_Valid BIT NOT NULL DEFAULT 1,
    DQ_Score INT,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Created_TS DATETIME2 DEFAULT GETDATE(),
    Updated_TS DATETIME2 DEFAULT GETDATE()
);
GO

CREATE UNIQUE NONCLUSTERED INDEX UX_API_Weather_Date_Store 
    ON Silver.api_weather_clean(Weather_Date, Store_ID);
CREATE NONCLUSTERED INDEX IX_API_Weather_Date 
    ON Silver.api_weather_clean(Weather_Date);
CREATE NONCLUSTERED INDEX IX_API_Weather_Store 
    ON Silver.api_weather_clean(Store_ID);
GO

PRINT '✓ Created: Silver.api_weather_clean';
GO

-- =====================================================
-- Summary
-- =====================================================
PRINT '';
PRINT '========================================';
PRINT 'Silver Layer DDL Execution Complete!';
PRINT '========================================';
PRINT '';
PRINT 'Created Tables with Source Prefixes:';
PRINT '  1. Silver.pos_transactions_header_clean  (POS)';
PRINT '  2. Silver.pos_transactions_lines_clean   (POS)';
PRINT '  3. Silver.erp_products_clean             (ERP - SCD Type 2)';
PRINT '  4. Silver.erp_stores_clean               (ERP)';
PRINT '  5. Silver.crm_customers_clean            (CRM)';
PRINT '  6. Silver.mkt_promotions_clean           (MKT)';
PRINT '  7. Silver.erp_inventory_clean            (ERP)';
PRINT '  8. Silver.api_weather_clean              (API)';
PRINT '';
PRINT 'Naming Convention: {source_prefix}_{entity}_clean';
PRINT '  - pos_  = Point of Sale transactions';
PRINT '  - erp_  = Enterprise Resource Planning';
PRINT '  - crm_  = Customer Relationship Management';
PRINT '  - mkt_  = Marketing campaigns';
PRINT '  - api_  = External API data';
PRINT '';
PRINT 'Next: Run dbt Silver models to populate tables';
PRINT '========================================';
GO