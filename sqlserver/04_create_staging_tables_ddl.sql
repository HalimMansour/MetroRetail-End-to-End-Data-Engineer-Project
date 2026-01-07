-- =====================================================
-- MetroRetail - Staging Layer DDL (CORRECTED)
-- Staging layer: Typed columns with data quality tracking
-- Conversion from Raw VARCHAR to proper data types
-- =====================================================

USE MetroRetailDB;
GO

-- =====================================================
-- 1. Staging - POS Transactions Header
-- =====================================================
IF OBJECT_ID('Staging.stg_pos_transactions_header', 'U') IS NOT NULL
    DROP TABLE Staging.stg_pos_transactions_header;
GO

CREATE TABLE Staging.stg_pos_transactions_header (
    Staging_ID INT IDENTITY(1,1) PRIMARY KEY,
    Raw_ID INT NOT NULL,
    
    -- Business Keys
    Transaction_ID VARCHAR(50) NOT NULL,
    
    -- Typed Attributes
    Transaction_Date DATE NULL,
    Transaction_TS DATETIME2 NULL,
    Store_ID VARCHAR(50) NULL,
    Customer_ID VARCHAR(50) NULL,
    Payment_Method VARCHAR(50) NULL,
    Total_Amount DECIMAL(18,2) NULL,
    Total_Discount DECIMAL(18,2) NULL,
    Line_Count INT NULL,
    
    -- Data Quality Flags
    DQ_Transaction_Date_Valid BIT DEFAULT 0,
    DQ_Transaction_TS_Valid BIT DEFAULT 0,
    DQ_Store_ID_Valid BIT DEFAULT 0,
    DQ_Customer_ID_Valid BIT DEFAULT 0,
    DQ_Total_Amount_Valid BIT DEFAULT 0,
    DQ_Total_Discount_Valid BIT DEFAULT 0,
    DQ_Line_Count_Valid BIT DEFAULT 0,
    DQ_Is_Valid BIT DEFAULT 0,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UQ_stg_pos_transactions_header_transaction 
        UNIQUE (Transaction_ID, Batch_ID)
);
GO

CREATE NONCLUSTERED INDEX IX_stg_pos_transactions_header_batch 
    ON Staging.stg_pos_transactions_header(Batch_ID);
CREATE NONCLUSTERED INDEX IX_stg_pos_transactions_header_date 
    ON Staging.stg_pos_transactions_header(Transaction_Date);
GO

PRINT '✓ Created: Staging.stg_pos_transactions_header';
GO

-- =====================================================
-- 2. Staging - POS Transactions Lines
-- =====================================================
IF OBJECT_ID('Staging.stg_pos_transactions_lines', 'U') IS NOT NULL
    DROP TABLE Staging.stg_pos_transactions_lines;
GO

CREATE TABLE Staging.stg_pos_transactions_lines (
    Staging_ID INT IDENTITY(1,1) PRIMARY KEY,
    Raw_ID INT NOT NULL,
    
    -- Business Keys
    Transaction_Line_ID VARCHAR(50) NOT NULL,
    Transaction_ID VARCHAR(50) NOT NULL,
    
    -- Typed Attributes
    Line_Number INT NULL,
    Product_SKU VARCHAR(50) NULL,
    Store_ID VARCHAR(50) NULL,
    Quantity INT NULL,
    Unit_Price DECIMAL(18,2) NULL,
    Cost_Price DECIMAL(18,2) NULL,
    Discount_Amount DECIMAL(18,2) NULL,
    Line_Sales_Amount DECIMAL(18,2) NULL,   -- some values include currency formatting
    Promotion_ID VARCHAR(50) NULL,
    
    -- Data Quality Flags
    DQ_Line_Number_Valid BIT DEFAULT 0,
    DQ_Product_SKU_Valid BIT DEFAULT 0,
    DQ_Store_ID_Valid BIT DEFAULT 0,
    DQ_Quantity_Valid BIT DEFAULT 0,
    DQ_Unit_Price_Valid BIT DEFAULT 0,
    DQ_Cost_Price_Valid BIT DEFAULT 0,
    DQ_Line_Sales_Amount_Valid BIT DEFAULT 0,
    DQ_Is_Valid BIT DEFAULT 0,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UQ_stg_pos_transactions_lines_line 
        UNIQUE (Transaction_Line_ID, Batch_ID)
);
GO

CREATE NONCLUSTERED INDEX IX_stg_pos_transactions_lines_batch 
    ON Staging.stg_pos_transactions_lines(Batch_ID);
CREATE NONCLUSTERED INDEX IX_stg_pos_transactions_lines_transaction 
    ON Staging.stg_pos_transactions_lines(Transaction_ID);
GO

PRINT '✓ Created: Staging.stg_pos_transactions_lines';
GO

-- =====================================================
-- 3. Staging - ERP Products
-- =====================================================
IF OBJECT_ID('Staging.stg_erp_products', 'U') IS NOT NULL
    DROP TABLE Staging.stg_erp_products;
GO

CREATE TABLE Staging.stg_erp_products (
    Staging_ID INT IDENTITY(1,1) PRIMARY KEY,
    Raw_ID INT NOT NULL,
    
    -- Business Keys
    Product_SKU VARCHAR(50) NOT NULL,
    
    -- Typed Attributes
    Product_Name VARCHAR(255) NULL,
    Category VARCHAR(100) NULL,
    Sub_Category VARCHAR(100) NULL,
    Price DECIMAL(18,2) NULL,
    Cost_Price DECIMAL(18,2) NULL,
    Supplier_ID VARCHAR(50) NULL,
    Last_Updated DATE NULL,
    -- Data Quality Flags
    
    DQ_Product_Name_Valid BIT DEFAULT 0,
    DQ_Category_Valid BIT DEFAULT 0,
    DQ_Sub_Category_Valid BIT DEFAULT 0,
    DQ_Price_Valid BIT DEFAULT 0,
    DQ_Cost_Price_Valid BIT DEFAULT 0,
    DQ_Supplier_ID_Valid BIT DEFAULT 0,
    DQ_Last_Updated_Valid BIT DEFAULT 0,
    DQ_Is_Valid BIT DEFAULT 0,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UQ_stg_erp_products_sku 
        UNIQUE (Product_SKU, Batch_ID)
);
GO

CREATE NONCLUSTERED INDEX IX_stg_erp_products_batch 
    ON Staging.stg_erp_products(Batch_ID);
CREATE NONCLUSTERED INDEX IX_stg_erp_products_category 
    ON Staging.stg_erp_products(Category, Sub_Category);
GO

PRINT '✓ Created: Staging.stg_erp_products';
GO

-- =====================================================
-- 4. Staging - ERP Stores
-- =====================================================
IF OBJECT_ID('Staging.stg_erp_stores', 'U') IS NOT NULL
    DROP TABLE Staging.stg_erp_stores;
GO

CREATE TABLE Staging.stg_erp_stores (
    Staging_ID INT IDENTITY(1,1) PRIMARY KEY,
    Raw_ID INT NOT NULL,
    
    -- Business Keys
    Store_ID VARCHAR(50) NULL,
    
    -- Typed Attributes
    Store_Name VARCHAR(255) NULL,
    City VARCHAR(100) NULL,
    Region VARCHAR(100) NULL,
    Store_Manager VARCHAR(255) NULL,
    Store_Area_sqm INT NULL,
    Open_Date DATE NULL,
    
    -- Data Quality Flags
    DQ_Store_ID_Valid BIT DEFAULT 0,
    DQ_Store_Name_Valid BIT DEFAULT 0,
    DQ_City_Valid BIT DEFAULT 0,
    DQ_Region_Valid BIT DEFAULT 0,
    DQ_Store_Area_Valid BIT DEFAULT 0,
    DQ_Open_Date_Valid BIT DEFAULT 0,
    DQ_Is_Valid BIT DEFAULT 0,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UQ_stg_erp_stores_store 
        UNIQUE (Store_ID, Batch_ID)
);
GO

CREATE NONCLUSTERED INDEX IX_stg_erp_stores_batch 
    ON Staging.stg_erp_stores(Batch_ID);
GO

PRINT '✓ Created: Staging.stg_erp_stores';
GO

-- =====================================================
-- 5. Staging - ERP Inventory
-- =====================================================
IF OBJECT_ID('Staging.stg_erp_inventory', 'U') IS NOT NULL
    DROP TABLE Staging.stg_erp_inventory;
GO

CREATE TABLE Staging.stg_erp_inventory (
    Staging_ID INT IDENTITY(1,1) PRIMARY KEY,
    Raw_ID INT NOT NULL,
    
    -- Business Keys
    Inventory_ID VARCHAR(50) NOT NULL,
    
    -- Typed Attributes
    Product_SKU VARCHAR(50) NULL,
    Store_ID VARCHAR(50) NULL,
    Snapshot_Date DATE NULL,
    Quantity_On_Hand INT NULL,
    Reorder_Level INT NULL,
    
    -- Data Quality Flags
    DQ_Product_SKU_Valid BIT DEFAULT 0,
    DQ_Store_ID_Valid BIT DEFAULT 0,
    DQ_Snapshot_Date_Valid BIT DEFAULT 0,
    DQ_Quantity_On_Hand_Valid BIT DEFAULT 0,
    DQ_Reorder_Level_Valid BIT DEFAULT 0,
    DQ_Is_Valid BIT DEFAULT 0,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UQ_stg_erp_inventory_id 
        UNIQUE (Inventory_ID, Batch_ID)
);
GO

CREATE NONCLUSTERED INDEX IX_stg_erp_inventory_batch 
    ON Staging.stg_erp_inventory(Batch_ID);
CREATE NONCLUSTERED INDEX IX_stg_erp_inventory_snapshot 
    ON Staging.stg_erp_inventory(Snapshot_Date);
GO

PRINT '✓ Created: Staging.stg_erp_inventory';
GO

-- =====================================================
-- 6. Staging - CRM Customers
-- =====================================================
IF OBJECT_ID('Staging.stg_crm_customers', 'U') IS NOT NULL
    DROP TABLE Staging.stg_crm_customers;
GO

CREATE TABLE Staging.stg_crm_customers (
    Staging_ID INT IDENTITY(1,1) PRIMARY KEY,
    Raw_ID INT NOT NULL,
    
    -- Business Keys
    Customer_ID VARCHAR(50) NULL,
    
    -- Typed Attributes
    Full_Name VARCHAR(255) NULL,
    Gender VARCHAR(50) NULL,
    Birthdate DATE NULL,
    Registration_Date DATE NULL,
    Email VARCHAR(255) NULL,
    Phone_Number VARCHAR(50) NULL,
    City VARCHAR(100) NULL,
    Preferred_Channel VARCHAR(50) NULL,
    
    -- Data Quality Flags
    DQ_Customer_ID_Valid BIT DEFAULT 0,
    DQ_Full_Name_Valid BIT DEFAULT 0,
    DQ_Gender_Valid BIT DEFAULT 0,
    DQ_Birthdate_Valid BIT DEFAULT 0,
    DQ_Registration_Date_Valid BIT DEFAULT 0,
    DQ_Email_Valid BIT DEFAULT 0,
    DQ_Phone_Valid BIT DEFAULT 0,
    DQ_Is_Valid BIT DEFAULT 0,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UQ_stg_crm_customers_id 
        UNIQUE (Customer_ID, Batch_ID)
);
GO

CREATE NONCLUSTERED INDEX IX_stg_crm_customers_batch 
    ON Staging.stg_crm_customers(Batch_ID);
GO

PRINT '✓ Created: Staging.stg_crm_customers';
GO

-- =====================================================
-- 7. Staging - MKT Promotions
-- =====================================================
IF OBJECT_ID('Staging.stg_mkt_promotions', 'U') IS NOT NULL
    DROP TABLE Staging.stg_mkt_promotions;
GO

CREATE TABLE Staging.stg_mkt_promotions (
    Staging_ID INT IDENTITY(1,1) PRIMARY KEY,
    Raw_ID INT NOT NULL,
    
    -- Business Keys
    Promotion_ID VARCHAR(50) NOT NULL,
    
    -- Typed Attributes
    Promo_Name VARCHAR(255) NULL,
    Promo_Type VARCHAR(50) NULL,
    Start_Date DATE NULL,
    End_Date DATE NULL,
    Promo_Cost DECIMAL(18,2) NULL,
    Eligible_SKUs NVARCHAR(MAX) NULL,
    
    -- Data Quality Flags
    DQ_Promo_Name_Valid BIT DEFAULT 0,
    DQ_Promo_Type_Valid BIT DEFAULT 0,
    DQ_Start_Date_Valid BIT DEFAULT 0,
    DQ_End_Date_Valid BIT DEFAULT 0,
    DQ_Promo_Cost_Valid BIT DEFAULT 0,
    DQ_Date_Range_Valid BIT DEFAULT 0,
    DQ_Is_Valid BIT DEFAULT 0,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UQ_stg_mkt_promotions_id 
        UNIQUE (Promotion_ID, Batch_ID)
);
GO

CREATE NONCLUSTERED INDEX IX_stg_mkt_promotions_batch 
    ON Staging.stg_mkt_promotions(Batch_ID);
CREATE NONCLUSTERED INDEX IX_stg_mkt_promotions_dates 
    ON Staging.stg_mkt_promotions(Start_Date, End_Date);
GO

PRINT '✓ Created: Staging.stg_mkt_promotions';
GO

-- =====================================================
-- 8. Staging - API Weather
-- =====================================================
IF OBJECT_ID('Staging.stg_api_weather', 'U') IS NOT NULL
    DROP TABLE Staging.stg_api_weather;
GO

CREATE TABLE Staging.stg_api_weather (
    Staging_ID INT IDENTITY(1,1) PRIMARY KEY,
    Raw_ID INT NOT NULL,
    
    -- Business Keys
    Weather_Date DATE NULL,
    Retail_Location_ID INT NULL,
    
    -- Typed Attributes
    Temperature_C DECIMAL(5,2) NULL,
    Precipitation_mm DECIMAL(5,2) NULL,
    Weather_Condition VARCHAR(100) NULL,
    
    -- Data Quality Flags
    DQ_Weather_Date_Valid BIT DEFAULT 0,
    DQ_Retail_Location_ID_Valid BIT DEFAULT 0,
    DQ_Temperature_Valid BIT DEFAULT 0,
    DQ_Precipitation_Valid BIT DEFAULT 0,
    DQ_Weather_Condition_Valid BIT DEFAULT 0,
    DQ_Is_Valid BIT DEFAULT 0,
    
    -- Metadata
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT UQ_stg_api_weather_date_location 
        UNIQUE (Weather_Date, Retail_Location_ID, Batch_ID)
);
GO

CREATE NONCLUSTERED INDEX IX_stg_api_weather_batch 
    ON Staging.stg_api_weather(Batch_ID);
CREATE NONCLUSTERED INDEX IX_stg_api_weather_date 
    ON Staging.stg_api_weather(Weather_Date);
GO

PRINT '✓ Created: Staging.stg_api_weather';
GO

-- =====================================================
-- Summary
-- =====================================================
PRINT '';
PRINT '========================================';
PRINT 'Staging Layer DDL Execution Complete!';
PRINT '========================================';
PRINT '';
PRINT 'Created Tables (with typed columns + DQ flags):';
PRINT '  1. Staging.stg_pos_transactions_header';
PRINT '  2. Staging.stg_pos_transactions_lines';
PRINT '  3. Staging.stg_erp_products';
PRINT '  4. Staging.stg_erp_stores';
PRINT '  5. Staging.stg_erp_inventory';
PRINT '  6. Staging.stg_crm_customers';
PRINT '  7. Staging.stg_mkt_promotions';
PRINT '  8. Staging.stg_api_weather';
PRINT '';
PRINT 'Key Features:';
PRINT '  - Proper data types (INT, DECIMAL, DATE)';
PRINT '  - Data Quality flags (DQ_*)';
PRINT '  - Raw_ID links back to source';
PRINT '  - Unique constraints on business keys';
PRINT '  - Performance indexes';
PRINT '========================================';
GO