-- =====================================================
-- MetroRetail - Raw Layer DDL (FIXED)
-- Raw layer accepts ALL columns as VARCHAR/NVARCHAR
-- Type conversion happens in Staging layer
-- =====================================================

USE MetroRetailDB;
GO

-- =====================================================
-- 1. Manifest Table (tracks all ingestion batches)
-- =====================================================
IF OBJECT_ID('Raw.Ingestion_Manifest', 'U') IS NOT NULL
    DROP TABLE Raw.Ingestion_Manifest;
GO

CREATE TABLE Raw.Ingestion_Manifest (
    Manifest_ID INT IDENTITY(1,1) PRIMARY KEY,
    Batch_ID VARCHAR(100) NOT NULL UNIQUE,
    Source_System VARCHAR(50) NOT NULL,
    Entity_Name VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Row_Count INT NOT NULL,
    Load_Start_TS DATETIME2 NOT NULL,
    Load_End_TS DATETIME2 NULL,
    Load_Status VARCHAR(20) NOT NULL,
    Error_Message NVARCHAR(MAX) NULL,
    Load_Duration_Seconds AS DATEDIFF(SECOND, Load_Start_TS, Load_End_TS),
    Created_TS DATETIME2 DEFAULT GETDATE()
);
GO

PRINT '✓ Created: Raw.Ingestion_Manifest';
GO

-- =====================================================
-- 2. POS - Transactions Header (ALL VARCHAR)
-- =====================================================
IF OBJECT_ID('Raw.pos_transactions_header', 'U') IS NOT NULL
    DROP TABLE Raw.pos_transactions_header;
GO

CREATE TABLE Raw.pos_transactions_header (
    Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    Transaction_ID VARCHAR(50) NOT NULL,
    Transaction_Date VARCHAR(50) NOT NULL,
    Transaction_TS VARCHAR(50) NOT NULL,
    Store_ID VARCHAR(50) NOT NULL,
    Customer_ID VARCHAR(50) NULL,
    Payment_Method VARCHAR(50) NOT NULL,
    Total_Amount VARCHAR(50) NOT NULL,
    Total_Discount VARCHAR(50) NULL,
    Line_Count VARCHAR(50) NOT NULL,
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_pos_transactions_header_batch 
    ON Raw.pos_transactions_header(Batch_ID);
GO

PRINT '✓ Created: Raw.pos_transactions_header';
GO

-- =====================================================
-- 3. POS - Transactions Lines (ALL VARCHAR)
-- =====================================================
IF OBJECT_ID('Raw.pos_transactions_lines', 'U') IS NOT NULL
    DROP TABLE Raw.pos_transactions_lines;
GO

CREATE TABLE Raw.pos_transactions_lines (
    Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    Transaction_Line_ID VARCHAR(50) NOT NULL,
    Transaction_ID VARCHAR(50) NOT NULL,
    Line_Number VARCHAR(50) NOT NULL,
    Product_SKU VARCHAR(50) NOT NULL,
    Store_ID VARCHAR(50) NOT NULL,
    Quantity VARCHAR(50) NOT NULL,
    Unit_Price VARCHAR(50) NOT NULL,
    Cost_Price VARCHAR(50) NOT NULL,
    Discount_Amount VARCHAR(50) NULL,
    Line_Sales_Amount VARCHAR(50) NOT NULL,
    Promotion_ID VARCHAR(50) NULL,
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_pos_transactions_lines_batch 
    ON Raw.pos_transactions_lines(Batch_ID);
GO

PRINT '✓ Created: Raw.pos_transactions_lines';
GO

-- =====================================================
-- 4. ERP - Products (ALL VARCHAR)
-- =====================================================
IF OBJECT_ID('Raw.erp_products', 'U') IS NOT NULL
    DROP TABLE Raw.erp_products;
GO

CREATE TABLE Raw.erp_products (
    Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    Product_SKU VARCHAR(50) NOT NULL,
    Product_Name VARCHAR(255) NOT NULL,
    Category VARCHAR(100) NOT NULL,
    Sub_Category VARCHAR(100) NOT NULL,
    Price VARCHAR(50) NOT NULL,
    Cost_Price VARCHAR(50) NULL,
    Supplier_ID VARCHAR(50) NOT NULL,
    Last_Updated VARCHAR(50) NOT NULL,
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_erp_products_batch 
    ON Raw.erp_products(Batch_ID);
GO

PRINT '✓ Created: Raw.erp_products';
GO

-- =====================================================
-- 5. ERP - Stores (ALL VARCHAR)
-- =====================================================
IF OBJECT_ID('Raw.erp_stores', 'U') IS NOT NULL
    DROP TABLE Raw.erp_stores;
GO

CREATE TABLE Raw.erp_stores (
    Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    Store_ID VARCHAR(50) NOT NULL,
    Store_Name VARCHAR(255) NOT NULL,
    City VARCHAR(100) NOT NULL,
    Region VARCHAR(100) NOT NULL,
    Store_Manager VARCHAR(255) NULL,
    Store_Area_sqm VARCHAR(50) NOT NULL,
    Open_Date VARCHAR(50) NOT NULL,
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_erp_stores_batch 
    ON Raw.erp_stores(Batch_ID);
GO

PRINT '✓ Created: Raw.erp_stores';
GO

-- =====================================================
-- 6. ERP - Inventory (ALL VARCHAR)
-- =====================================================
IF OBJECT_ID('Raw.erp_inventory', 'U') IS NOT NULL
    DROP TABLE Raw.erp_inventory;
GO

CREATE TABLE Raw.erp_inventory (
    Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    Inventory_ID VARCHAR(50) NOT NULL,
    Product_SKU VARCHAR(50) NOT NULL,
    Store_ID VARCHAR(50) NOT NULL,
    Snapshot_Date VARCHAR(50) NOT NULL,
    Quantity_On_Hand VARCHAR(50) NOT NULL,
    Reorder_Level VARCHAR(50) NOT NULL,
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_erp_inventory_batch 
    ON Raw.erp_inventory(Batch_ID);
GO

PRINT '✓ Created: Raw.erp_inventory';
GO

-- =====================================================
-- 7. CRM - Customers (ALL VARCHAR)
-- =====================================================
IF OBJECT_ID('Raw.crm_customers', 'U') IS NOT NULL
    DROP TABLE Raw.crm_customers;
GO

CREATE TABLE Raw.crm_customers (
    Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    Customer_ID VARCHAR(50) NOT NULL,
    Full_Name VARCHAR(255) NOT NULL,
    Gender VARCHAR(50) NOT NULL,
    Birthdate VARCHAR(50) NOT NULL,
    Registration_Date VARCHAR(50) NOT NULL,
    Email VARCHAR(255) NULL,
    Phone_Number VARCHAR(50) NULL,
    City VARCHAR(100) NULL,
    Preferred_Channel VARCHAR(50) NOT NULL,
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_crm_customers_batch 
    ON Raw.crm_customers(Batch_ID);
GO

PRINT '✓ Created: Raw.crm_customers';
GO

-- =====================================================
-- 8. MKT - Promotions (ALL VARCHAR except NVARCHAR(MAX))
-- =====================================================
IF OBJECT_ID('Raw.mkt_promotions', 'U') IS NOT NULL
    DROP TABLE Raw.mkt_promotions;
GO

CREATE TABLE Raw.mkt_promotions (
    Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    Promotion_ID VARCHAR(50) NOT NULL,
    Promo_Name VARCHAR(255) NOT NULL,
    Promo_Type VARCHAR(50) NOT NULL,
    Start_Date VARCHAR(50) NOT NULL,
    End_Date VARCHAR(50) NOT NULL,
    Promo_Cost VARCHAR(50) NOT NULL,
    Eligible_SKUs NVARCHAR(MAX) NULL,
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_mkt_promotions_batch 
    ON Raw.mkt_promotions(Batch_ID);
GO

PRINT '✓ Created: Raw.mkt_promotions';
GO

-- =====================================================
-- 9. API - Weather (ALL VARCHAR)
-- =====================================================
IF OBJECT_ID('Raw.api_weather', 'U') IS NOT NULL
    DROP TABLE Raw.api_weather;
GO

CREATE TABLE Raw.api_weather (
    Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    Weather_Date VARCHAR(50) NOT NULL,
    Retail_Location_ID VARCHAR(50) NOT NULL,
    Temperature_C VARCHAR(50) NULL,
    Precipitation_mm VARCHAR(50) NOT NULL,
    Weather_Condition VARCHAR(100) NOT NULL,
    Batch_ID VARCHAR(100) NOT NULL,
    Source_File VARCHAR(255) NOT NULL,
    Load_Timestamp DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_api_weather_batch 
    ON Raw.api_weather(Batch_ID);
GO

PRINT '✓ Created: Raw.api_weather';
GO

-- =====================================================
-- Summary
-- =====================================================
PRINT '';
PRINT '========================================';
PRINT 'Raw Layer DDL Execution Complete!';
PRINT '========================================';
PRINT '';
PRINT 'Created Tables (ALL VARCHAR for dirty data):';
PRINT '  1. Raw.Ingestion_Manifest';
PRINT '  2. Raw.pos_transactions_header';
PRINT '  3. Raw.pos_transactions_lines';
PRINT '  4. Raw.erp_products';
PRINT '  5. Raw.erp_stores';
PRINT '  6. Raw.erp_inventory';
PRINT '  7. Raw.crm_customers';
PRINT '  8. Raw.mkt_promotions';
PRINT '  9. Raw.api_weather';
PRINT '';
PRINT 'Note: Type conversion will happen in Staging layer';
PRINT '========================================';
GO