-- =====================================================
-- MetroRetail Database Schema Setup
-- Creates schemas for Medallion Architecture
-- =====================================================

USE MetroRetailDB;
GO

-- Create schemas if they don't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Raw')
BEGIN
    EXEC('CREATE SCHEMA Raw');
    PRINT 'Schema Raw created';
END
ELSE
    PRINT 'Schema Raw already exists';
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Staging')
BEGIN
    EXEC('CREATE SCHEMA Staging');
    PRINT 'Schema Staging created';
END
ELSE
    PRINT 'Schema Staging already exists';
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Silver')
BEGIN
    EXEC('CREATE SCHEMA Silver');
    PRINT 'Schema Silver created';
END
ELSE
    PRINT 'Schema Silver already exists';
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Gold')
BEGIN
    EXEC('CREATE SCHEMA Gold');
    PRINT 'Schema Gold created';
END
ELSE
    PRINT 'Schema Gold already exists';
GO

PRINT 'All schemas created successfully!';