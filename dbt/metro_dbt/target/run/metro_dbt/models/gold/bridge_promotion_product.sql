
  
    USE [MetroRetailDB];
    USE [MetroRetailDB];
    
    

    

    
    USE [MetroRetailDB];
    EXEC('
        create view "Gold"."bridge_promotion_product__dbt_tmp__dbt_tmp_vw" as 

/*
    Gold Model: Promotion-Product Bridge Table
    Source: Silver.mkt_promotions_clean + Silver.erp_products_clean
    Target: Gold.bridge_promotion_product

    APPROACH: Use natural keys (Promotion_ID, Product_SKU) 
    Then join to dimensions in a view or Power BI
    
    Handles:
    - "P0047|P0133|P0177|P0067|P0114" → 5 rows
    - "P0189" → 1 row
    - "N/A" → No rows (store-level promo)
*/

WITH promotions AS (
    SELECT 
        Promotion_ID,
        Promo_Name,
        Eligible_SKUs,
        Start_Date,
        End_Date
    FROM "MetroRetailDB"."Silver"."mkt_promotions_clean"
    WHERE Is_Valid = 1
        AND Eligible_SKUs IS NOT NULL 
        AND Eligible_SKUs <> ''N/A''  -- Exclude store-level promos
),

-- Parse pipe-separated SKUs into individual rows
parsed_skus AS (
    SELECT 
        p.Promotion_ID,
        p.Promo_Name,
        p.Start_Date,
        p.End_Date,
        UPPER(LTRIM(RTRIM(value))) AS Product_SKU
    FROM promotions p
    CROSS APPLY STRING_SPLIT(p.Eligible_SKUs, ''|'')
    WHERE LTRIM(RTRIM(value)) <> ''''  -- Exclude empty strings
),

-- Validate that products exist
with_validation AS (
    SELECT 
        ps.Promotion_ID,
        ps.Promo_Name,
        ps.Start_Date,
        ps.End_Date,
        ps.Product_SKU
    FROM parsed_skus ps
    WHERE EXISTS (
        SELECT 1 
        FROM "MetroRetailDB"."Silver"."erp_products_clean" pr 
        WHERE pr.Product_SKU = ps.Product_SKU 
            AND pr.Is_Current = 1
            AND pr.Is_Valid = 1
    )
),

final AS (
    SELECT 
        Promotion_ID,
        Product_SKU,
        GETDATE() AS Created_TS
    FROM with_validation
)

SELECT * FROM final

/*
    Output Schema:
    - Promotion_ID: VARCHAR(50) - Natural key
    - Product_SKU: VARCHAR(50) - Natural key
    - Created_TS: DATETIME2
    
    Example Transformation:
    
    Input (mkt_promotions_clean):
    Promotion_ID | Eligible_SKUs
    PROMO001     | P0047|P0133|P0177
    PROMO002     | P0189
    PROMO003     | N/A
    
    Output (bridge_promotion_product):
    Promotion_ID | Product_SKU
    PROMO001     | P0047
    PROMO001     | P0133
    PROMO001     | P0177
    PROMO002     | P0189
    (no row for PROMO003 because Eligible_SKUs = ''N/A'')
    
    Usage Options:
    
    Option 1 - Direct Join in SQL:
    SELECT 
        p.Promo_Name,
        pr.Product_Name,
        pr.Category
    FROM Gold.bridge_promotion_product b
    INNER JOIN Gold.dim_promotion p ON b.Promotion_ID = p.Promotion_ID
    INNER JOIN Gold.dim_product pr ON b.Product_SKU = pr.Product_SKU AND pr.Is_Current = 1;
    
    Option 2 - Create view with surrogate keys:
    CREATE VIEW Gold.vw_bridge_promotion_product AS
    SELECT 
        p.Promotion_Key,
        pr.Product_Key,
        b.Created_TS
    FROM Gold.bridge_promotion_product b
    INNER JOIN Gold.dim_promotion p ON b.Promotion_ID = p.Promotion_ID
    INNER JOIN Gold.dim_product pr ON b.Product_SKU = pr.Product_SKU AND pr.Is_Current = 1;
    
    Option 3 - Power BI Relationships:
    bridge[Promotion_ID] → dim_promotion[Promotion_ID]
    bridge[Product_SKU] → dim_product[Product_SKU]
*/;
    ')

EXEC('
            SELECT * INTO "MetroRetailDB"."Gold"."bridge_promotion_product__dbt_tmp" FROM "MetroRetailDB"."Gold"."bridge_promotion_product__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS Gold.bridge_promotion_product__dbt_tmp__dbt_tmp_vw')



    
    use [MetroRetailDB];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'Gold_bridge_promotion_product__dbt_tmp_cci'
        AND object_id=object_id('Gold_bridge_promotion_product__dbt_tmp')
    )
    DROP index "Gold"."bridge_promotion_product__dbt_tmp".Gold_bridge_promotion_product__dbt_tmp_cci
    CREATE CLUSTERED COLUMNSTORE INDEX Gold_bridge_promotion_product__dbt_tmp_cci
    ON "Gold"."bridge_promotion_product__dbt_tmp"

   


  