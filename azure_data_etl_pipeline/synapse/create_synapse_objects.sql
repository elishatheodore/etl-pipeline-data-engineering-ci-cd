-- =================================================================
-- Azure Synapse Analytics Database Objects
-- Brazilian E-Commerce Data Warehouse
-- =================================================================

-- Create database if not exists
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'BrazilianEcommerceDW')
BEGIN
    CREATE DATABASE BrazilianEcommerceDW
    WITH (
        ENCRYPTION = ON,
        COLLATE = SQL_Latin1_General_CP1_CI_AS
    );
END
GO

-- Use the database
USE BrazilianEcommerceDW;
GO

-- =================================================================
-- Create Schema
-- =================================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
BEGIN
    EXEC('CREATE SCHEMA staging');
END
GO

-- =================================================================
-- Create External Data Sources
-- =================================================================

-- Create external data source for Azure Data Lake Storage
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'AzureDataLakeStorage')
BEGIN
    CREATE EXTERNAL DATA SOURCE AzureDataLakeStorage
    WITH (
        TYPE = HADOOP,
        LOCATION = 'abfss://gold@stbrazilianecommerce.dfs.core.windows.net',
        CREDENTIAL = [AzureDataLakeCredential]
    );
END
GO

-- =================================================================
-- Create External File Formats
-- =================================================================

-- Delta format
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'DeltaFormat')
BEGIN
    CREATE EXTERNAL FILE FORMAT DeltaFormat
    WITH (
        FORMAT_TYPE = DELTA,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );
END
GO

-- =================================================================
-- Create External Tables for Gold Layer
-- =================================================================

-- External table for dim_customers
IF OBJECT_ID('gold.dim_customers_external', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.dim_customers_external;
GO

CREATE EXTERNAL TABLE gold.dim_customers_external
(
    customer_key BIGINT,
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_zip_code_prefix_clean VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(2),
    customer_region VARCHAR(20),
    gold_ingestion_timestamp DATETIME2,
    gold_batch_id VARCHAR(50)
)
WITH (
    LOCATION = 'dim_customers/',
    DATA_SOURCE = AzureDataLakeStorage,
    FILE_FORMAT = DeltaFormat
);
GO

-- External table for dim_products
IF OBJECT_ID('gold.dim_products_external', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.dim_products_external;
GO

CREATE EXTERNAL TABLE gold.dim_products_external
(
    product_key BIGINT,
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g DECIMAL(10,2),
    product_length_cm DECIMAL(10,2),
    product_height_cm DECIMAL(10,2),
    product_width_cm DECIMAL(10,2),
    product_volume_cm3 DECIMAL(15,2),
    product_density_g_per_cm3 DECIMAL(10,4),
    gold_ingestion_timestamp DATETIME2,
    gold_batch_id VARCHAR(50)
)
WITH (
    LOCATION = 'dim_products/',
    DATA_SOURCE = AzureDataLakeStorage,
    FILE_FORMAT = DeltaFormat
);
GO

-- External table for dim_sellers
IF OBJECT_ID('gold.dim_sellers_external', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.dim_sellers_external;
GO

CREATE EXTERNAL TABLE gold.dim_sellers_external
(
    seller_key BIGINT,
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_zip_code_prefix_clean VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(2),
    seller_region VARCHAR(20),
    gold_ingestion_timestamp DATETIME2,
    gold_batch_id VARCHAR(50)
)
WITH (
    LOCATION = 'dim_sellers/',
    DATA_SOURCE = AzureDataLakeStorage,
    FILE_FORMAT = DeltaFormat
);
GO

-- External table for dim_date
IF OBJECT_ID('gold.dim_date_external', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.dim_date_external;
GO

CREATE EXTERNAL TABLE gold.dim_date_external
(
    date_key INT,
    date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day INT,
    day_of_week INT,
    day_name VARCHAR(20),
    day_of_year INT,
    week_of_year INT,
    is_weekend BIT,
    is_holiday BIT,
    gold_ingestion_timestamp DATETIME2,
    gold_batch_id VARCHAR(50)
)
WITH (
    LOCATION = 'dim_date/',
    DATA_SOURCE = AzureDataLakeStorage,
    FILE_FORMAT = DeltaFormat
);
GO

-- External table for dim_payment_type
IF OBJECT_ID('gold.dim_payment_type_external', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.dim_payment_type_external;
GO

CREATE EXTERNAL TABLE gold.dim_payment_type_external
(
    payment_type_key BIGINT,
    payment_type VARCHAR(50),
    payment_type_clean VARCHAR(50),
    gold_ingestion_timestamp DATETIME2,
    gold_batch_id VARCHAR(50)
)
WITH (
    LOCATION = 'dim_payment_type/',
    DATA_SOURCE = AzureDataLakeStorage,
    FILE_FORMAT = DeltaFormat
);
GO

-- External table for dim_review_sentiment
IF OBJECT_ID('gold.dim_review_sentiment_external', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.dim_review_sentiment_external;
GO

CREATE EXTERNAL TABLE gold.dim_review_sentiment_external
(
    review_sentiment_key BIGINT,
    review_score INT,
    review_sentiment VARCHAR(20),
    gold_ingestion_timestamp DATETIME2,
    gold_batch_id VARCHAR(50)
)
WITH (
    LOCATION = 'dim_review_sentiment/',
    DATA_SOURCE = AzureDataLakeStorage,
    FILE_FORMAT = DeltaFormat
);
GO

-- External table for fact_orders
IF OBJECT_ID('gold.fact_orders_external', 'U') IS NOT NULL
    DROP EXTERNAL TABLE gold.fact_orders_external;
GO

CREATE EXTERNAL TABLE gold.fact_orders_external
(
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    date_key INT,
    order_item_id INT,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    order_item_total_value DECIMAL(10,2),
    freight_percentage DECIMAL(5,4),
    total_payment_value DECIMAL(10,2),
    payment_methods_count INT,
    primary_payment_type VARCHAR(50),
    review_score INT,
    review_sentiment VARCHAR(20),
    order_status VARCHAR(50),
    delivery_status VARCHAR(50),
    order_delivery_days INT,
    order_estimated_delivery_days INT,
    order_delivery_delay_days INT,
    order_purchase_timestamp DATETIME2,
    order_approved_at DATETIME2,
    order_delivered_carrier_date DATETIME2,
    order_delivered_customer_date DATETIME2,
    order_estimated_delivery_date DATETIME2,
    gold_ingestion_timestamp DATETIME2,
    gold_batch_id VARCHAR(50)
)
WITH (
    LOCATION = 'fact_orders/',
    DATA_SOURCE = AzureDataLakeStorage,
    FILE_FORMAT = DeltaFormat
);
GO

-- =================================================================
-- Create Internal Tables (Materialized Views)
-- =================================================================

-- Materialized dim_customers table
IF OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL
    DROP TABLE gold.dim_customers;
GO

SELECT *
INTO gold.dim_customers
FROM gold.dim_customers_external;
GO

-- Create clustered columnstore index for performance
CREATE CLUSTERED COLUMNSTORE INDEX CCI_dim_customers ON gold.dim_customers;
GO

-- Materialized dim_products table
IF OBJECT_ID('gold.dim_products', 'U') IS NOT NULL
    DROP TABLE gold.dim_products;
GO

SELECT *
INTO gold.dim_products
FROM gold.dim_products_external;
GO

CREATE CLUSTERED COLUMNSTORE INDEX CCI_dim_products ON gold.dim_products;
GO

-- Materialized dim_sellers table
IF OBJECT_ID('gold.dim_sellers', 'U') IS NOT NULL
    DROP TABLE gold.dim_sellers;
GO

SELECT *
INTO gold.dim_sellers
FROM gold.dim_sellers_external;
GO

CREATE CLUSTERED COLUMNSTORE INDEX CCI_dim_sellers ON gold.dim_sellers;
GO

-- Materialized dim_date table
IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL
    DROP TABLE gold.dim_date;
GO

SELECT *
INTO gold.dim_date
FROM gold.dim_date_external;
GO

CREATE CLUSTERED COLUMNSTORE INDEX CCI_dim_date ON gold.dim_date;
GO

-- Materialized dim_payment_type table
IF OBJECT_ID('gold.dim_payment_type', 'U') IS NOT NULL
    DROP TABLE gold.dim_payment_type;
GO

SELECT *
INTO gold.dim_payment_type
FROM gold.dim_payment_type_external;
GO

CREATE CLUSTERED COLUMNSTORE INDEX CCI_dim_payment_type ON gold.dim_payment_type;
GO

-- Materialized dim_review_sentiment table
IF OBJECT_ID('gold.dim_review_sentiment', 'U') IS NOT NULL
    DROP TABLE gold.dim_review_sentiment;
GO

SELECT *
INTO gold.dim_review_sentiment
FROM gold.dim_review_sentiment_external;
GO

CREATE CLUSTERED COLUMNSTORE INDEX CCI_dim_review_sentiment ON gold.dim_review_sentiment;
GO

-- Materialized fact_orders table
IF OBJECT_ID('gold.fact_orders', 'U') IS NOT NULL
    DROP TABLE gold.fact_orders;
GO

SELECT *
INTO gold.fact_orders
FROM gold.fact_orders_external;
GO

CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_orders ON gold.fact_orders;
GO

-- =================================================================
-- Create Primary Keys and Foreign Keys
-- =================================================================

-- Primary Keys
ALTER TABLE gold.dim_customers ADD CONSTRAINT PK_dim_customers PRIMARY KEY (customer_key);
ALTER TABLE gold.dim_products ADD CONSTRAINT PK_dim_products PRIMARY KEY (product_key);
ALTER TABLE gold.dim_sellers ADD CONSTRAINT PK_dim_sellers PRIMARY KEY (seller_key);
ALTER TABLE gold.dim_date ADD CONSTRAINT PK_dim_date PRIMARY KEY (date_key);
ALTER TABLE gold.dim_payment_type ADD CONSTRAINT PK_dim_payment_type PRIMARY KEY (payment_type_key);
ALTER TABLE gold.dim_review_sentiment ADD CONSTRAINT PK_dim_review_sentiment PRIMARY KEY (review_sentiment_key);

-- =================================================================
-- Create Views for Analytics
-- =================================================================

-- Sales by date view
IF OBJECT_ID('gold.v_sales_by_date', 'V') IS NOT NULL
    DROP VIEW gold.v_sales_by_date;
GO

CREATE VIEW gold.v_sales_by_date AS
SELECT 
    d.date_key,
    d.date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_name,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fo.customer_id) AS unique_customers,
    SUM(fo.order_item_total_value) AS total_revenue,
    SUM(fo.price) AS total_product_value,
    SUM(fo.freight_value) AS total_freight_value,
    AVG(fo.order_item_total_value) AS avg_order_value,
    AVG(fo.review_score) AS avg_review_score
FROM gold.fact_orders fo
INNER JOIN gold.dim_date d ON fo.date_key = d.date_key
WHERE fo.order_status = 'delivered'
GROUP BY 
    d.date_key, d.date, d.year, d.quarter, d.month, d.month_name, d.day_name;
GO

-- Sales by product category view
IF OBJECT_ID('gold.v_sales_by_category', 'V') IS NOT NULL
    DROP VIEW gold.v_sales_by_category;
GO

CREATE VIEW gold.v_sales_by_category AS
SELECT 
    dp.product_category_name_english,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fo.customer_id) AS unique_customers,
    SUM(fo.order_item_total_value) AS total_revenue,
    AVG(fo.order_item_total_value) AS avg_order_value,
    AVG(fo.review_score) AS avg_review_score,
    COUNT(DISTINCT dp.product_id) AS unique_products
FROM gold.fact_orders fo
INNER JOIN gold.dim_products dp ON fo.product_id = dp.product_id
WHERE fo.order_status = 'delivered'
GROUP BY dp.product_category_name_english
ORDER BY total_revenue DESC;
GO

-- Sales by region view
IF OBJECT_ID('gold.v_sales_by_region', 'V') IS NOT NULL
    DROP VIEW gold.v_sales_by_region;
GO

CREATE VIEW gold.v_sales_by_region AS
SELECT 
    dc.customer_region,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    COUNT(DISTINCT fo.customer_id) AS unique_customers,
    SUM(fo.order_item_total_value) AS total_revenue,
    AVG(fo.order_item_total_value) AS avg_order_value,
    AVG(fo.review_score) AS avg_review_score
FROM gold.fact_orders fo
INNER JOIN gold.dim_customers dc ON fo.customer_id = dc.customer_id
WHERE fo.order_status = 'delivered'
GROUP BY dc.customer_region
ORDER BY total_revenue DESC;
GO

-- Delivery performance view
IF OBJECT_ID('gold.v_delivery_performance', 'V') IS NOT NULL
    DROP VIEW gold.v_delivery_performance;
GO

CREATE VIEW gold.v_delivery_performance AS
SELECT 
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(CASE WHEN fo.delivery_status = 'On Time' THEN 1 ELSE 0 END) AS on_time_orders,
    SUM(CASE WHEN fo.delivery_status LIKE 'Late%' THEN 1 ELSE 0 END) AS late_orders,
    AVG(fo.order_delivery_days) AS avg_delivery_days,
    AVG(fo.order_delivery_delay_days) AS avg_delivery_delay_days,
    CAST(SUM(CASE WHEN fo.delivery_status = 'On Time' THEN 1 ELSE 0 END) * 100.0 / COUNT(fo.order_id) AS DECIMAL(5,2)) AS on_time_percentage
FROM gold.fact_orders fo
INNER JOIN gold.dim_date d ON fo.date_key = d.date_key
WHERE fo.order_status = 'delivered'
GROUP BY d.year, d.quarter, d.month, d.month_name
ORDER BY d.year, d.quarter, d.month;
GO

-- =================================================================
-- Create Stored Procedures
-- =================================================================

-- Procedure to load gold data
IF OBJECT_ID('dbo.sp_load_gold_data', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_load_gold_data;
GO

CREATE PROCEDURE dbo.sp_load_gold_data
    @gold_path VARCHAR(500),
    @storage_account VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @start_time DATETIME2 = GETDATE();
    
    PRINT 'Starting gold data load at ' + CONVERT(VARCHAR, @start_time);
    
    BEGIN TRY
        -- Refresh external tables
        EXEC('SELECT * INTO #temp_customers FROM gold.dim_customers_external');
        EXEC('SELECT * INTO #temp_products FROM gold.dim_products_external');
        EXEC('SELECT * INTO #temp_sellers FROM gold.dim_sellers_external');
        EXEC('SELECT * INTO #temp_date FROM gold.dim_date_external');
        EXEC('SELECT * INTO #temp_payment_type FROM gold.dim_payment_type_external');
        EXEC('SELECT * INTO #temp_review_sentiment FROM gold.dim_review_sentiment_external');
        EXEC('SELECT * INTO #temp_fact_orders FROM gold.fact_orders_external');
        
        -- Update materialized tables
        TRUNCATE TABLE gold.dim_customers;
        INSERT INTO gold.dim_customers SELECT * FROM gold.dim_customers_external;
        
        TRUNCATE TABLE gold.dim_products;
        INSERT INTO gold.dim_products SELECT * FROM gold.dim_products_external;
        
        TRUNCATE TABLE gold.dim_sellers;
        INSERT INTO gold.dim_sellers SELECT * FROM gold.dim_sellers_external;
        
        TRUNCATE TABLE gold.dim_date;
        INSERT INTO gold.dim_date SELECT * FROM gold.dim_date_external;
        
        TRUNCATE TABLE gold.dim_payment_type;
        INSERT INTO gold.dim_payment_type SELECT * FROM gold.dim_payment_type_external;
        
        TRUNCATE TABLE gold.dim_review_sentiment;
        INSERT INTO gold.dim_review_sentiment SELECT * FROM gold.dim_review_sentiment_external;
        
        TRUNCATE TABLE gold.fact_orders;
        INSERT INTO gold.fact_orders SELECT * FROM gold.fact_orders_external;
        
        -- Update statistics
        UPDATE STATISTICS gold.dim_customers;
        UPDATE STATISTICS gold.dim_products;
        UPDATE STATISTICS gold.dim_sellers;
        UPDATE STATISTICS gold.dim_date;
        UPDATE STATISTICS gold.dim_payment_type;
        UPDATE STATISTICS gold.dim_review_sentiment;
        UPDATE STATISTICS gold.fact_orders;
        
        DECLARE @end_time DATETIME2 = GETDATE();
        DECLARE @duration_seconds INT = DATEDIFF(SECOND, @start_time, @end_time);
        
        PRINT 'Gold data load completed successfully in ' + CAST(@duration_seconds AS VARCHAR) + ' seconds';
        
        SELECT 
            'Success' AS status,
            @start_time AS start_time,
            @end_time AS end_time,
            @duration_seconds AS duration_seconds;
        
    END TRY
    BEGIN CATCH
        PRINT 'Error during gold data load: ' + ERROR_MESSAGE();
        
        SELECT 
            'Error' AS status,
            ERROR_NUMBER() AS error_number,
            ERROR_MESSAGE() AS error_message,
            @start_time AS start_time;
        
        THROW;
    END CATCH
END;
GO

-- Procedure to get data quality metrics
IF OBJECT_ID('dbo.sp_get_data_quality_metrics', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_get_data_quality_metrics;
GO

CREATE PROCEDURE dbo.sp_get_data_quality_metrics
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Orders data quality
    SELECT 
        'Orders' AS table_name,
        COUNT(*) AS total_records,
        COUNT(DISTINCT order_id) AS unique_orders,
        SUM(CASE WHEN order_id IS NULL OR order_id = '' THEN 1 ELSE 0 END) AS null_order_ids,
        SUM(CASE WHEN customer_id IS NULL OR customer_id = '' THEN 1 ELSE 0 END) AS null_customer_ids,
        SUM(CASE WHEN order_status IS NULL OR order_status = '' THEN 1 ELSE 0 END) AS null_status,
        AVG(CASE WHEN order_delivery_days IS NOT NULL THEN order_delivery_days END) AS avg_delivery_days
    FROM gold.fact_orders;
    
    -- Products data quality
    SELECT 
        'Products' AS table_name,
        COUNT(*) AS total_records,
        COUNT(DISTINCT product_id) AS unique_products,
        SUM(CASE WHEN product_id IS NULL OR product_id = '' THEN 1 ELSE 0 END) AS null_product_ids,
        SUM(CASE WHEN product_category_name_english IS NULL OR product_category_name_english = '' THEN 1 ELSE 0 END) AS null_categories,
        AVG(CASE WHEN product_weight_g IS NOT NULL AND product_weight_g > 0 THEN product_weight_g END) AS avg_weight
    FROM gold.dim_products;
    
    -- Customers data quality
    SELECT 
        'Customers' AS table_name,
        COUNT(*) AS total_records,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(CASE WHEN customer_id IS NULL OR customer_id = '' THEN 1 ELSE 0 END) AS null_customer_ids,
        SUM(CASE WHEN customer_city IS NULL OR customer_city = '' THEN 1 ELSE 0 END) AS null_cities,
        SUM(CASE WHEN customer_state IS NULL OR customer_state = '' THEN 1 ELSE 0 END) AS null_states
    FROM gold.dim_customers;
END;
GO

PRINT 'Brazilian E-Commerce Data Warehouse objects created successfully!';
