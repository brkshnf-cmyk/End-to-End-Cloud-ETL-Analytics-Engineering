-- ============================================================
-- Script Title: Create Silver Layer Tables for Cleansed Olist Data
--
-- Purpose:
--   This script creates the Silver schema and all curated tables used
--   for cleaned, standardized, and query‑ready data. The Silver layer
--   serves as the refined version of the raw Bronze data, ensuring
--   consistency, corrected naming, and structural alignment for
--   downstream transformations.
--
-- Business Context:
--   The Silver layer provides analytics‑ready tables that remove raw
--   ingestion inconsistencies. This layer is used by the Gold
--   dimensional model (Fact and Dimension tables) to support business
--   reporting, dashboards, and advanced analytics.
--
-- Technical Summary:
--   1. Creates the 'silver' schema if it does not already exist.
--   2. Drops existing Silver tables to allow clean rebuilds.
--   3. Recreates curated tables with standardized names and structures.
--   4. Tables include:
--        - customers
--        - geolocation
--        - order_items
--        - order_payments
--        - order_reviews
--        - orders
--        - products
--        - sellers
--        
-- Notes:
--   - These tables are structurally identical to Bronze but intended
--     for cleaned and validated data.
--   - product_category_name_translation table is removed in this layer 
--   - Transformations (deduplication, type casting, standardization)
--     occur during the ETL load into Silver.
-- ============================================================

-- Create Silver Schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END;
GO

---------------------------------------------------------------
-- Customers Table
---------------------------------------------------------------
IF OBJECT_ID('silver.olist_customers_dataset', 'U') IS NOT NULL
    DROP TABLE silver.customers;

CREATE TABLE silver.customers
(
    customer_id VARCHAR(255) NOT NULL,
    customer_unique_id VARCHAR(255) NOT NULL,
    customer_zip_code_prefix INT NULL,
    customer_city VARCHAR(50) NULL,
    customer_state VARCHAR(50) NULL
);

---------------------------------------------------------------
-- Geolocation Table
---------------------------------------------------------------
IF OBJECT_ID('silver.olist_geolocation_dataset', 'U') IS NOT NULL
    DROP TABLE silver.geolocation;

CREATE TABLE silver.geolocation
(
    geolocation_zip_code_prefix INT NULL,
    geolocation_lat FLOAT NULL,
    geolocation_lng FLOAT NULL,
    geolocation_city VARCHAR(50) NULL,
    geolocation_state VARCHAR(50) NULL
);

---------------------------------------------------------------
-- Order Items Table
---------------------------------------------------------------
IF OBJECT_ID('silver.olist_order_items_dataset', 'U') IS NOT NULL
    DROP TABLE silver.order_items;

CREATE TABLE silver.order_items
(
    order_id VARCHAR(255) NOT NULL,
    order_item_id VARCHAR(255) NOT NULL,
    product_id VARCHAR(255) NULL,
    seller_id VARCHAR(255) NULL,
    shipping_limit_date DATETIME NULL,
    price DECIMAL(10,5) NULL,
    freight_value DECIMAL(10,5) NULL
);

---------------------------------------------------------------
-- Payments Table
---------------------------------------------------------------
IF OBJECT_ID('silver.olist_order_payments_dataset', 'U') IS NOT NULL
    DROP TABLE silver.order_payments;

CREATE TABLE silver.order_payments
(
    order_id VARCHAR(255) NOT NULL,
    payment_sequential SMALLINT NULL,
    payment_type VARCHAR(50) NULL,
    payment_installments SMALLINT NULL,
    payment_value DECIMAL NULL
);

---------------------------------------------------------------
-- Reviews Table
---------------------------------------------------------------
IF OBJECT_ID('silver.olist_order_reviews_dataset', 'U') IS NOT NULL
    DROP TABLE silver.order_reviews;

CREATE TABLE silver.order_reviews
(
    review_id VARCHAR(255) NOT NULL,
    order_id VARCHAR(255) NULL,
    review_score SMALLINT NULL,
    review_comment_title VARCHAR(255) NULL,
    review_comment_message VARCHAR(1027) NULL,
    review_creation_date DATETIME NULL,
    review_answer_timestamp DATETIME NULL
);

---------------------------------------------------------------
-- Orders Table
---------------------------------------------------------------
IF OBJECT_ID('silver.olist_orders_dataset', 'U') IS NOT NULL
    DROP TABLE silver.orders;

CREATE TABLE silver.orders
(
    order_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255) NULL,
    order_status VARCHAR(50) NULL,
    order_purchase_timestamp DATETIME NULL,
    order_approved_at DATETIME NULL,
    order_dalivered_carrier_date DATETIME NULL,
    order_delivered_customer_date DATETIME NULL,
    order_estimated_delivery_date DATETIME NULL
);

---------------------------------------------------------------
-- Products Table
---------------------------------------------------------------
IF OBJECT_ID('silver.olist_products_dataset', 'U') IS NOT NULL
    DROP TABLE silver.products;

CREATE TABLE silver.products
(
    product_id VARCHAR(255) NOT NULL,
    product_catagory_name VARCHAR(255) NULL,
    product_name_lenght INT NULL,
    product_description_lenght INT NULL,
    product_photos_qty INT NULL,
    product_weight_g INT NULL,
    product_length_cm INT NULL,
    product_height_cm INT NULL,
    product_width_cm INT NULL
);

---------------------------------------------------------------
-- Sellers Table
---------------------------------------------------------------
IF OBJECT_ID('silver.olist_sellers_dataset', 'U') IS NOT NULL
    DROP TABLE silver.sellers;

CREATE TABLE silver.sellers
(
    sellers_id VARCHAR(255) NOT NULL,
    seller_zip_code_prefix INT NULL,
    seller_city VARCHAR(50) NULL,
    seller_state VARCHAR(50)
);

