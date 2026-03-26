-- ============================================================
-- Script Title: Create Bronze Layer Tables for Olist Raw Ingestion
--
-- Purpose:
--   This script creates the Bronze schema and all raw ingestion tables
--   for the Brazilian E‑Commerce (Olist) dataset. These tables store
--   unmodified, row‑level data exactly as received from the source CSVs.
--
-- Business Context:
--   The Bronze layer acts as the immutable landing zone for all raw data.
--   It ensures traceability, reproducibility, and auditability of the
--   analytics pipeline. All downstream Silver and Gold transformations
--   depend on these raw tables.
--
-- Technical Summary:
--   1. Creates the 'bronze' schema if it does not already exist.
--   2. Drops existing Bronze tables to allow clean re‑ingestion.
--   3. Recreates all raw tables with appropriate data types.
--   4. Tables include:
--        - Customers
--        - Geolocation
--        - Order Items
--        - Payments
--        - Reviews
--        - Orders
--        - Products
--        - Sellers
--        - Product Category Translations
--
-- Notes:
--   - These tables intentionally mirror the source CSV structure.
--   - No business rules, deduplication, or transformations occur here.
--   - All cleansing and modeling will occur in the Silver and Gold layers.
-- ============================================================

-- Create Bronze Schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END;
GO

---------------------------------------------------------------
-- Customers Table
---------------------------------------------------------------
IF OBJECT_ID('bronze.olist_customers_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_customers_dataset;

CREATE TABLE bronze.olist_customers_dataset
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
IF OBJECT_ID('bronze.olist_geolocation_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_geolocation_dataset;

CREATE TABLE bronze.olist_geolocation_dataset
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
IF OBJECT_ID('bronze.olist_order_items_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_order_items_dataset;

CREATE TABLE bronze.olist_order_items_dataset
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
IF OBJECT_ID('bronze.olist_order_payments_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_order_payments_dataset;

CREATE TABLE bronze.olist_order_payments_dataset
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
IF OBJECT_ID('bronze.olist_order_reviews_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_order_reviews_dataset;

CREATE TABLE bronze.olist_order_reviews_dataset
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
IF OBJECT_ID('bronze.olist_orders_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_orders_dataset;

CREATE TABLE bronze.olist_orders_dataset
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
IF OBJECT_ID('bronze.olist_products_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_products_dataset;

CREATE TABLE bronze.olist_products_dataset
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
IF OBJECT_ID('bronze.olist_sellers_dataset', 'U') IS NOT NULL
    DROP TABLE bronze.olist_sellers_dataset;

CREATE TABLE bronze.olist_sellers_dataset
(
    sellers_id VARCHAR(255) NOT NULL,
    seller_zip_code_prefix INT NULL,
    seller_city VARCHAR(50) NULL,
    seller_state VARCHAR(50)
);

---------------------------------------------------------------
-- Product Category Translation Table
---------------------------------------------------------------
IF OBJECT_ID('bronze.product_category_name_translation', 'U') IS NOT NULL
    DROP TABLE bronze.product_category_name_translation;

CREATE TABLE bronze.product_category_name_translation
(
    product_category_name VARCHAR(255),
    product_category_name_english VARCHAR(255)
);
GO