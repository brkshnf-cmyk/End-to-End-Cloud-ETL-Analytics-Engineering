-- ============================================================
-- Script Title: Silver ETL – Clean and Load Order Items Data
--
-- Purpose:
--   This script cleans, standardizes, and deduplicates order item
--   records from the Bronze layer before loading them into the curated
--   Silver layer. It ensures that order item data is valid, typed
--   correctly, and free of duplicates.
--
-- Business Context:
--   Order items form the backbone of revenue, product performance,
--   logistics, and seller analytics. Clean order item data is essential
--   for building the FactOrderItems table in the Gold layer and for
--   accurate reporting on sales, freight costs, and fulfillment metrics.
--
-- Technical Summary:
--   1. Cleans and standardizes fields:
--        - TRIM whitespace
--        - Convert order_item_id to SMALLINT
--        - Convert price and freight_value to FLOAT and enforce positivity
--        - Convert shipping_limit_date to DATETIME
--   2. Deduplicates records using ROW_NUMBER() over (order_id, order_item_id)
--   3. Inserts only new, cleaned records into silver.order_items
--   4. Performs post‑load validation checks:
--        - Null checks
--        - Negative value checks
--
-- Notes:
--   - ABS() is used to correct negative price/freight values, which
--     occasionally appear due to source inconsistencies.
--   - Deduplication ensures one row per (order_id, order_item_id).
-- ============================================================


---------------------------------------------------------------
-- Insert Cleaned & Deduplicated Data Into Silver Layer
---------------------------------------------------------------

;WITH silver_ready AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRY_CAST(order_item_id AS SMALLINT) AS order_item_id,
        TRIM(product_id) AS product_id,
        TRIM(seller_id) AS seller_id,
        TRY_CAST(shipping_limit_date AS DATETIME) AS shipping_limit_date,
        ABS(TRY_CAST(price AS FLOAT)) AS price,
        ABS(TRY_CAST(freight_value AS FLOAT)) AS freight_value,

        ROW_NUMBER() OVER (
            PARTITION BY 
                TRIM(order_id), 
                TRY_CAST(order_item_id AS SMALLINT)
            ORDER BY shipping_limit_date DESC
        ) AS rn

    FROM bronze.olist_order_items_dataset
)

INSERT INTO silver.order_items (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
)
SELECT 
    s.order_id,
    s.order_item_id,
    s.product_id,
    s.seller_id,
    s.shipping_limit_date,
    s.price,
    s.freight_value
FROM silver_ready s
WHERE rn = 1
  AND NOT EXISTS (
        SELECT 1
        FROM silver.order_items o
        WHERE 
            o.order_id = s.order_id
            AND o.order_item_id = s.order_item_id
    );


---------------------------------------------------------------
-- Post‑Load Data Quality Checks
---------------------------------------------------------------

-- Null Check Summary
SELECT
    COUNT(*) AS total_rows,
    
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END) AS null_order_item_id,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller_id,
    SUM(CASE WHEN shipping_limit_date IS NULL THEN 1 ELSE 0 END) AS null_shipping_date,
    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_price,
    SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END) AS null_freight
FROM silver.order_items;


-- Negative Value Check
SELECT
    COUNT(*) AS total_rows,

    SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS negative_price,
    SUM(CASE WHEN freight_value < 0 THEN 1 ELSE 0 END) AS negative_freight_value
FROM silver.order_items;