-- ============================================================
-- Script Title: Silver ETL – Clean and Load Sellers Data
--
-- Purpose:
--   This script cleans, standardizes, and deduplicates seller records
--   from the Bronze layer before loading them into the curated Silver
--   layer. It ensures that seller identifiers, ZIP codes, and location
--   attributes are properly formatted and analytics‑ready.
--
-- Business Context:
--   Seller data is essential for marketplace analytics, seller
--   performance scoring, logistics optimization, and building the
--   Gold DimSeller dimension. Clean seller attributes improve join
--   accuracy across orders, order items, and geolocation data.
--
-- Technical Summary:
--   1. Cleans and standardizes fields:
--        - TRIM whitespace
--        - Convert ZIP prefix to INT
--        - Normalize city/state casing
--   2. Deduplicates sellers using ROW_NUMBER() over seller_id.
--   3. Inserts only new, cleaned records into silver.sellers.
--
-- Notes:
--   - Deduplication ensures one row per seller_id.
--   - State codes are standardized to uppercase (e.g., 'SP', 'RJ').
-- ============================================================


---------------------------------------------------------------
-- Insert Cleaned & Deduplicated Data Into Silver Layer
---------------------------------------------------------------

WITH silver_ready AS (
    SELECT
        TRIM(seller_id) AS seller_id,
        TRY_CAST(seller_zip_code_prefix AS INT) AS zip,
        LOWER(TRIM(seller_city)) AS city,
        UPPER(TRIM(seller_state)) AS [state],

        ROW_NUMBER() OVER (
            PARTITION BY seller_id
            ORDER BY seller_id
        ) AS rn
    FROM bronze.olist_sellers_dataset
)

INSERT INTO silver.sellers (
    seller_id,
    zip,
    city,
    [state]
)
SELECT
    s.seller_id,
    s.zip,
    s.city,
    s.[state]
FROM silver_ready s
WHERE rn = 1
  AND NOT EXISTS (
        SELECT 1
        FROM silver.sellers t
        WHERE t.seller_id = s.seller_id
    );