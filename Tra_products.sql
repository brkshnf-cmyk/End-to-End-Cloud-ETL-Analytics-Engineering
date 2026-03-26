-- ============================================================
-- Script Title: Silver ETL – Clean and Load Products Data
--
-- Purpose:
--   This script cleans, standardizes, and enriches product records from
--   the Bronze layer before loading them into the curated Silver layer.
--   It joins product category names to their English translations,
--   enforces correct data types, and removes duplicate product entries.
--
-- Business Context:
--   Product data is essential for category‑level analytics, assortment
--   optimization, product performance reporting, and building the
--   Gold DimProduct dimension. Clean and enriched product attributes
--   improve downstream insights and reporting accuracy.
--
-- Technical Summary:
--   1. TRUNCATE the Silver table to ensure a clean reload.
--   2. Join product records with the translation table to enrich
--      category names.
--   3. Convert all numeric attributes to INT.
--   4. Deduplicate products using ROW_NUMBER() over product_id.
--   5. Insert only the first valid record per product_id.
--
-- Notes:
--   - Missing category translations are labeled as 'UNKNOWN'.
--   - The Olist dataset contains occasional duplicate product rows.
--     Deduplication ensures one row per product_id.
-- ============================================================


---------------------------------------------------------------
-- Truncate Before Load
---------------------------------------------------------------
TRUNCATE TABLE silver.products;


---------------------------------------------------------------
-- Clean, Standardize, and Enrich Product Records
---------------------------------------------------------------

WITH silver_ready AS (
    SELECT
        TRIM(a.product_id) AS product_id,

        -- Enrich category name using translation table
        COALESCE(b.product_category_name_english, 'UNKNOWN') AS product_category_name,

        -- Convert numeric attributes
        TRY_CAST(product_name_lenght AS INT) AS product_name_lenght,
        TRY_CAST(product_description_lenght AS INT) AS product_description_lenght,
        TRY_CAST(product_photos_qty AS INT) AS product_photos_qty,
        TRY_CAST(product_weight_g AS INT) AS product_weight_g,
        TRY_CAST(product_length_cm AS INT) AS product_length_cm,
        TRY_CAST(product_height_cm AS INT) AS product_height_cm,
        TRY_CAST(product_width_cm AS INT) AS product_width_cm,

        -- Deduplicate by product_id
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(a.product_id)
            ORDER BY TRIM(a.product_id)
        ) AS rn

    FROM bronze.olist_products_dataset a
    LEFT JOIN bronze.product_category_name_translation b
        ON a.product_category_name = b.product_category_name
)

---------------------------------------------------------------
-- Load Cleaned Products Into Silver Layer
---------------------------------------------------------------

INSERT INTO silver.products (
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM silver_ready
WHERE rn = 1;