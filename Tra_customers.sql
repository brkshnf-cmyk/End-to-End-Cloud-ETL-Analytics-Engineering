-- ============================================================
-- Script Title: Silver ETL – Clean and Load Customer Data
--
-- Purpose:
--   This script performs data quality validation, cleansing, and
--   standardization of customer records from the Bronze layer before
--   loading them into the curated Silver layer. It ensures that customer
--   data is consistent, deduplicated, and analytics‑ready.
--
-- Business Context:
--   Clean customer data is essential for accurate reporting on customer
--   behavior, geographic segmentation, order patterns, and retention
--   analytics. The Silver layer provides a trusted foundation for the
--   Gold dimensional model (DimCustomer).
--
-- Technical Summary:
--   1. Performs data quality checks:
--        - Null customer IDs
--        - Invalid ZIP codes
--        - Incorrect state abbreviations
--   2. Cleans and standardizes fields:
--        - TRIM whitespace
--        - Normalize city/state casing
--        - Convert ZIP codes to INT
--   3. Deduplicates customers using ROW_NUMBER()
--   4. Inserts only new, cleaned records into silver.customers
--
-- Notes:
--   - Deduplication is based on customer_id.
--   - State codes are standardized to uppercase (e.g., 'SP', 'RJ').
--   - City names are standardized to lowercase for consistency.
-- ============================================================


---------------------------------------------------------------
-- Data Quality Checks
---------------------------------------------------------------

-- Null customer_id check
SELECT COUNT(*) AS null_customer_id_count
FROM bronze.olist_customers_dataset
WHERE TRIM(customer_id) IS NULL;

-- Invalid ZIP codes
SELECT *
FROM bronze.olist_customers_dataset
WHERE TRY_CAST(customer_zip_code_prefix AS INT) IS NULL;

-- State code length issues
SELECT *
FROM bronze.olist_customers_dataset
WHERE LEN(TRIM(customer_state)) <> 2;


---------------------------------------------------------------
-- Insert Cleaned & Deduplicated Data Into Silver Layer
---------------------------------------------------------------

WITH silver_ready AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(customer_unique_id) AS customer_unique_id,
        TRY_CAST(customer_zip_code_prefix AS INT) AS zip,
        LOWER(TRIM(customer_city)) AS city,
        UPPER(TRIM(customer_state)) AS [state],
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY customer_unique_id
        ) AS rn
    FROM bronze.olist_customers_dataset
)

INSERT INTO silver.customers (
    customer_id,
    customer_unique_id,
    zip,
    city,
    [state]
)
SELECT
    s.customer_id,
    s.customer_unique_id,
    s.zip,
    s.city,
    s.[state]
FROM silver_ready s
WHERE rn = 1
  AND NOT EXISTS (
        SELECT 1
        FROM silver.customers t
        WHERE t.customer_id = s.customer_id
    );