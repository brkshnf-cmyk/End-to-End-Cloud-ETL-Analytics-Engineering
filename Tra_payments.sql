-- ============================================================
-- Script Title: Silver ETL – Clean and Load Order Payments Data
--
-- Purpose:
--   This script removes duplicate payment records, standardizes fields,
--   enforces correct data types, and loads a clean, deduplicated version
--   of order payment data into the Silver layer.
--
-- Business Context:
--   Payment data is critical for revenue analytics, installment behavior
--   analysis, fraud detection, and financial reporting. Ensuring that
--   each payment installment is represented once per order is essential
--   for accurate financial metrics and downstream fact table modeling.
--
-- Technical Summary:
--   1. TRUNCATE the Silver table to ensure a clean reload.
--   2. Remove perfect duplicates using DISTINCT.
--   3. Standardize and type‑cast all fields.
--   4. Deduplicate using ROW_NUMBER() over (order_id, payment_sequential)
--        - Keeps the record with the highest payment_value.
--   5. Insert only one clean record per payment installment.
--
-- Notes:
--   - DISTINCT removes exact duplicate rows from the Bronze layer.
--   - ROW_NUMBER() ensures only one record per (order_id, payment_sequential).
--   - This logic resolves issues where identical payment rows appear
--     multiple times in the raw dataset.
-- ============================================================


---------------------------------------------------------------
-- Truncate Before Load
---------------------------------------------------------------
TRUNCATE TABLE silver.order_payments;


---------------------------------------------------------------
-- Clean, Standardize, and Deduplicate Payment Records
---------------------------------------------------------------

WITH cleaned AS (
    SELECT DISTINCT
        TRIM(order_id) AS order_id,
        TRY_CAST(payment_sequential AS SMALLINT) AS payment_sequential,
        LOWER(TRIM(payment_type)) AS payment_type,
        TRY_CAST(payment_installments AS SMALLINT) AS payment_installments,
        TRY_CAST(payment_value AS FLOAT) AS payment_value
    FROM bronze.olist_order_payments_dataset
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, payment_sequential
            ORDER BY payment_value DESC, order_id
        ) AS rn
    FROM cleaned
)

---------------------------------------------------------------
-- Load Cleaned Payments Into Silver Layer
---------------------------------------------------------------

INSERT INTO silver.order_payments (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
)
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM deduped
WHERE rn = 1;