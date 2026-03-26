-- ============================================================
-- Script Title: Silver ETL – Clean and Load Order Reviews Data
--
-- Purpose:
--   This script cleans, validates, and standardizes customer review
--   records from the Bronze layer before loading them into the curated
--   Silver layer. It ensures that review scores are valid, text fields
--   are normalized, and duplicate reviews are removed.
--
-- Business Context:
--   Review data is essential for customer satisfaction analytics,
--   seller performance scoring, product quality monitoring, and
--   building the Gold FactReviews table. Clean review data improves
--   sentiment analysis and customer experience insights.
--
-- Technical Summary:
--   1. Cleans and standardizes fields:
--        - TRIM whitespace
--        - Replace empty comment fields with 'N/A'
--        - Validate review_score (must be between 1 and 5)
--        - Convert timestamps to DATETIME
--   2. Deduplicates reviews using ROW_NUMBER() over review_id.
--   3. Inserts only new, cleaned records into silver.order_reviews.
--   4. Ensures idempotency by preventing duplicate inserts on rerun.
--
-- Notes:
--   - Invalid review scores are set to NULL for downstream handling.
--   - Deduplication keeps the most recent review based on creation date.
-- ============================================================


---------------------------------------------------------------
-- Insert Cleaned & Deduplicated Data Into Silver Layer
---------------------------------------------------------------

WITH silver_ready AS (
    SELECT
        TRIM(review_id) AS review_id,
        TRIM(order_id) AS order_id,

        -- Validate rating (must be between 1 and 5)
        CASE 
            WHEN TRY_CAST(review_score AS SMALLINT) BETWEEN 1 AND 5 
            THEN TRY_CAST(review_score AS SMALLINT)
            ELSE NULL
        END AS review_score,

        -- Replace empty strings with 'N/A'
        COALESCE(NULLIF(TRIM(review_comment_title), ''), 'N/A') AS review_comment_title,
        COALESCE(NULLIF(TRIM(review_comment_message), ''), 'N/A') AS review_comment_message,

        -- Convert timestamps
        TRY_CAST(review_creation_date AS DATETIME) AS review_creation_date,
        TRY_CAST(review_answer_timestamp AS DATETIME) AS review_answer_timestamp,

        -- Deduplicate by review_id, keeping the most recent review
        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY TRY_CAST(review_creation_date AS DATETIME) DESC
        ) AS rn

    FROM bronze.olist_order_reviews_dataset
)

INSERT INTO silver.order_reviews (
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
)
SELECT 
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM silver_ready
WHERE rn = 1
  -- Prevent duplicates on rerun
  AND review_id NOT IN (
        SELECT review_id FROM silver.order_reviews
    );