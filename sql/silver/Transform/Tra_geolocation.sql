-- ============================================================
-- Script Title: Silver ETL – Clean and Load Geolocation Data
--
-- Purpose:
--   This script validates, cleans, and standardizes geolocation data
--   from the Bronze layer before loading it into the curated Silver
--   layer. It ensures that ZIP codes, coordinates, and geographic
--   attributes are properly typed and formatted for downstream use.
--
-- Business Context:
--   Accurate geolocation data is essential for delivery analysis,
--   regional performance metrics, logistics optimization, and
--   geographic segmentation. Clean coordinates and standardized
--   city/state fields improve join accuracy and reporting quality.
--
-- Technical Summary:
--   1. Performs data quality checks:
--        - Invalid ZIP codes
--        - Invalid latitude/longitude values
--        - Null or malformed city/state fields
--        - Incorrect state code lengths
--   2. Cleans and standardizes fields:
--        - TRIM whitespace
--        - Normalize city/state casing
--        - Enforce CHAR(2) state codes
--        - Convert ZIP, lat, lng to numeric types
--   3. Loads cleaned data directly into silver.geolocation
--
-- Notes:
--   - No deduplication is applied here because the Olist geolocation
--     dataset contains multiple rows per ZIP prefix by design.
--   - State codes are standardized to uppercase (e.g., 'SP', 'RJ').
-- ============================================================


---------------------------------------------------------------
-- Data Quality Checks
---------------------------------------------------------------

-- Invalid ZIP codes
SELECT *
FROM bronze.olist_geolocation_dataset
WHERE TRY_CAST(geolocation_zip_code_prefix AS INT) IS NULL;

-- Invalid latitude values
SELECT *
FROM bronze.olist_geolocation_dataset
WHERE TRY_CAST(geolocation_lat AS FLOAT) IS NULL;

-- Invalid longitude values
SELECT *
FROM bronze.olist_geolocation_dataset
WHERE TRY_CAST(geolocation_lng AS FLOAT) IS NULL;

-- Null city values
SELECT *
FROM bronze.olist_geolocation_dataset
WHERE TRIM(geolocation_city) IS NULL;

-- Null state values
SELECT *
FROM bronze.olist_geolocation_dataset
WHERE TRIM(geolocation_state) IS NULL;

-- Incorrect state code length
SELECT *
FROM bronze.olist_geolocation_dataset
WHERE LEN(TRIM(geolocation_state)) <> 2;


---------------------------------------------------------------
-- Insert Cleaned Data Into Silver Layer
---------------------------------------------------------------

WITH silver_ready AS (
    SELECT
        TRY_CAST(geolocation_zip_code_prefix AS INT) AS zip,
        TRY_CAST(geolocation_lat AS FLOAT) AS lat,
        TRY_CAST(geolocation_lng AS FLOAT) AS lng,
        LOWER(TRIM(geolocation_city)) AS city,
        UPPER(CAST(TRIM(geolocation_state) AS CHAR(2))) AS [state]
    FROM bronze.olist_geolocation_dataset
)

INSERT INTO silver.geolocation (
    zip,
    lat,
    lng,
    city,
    [state]
)
SELECT
    zip,
    lat,
    lng,
    city,
    [state]
FROM silver_ready;