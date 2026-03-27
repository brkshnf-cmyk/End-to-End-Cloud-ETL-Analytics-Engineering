-- ============================================================
-- Script Title: Silver ETL – Clean and Load Orders Data
--
-- Purpose:
--   This script cleans, standardizes, and validates order records from
--   the Bronze layer before loading them into the curated Silver layer.
--   It corrects inconsistent status values, enforces proper datetime
--   types, and removes records with invalid chronological order flows.
--
-- Business Context:
--   Clean order data is essential for accurate delivery analytics.
--   Ensuring valid order timelines prevents misleading insights in the
--   Gold FactOrderItems and FactPayments tables.
--
-- Technical Summary:
--   1. TRUNCATE the Silver table to ensure a clean reload.
--   2. Standardize order_status values into consistent business labels.
--   3. Convert all timestamp fields to DATETIME.
--   4. Validate chronological order flow:
--        - purchase_timestamp <= approved_at
--        - approved_at <= delivered_carrier_date
--        - delivered_carrier_date <= delivered_customer_date
--        - delivered_customer_date <= estimated_delivery_date
--   5. Remove invalid records using is_invalid_order_flow flag.
--
-- Notes:
--   - The Olist dataset contains known timestamp inconsistencies.
--     This script filters them out to maintain analytical integrity.
--   - Status normalization ensures consistent reporting categories.
-- ============================================================


---------------------------------------------------------------
-- Truncate Before Load
---------------------------------------------------------------
TRUNCATE TABLE silver.orders;


---------------------------------------------------------------
-- Clean, Standardize, and Validate Order Records
---------------------------------------------------------------

WITH Date_Corr AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(customer_id) AS customer_id,

        -- Normalize order status
        CASE 
            WHEN order_status = 'delivered' THEN 'Delivered'
            WHEN order_status = 'invoiced' THEN 'Invoiced'
            WHEN order_status = 'shipped' THEN 'Shipped'
            WHEN order_status = 'processing' THEN 'Processing'
            WHEN order_status = 'canceled' THEN 'Canceled'
            ELSE 'Unavailable'
        END AS order_status,

        -- Convert timestamps
        TRY_CAST(order_purchase_timestamp AS DATETIME) AS order_purchase_timestamp,
        TRY_CAST(order_approved_at AS DATETIME) AS order_approved_at,
        TRY_CAST(order_delivered_carrier_date AS DATETIME) AS order_delivered_carrier_date,
        TRY_CAST(order_delivered_customer_date AS DATETIME) AS order_delivered_customer_date,
        TRY_CAST(order_estimated_delivery_date AS DATETIME) AS order_estimated_delivery_date,

        -- Validation: purchase must be before approval
        CASE 
            WHEN order_purchase_timestamp IS NOT NULL 
                 AND order_approved_at IS NOT NULL 
                 AND order_purchase_timestamp > order_approved_at THEN 1 
            ELSE 0 
        END AS invalid_purchase_vs_approval,

        -- Validation: approval must be before carrier pickup
        CASE 
            WHEN order_approved_at IS NOT NULL 
                 AND order_delivered_carrier_date IS NOT NULL 
                 AND order_approved_at > order_delivered_carrier_date THEN 1 
            ELSE 0 
        END AS invalid_approval_vs_carrier,

        -- Validation: carrier pickup must be before customer delivery
        CASE 
            WHEN order_delivered_carrier_date IS NOT NULL 
                 AND order_delivered_customer_date IS NOT NULL 
                 AND order_delivered_carrier_date > order_delivered_customer_date THEN 1 
            ELSE 0 
        END AS invalid_carrier_vs_customer,

        -- Validation: customer delivery must be before estimated delivery
        CASE 
            WHEN order_delivered_customer_date IS NOT NULL 
                 AND order_estimated_delivery_date IS NOT NULL 
                 AND order_delivered_customer_date > order_estimated_delivery_date THEN 1 
            ELSE 0 
        END AS invalid_customer_vs_estimated,

        -- Combined invalid flow flag
        CASE 
            WHEN
                (order_purchase_timestamp IS NOT NULL AND order_approved_at IS NOT NULL AND order_purchase_timestamp > order_approved_at) OR
                (order_approved_at IS NOT NULL AND order_delivered_carrier_date IS NOT NULL AND order_approved_at > order_delivered_carrier_date) OR
                (order_delivered_carrier_date IS NOT NULL AND order_delivered_customer_date IS NOT NULL AND order_delivered_carrier_date > order_delivered_customer_date)
            THEN 1
            ELSE 0
        END AS is_invalid_order_flow

    FROM bronze.olist_orders_dataset
)

---------------------------------------------------------------
-- Load Valid Orders Into Silver Layer
---------------------------------------------------------------

INSERT INTO silver.orders (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
)
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
FROM Date_Corr
WHERE is_invalid_order_flow = 0;