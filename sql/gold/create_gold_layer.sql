-- ============================================================
-- Script Title: Create Gold Layer (Dimensional Model) for Olist
--
-- Purpose:
--   This script creates the Gold schema and defines all dimension and
--   fact views used for analytics and reporting. It also builds the
--   physical DimDate table. The Gold layer represents the final,
--   analytics‑ready star schema built from curated Silver data.
--
-- Business Context:
--   The Gold layer powers BI dashboards, KPI reporting, and advanced
--   analytics. It organizes data into facts and dimensions, enabling fast,
--   intuitive analysis of customers, sellers, products, orders, payments,
--   and reviews.
--
-- Technical Summary:
--   1. Creates the Gold schema.
--   2. Builds dimension views:
--        - DimCustomer
--        - DimProducts
--        - DimSeller
--        - DimOrder (BRIDGE DIMENSION)
--        - DimDate (physical table)
--   3. Builds fact views:
--        - FactPayments
--        - FactReviews
--        - FactOrderItems
--   4. Generates surrogate date keys using YYYYMMDD integers.
-- ============================================================


---------------------------------------------------------------
-- Create Gold Schema
---------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END;
GO;


---------------------------------------------------------------
-- Dimension: Customer
---------------------------------------------------------------
CREATE OR ALTER VIEW gold.dim_customer AS
SELECT 
    customer_id,
    customer_unique_id,
    zip,
    city,
    state
FROM silver.customers;


---------------------------------------------------------------
-- BRIDGING DIMENSION: DimOrder
-- Grain: 1 row per order_id
-- Purpose: Connect FactOrderItems and FactPayments cleanly in Power BI
---------------------------------------------------------------
CREATE OR ALTER VIEW gold.dim_order AS
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    --Calculate Delivery Status
    CASE 
        WHEN DATEDIFF(DAY, order_delivered_customer_date, order_estimated_delivery_date) > 3 
            THEN 'Early_Delivery'
        WHEN DATEDIFF(DAY, order_delivered_customer_date, order_estimated_delivery_date) BETWEEN 0 AND 3 
            THEN 'Ontime_Delivery'
        WHEN DATEDIFF(DAY, order_delivered_customer_date, order_estimated_delivery_date) BETWEEN -3 AND -1 
            THEN 'Late_Delivery'
        WHEN DATEDIFF(DAY, order_delivered_customer_date, order_estimated_delivery_date) < -3 
            THEN 'Very_Late_Delivery'
        ELSE 'Not_Delivered'
    END AS delivery_status,

    -- Surrogate date keys
    CAST(CONVERT(CHAR(8), order_purchase_timestamp, 112) AS INT) AS purchase_date_id,
    CAST(CONVERT(CHAR(8), order_approved_at, 112) AS INT) AS approved_date_id,
    CAST(CONVERT(CHAR(8), order_delivered_carrier_date, 112) AS INT) AS delivered_carrier_date_id,
    CAST(CONVERT(CHAR(8), order_delivered_customer_date, 112) AS INT) AS delivered_customer_date_id,
    CAST(CONVERT(CHAR(8), order_estimated_delivery_date, 112) AS INT) AS estimated_delivery_date_id,

    --Calculate Delivery Factors
    DATEDIFF(day,order_purchase_timestamp,order_approved_at) AS Purchase_approval,
    DATEDIFF(day,order_approved_at,order_delivered_carrier_date) AS approval_carrier,
    DATEDIFF(day,order_delivered_carrier_date,order_delivered_customer_date) AS Carrier_Customer,
    DATEDIFF(day,order_purchase_timestamp,order_delivered_customer_date) AS Purchase_Customer

    

FROM silver.orders;


---------------------------------------------------------------
-- Fact: Payments
---------------------------------------------------------------
CREATE OR ALTER VIEW gold.fact_payments AS
SELECT
    p.order_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value,

    -- Surrogate date key
    CAST(CONVERT(CHAR(8), o.order_purchase_timestamp, 112) AS INT) AS payment_date_id,

    -- Raw timestamp
    o.order_purchase_timestamp AS payment_timestamp

FROM silver.order_payments p
INNER JOIN silver.orders o
    ON p.order_id = o.order_id;


---------------------------------------------------------------
-- Dimension: Products
---------------------------------------------------------------
CREATE OR ALTER VIEW gold.dim_products AS
SELECT 
    product_id,
    product_category_name,
    product_weight_g,
    (product_length_cm * product_height_cm * product_width_cm) AS product_size_volume,
    product_photos_qty,
    product_description_length AS product_description_length,
    product_name_length AS product_name_length,
    CASE
        WHEN product_description_length < 200
            THEN 'Very_Little_Description'
        WHEN product_description_length BETWEEN 200 AND 600
            THEN 'Little_Description'
        WHEN product_description_length BETWEEN 600 AND 1000
            THEN 'Well_Described' 
        WHEN product_description_length BETWEEN 1000 AND 2000
            THEN 'Very_Good_Description'
        WHEN product_description_length > 2000
            THEN 'Highly_Described'
    END AS Description_Level
FROM silver.products;


---------------------------------------------------------------
-- Fact: Reviews
---------------------------------------------------------------
CREATE OR ALTER VIEW gold.fact_reviews AS
SELECT
    r.review_id,
    r.order_id,
    r.review_score,

    -- Surrogate date keys
    CAST(CONVERT(CHAR(8), r.review_creation_date, 112) AS INT) AS review_creation_date_id,
    CAST(CONVERT(CHAR(8), r.review_answer_timestamp, 112) AS INT) AS review_answer_date_id,

    -- Raw timestamps
    r.review_creation_date,
    r.review_answer_timestamp,

    --Calculate review comment character length
    LEN(r.review_comment_title) AS title_length,
    LEN(r.review_comment_message) AS message_length

FROM silver.order_reviews r;


---------------------------------------------------------------
-- Dimension: Seller
---------------------------------------------------------------
CREATE OR ALTER VIEW gold.dim_seller AS
SELECT 
    seller_id,
    zip,
    city,
    state
FROM silver.sellers;


---------------------------------------------------------------
-- Dimension: Date (Physical Table)
---------------------------------------------------------------

IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL 
    DROP TABLE gold.dim_date;

CREATE TABLE gold.dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    year INT,
    quarter INT,
    day_of_week INT,
    is_weekend BIT
);

WITH date_range AS (
    SELECT CAST('2016-01-01' AS DATE) AS full_date
    UNION ALL
    SELECT DATEADD(DAY, 1, full_date)
    FROM date_range
    WHERE full_date < '2018-12-31'
)
INSERT INTO gold.dim_date
SELECT
    CAST(CONVERT(CHAR(8), full_date, 112) AS INT) AS date_id,
    full_date,
    DAY(full_date),
    MONTH(full_date),
    YEAR(full_date),
    DATEPART(QUARTER, full_date),
    DATEPART(WEEKDAY, full_date),
    CASE WHEN DATEPART(WEEKDAY, full_date) IN (1, 7) THEN 1 ELSE 0 END
FROM date_range
OPTION (MAXRECURSION 0);


---------------------------------------------------------------
-- Fact: Order Items
---------------------------------------------------------------
CREATE OR ALTER VIEW gold.fact_order_items AS
SELECT
    i.order_id AS item_order_id,
    i.order_item_id AS item_order_number,
    i.product_id AS item_product_id,
    i.seller_id AS item_seller_id,

    -- Surrogate key for shipping limit date
    CAST(CONVERT(CHAR(8), i.shipping_limit_date, 112) AS INT) AS shipping_limit_date_id,

    -- Measures
    i.price AS item_price,
    i.freight_value AS item_freight

FROM silver.order_items i;