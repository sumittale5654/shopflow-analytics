-- ============================================================
-- ShopFlow Analytics — Snowflake Reporting Queries
-- Run these in Snowflake Worksheets after the pipeline loads data.
-- ============================================================

USE DATABASE SHOPFLOW_DWH;
USE SCHEMA MARTS;
USE WAREHOUSE SHOPFLOW_WH;


-- ── 1. Monthly revenue trend ─────────────────────────────────────────────────
SELECT
    DATE_TRUNC('month', SALE_DATE)         AS month,
    ROUND(SUM(GROSS_REVENUE), 2)           AS total_revenue,
    ROUND(SUM(GROSS_PROFIT), 2)            AS total_profit,
    ROUND(AVG(MARGIN_PCT), 2)              AS avg_margin_pct,
    SUM(NUM_ORDERS)                        AS total_orders,
    SUM(UNITS_SOLD)                        AS units_sold
FROM DAILY_SALES
GROUP BY 1
ORDER BY 1 DESC;


-- ── 2. Revenue by product category (last 30 days) ────────────────────────────
SELECT
    CATEGORY,
    ROUND(SUM(GROSS_REVENUE), 2)           AS revenue,
    ROUND(SUM(GROSS_PROFIT), 2)            AS profit,
    ROUND(AVG(MARGIN_PCT), 2)              AS avg_margin_pct,
    SUM(NUM_ORDERS)                        AS orders
FROM DAILY_SALES
WHERE SALE_DATE >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY CATEGORY
ORDER BY revenue DESC;


-- ── 3. Top 20 products by revenue ────────────────────────────────────────────
SELECT
    REVENUE_RANK,
    PRODUCT_NAME,
    CATEGORY,
    BRAND,
    TOTAL_UNITS_SOLD,
    ROUND(TOTAL_REVENUE, 2)                AS total_revenue,
    ROUND(AVG_SELLING_PRICE, 2)            AS avg_price,
    ROUND(AVG_DISCOUNT_PCT, 2)             AS avg_discount_pct,
    UNIQUE_CUSTOMERS
FROM PRODUCT_PERFORMANCE
ORDER BY REVENUE_RANK
LIMIT 20;


-- ── 4. Customer LTV distribution by segment ───────────────────────────────────
SELECT
    LTV_SEGMENT,
    COUNT(*)                               AS customer_count,
    ROUND(AVG(LIFETIME_VALUE), 2)          AS avg_ltv,
    ROUND(SUM(LIFETIME_VALUE), 2)          AS total_ltv,
    ROUND(AVG(TOTAL_ORDERS), 2)            AS avg_orders,
    ROUND(AVG(AVG_ORDER_VALUE), 2)         AS avg_order_value
FROM CUSTOMER_LTV
GROUP BY LTV_SEGMENT
ORDER BY avg_ltv DESC;


-- ── 5. Customer tier performance ─────────────────────────────────────────────
SELECT
    CUSTOMER_TIER,
    COUNT(*)                               AS customers,
    ROUND(AVG(LIFETIME_VALUE), 2)          AS avg_ltv,
    ROUND(AVG(TOTAL_ORDERS), 1)            AS avg_orders
FROM CUSTOMER_LTV
GROUP BY CUSTOMER_TIER
ORDER BY avg_ltv DESC;


-- ── 6. Day-of-week revenue pattern ───────────────────────────────────────────
SELECT
    DAYNAME(SALE_DATE)                     AS day_name,
    DAYOFWEEK(SALE_DATE)                   AS dow_num,
    ROUND(AVG(GROSS_REVENUE), 2)           AS avg_daily_revenue,
    ROUND(AVG(NUM_ORDERS), 1)              AS avg_orders
FROM DAILY_SALES
GROUP BY 1, 2
ORDER BY 2;


-- ── 7. VIP customers ranked by LTV ───────────────────────────────────────────
SELECT
    FULL_NAME,
    EMAIL,
    CUSTOMER_TIER,
    COUNTRY,
    TOTAL_ORDERS,
    ROUND(LIFETIME_VALUE, 2)               AS lifetime_value,
    ROUND(AVG_ORDER_VALUE, 2)              AS avg_order_value,
    FIRST_ORDER_DATE::DATE                 AS first_order,
    LAST_ORDER_DATE::DATE                  AS last_order
FROM CUSTOMER_LTV
WHERE LTV_SEGMENT = 'VIP'
ORDER BY LIFETIME_VALUE DESC
LIMIT 50;


-- ── 8. Category margin comparison ────────────────────────────────────────────
SELECT
    CATEGORY,
    ROUND(SUM(GROSS_REVENUE), 2)           AS total_revenue,
    ROUND(SUM(GROSS_PROFIT), 2)            AS total_profit,
    ROUND(SUM(GROSS_PROFIT) / NULLIF(SUM(GROSS_REVENUE), 0) * 100, 2) AS overall_margin_pct
FROM DAILY_SALES
GROUP BY CATEGORY
ORDER BY overall_margin_pct DESC;
