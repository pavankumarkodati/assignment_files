-- ===========================================================================
-- 03_business_metrics.sql
-- The five analytics questions from the assignment, against the MART star schema.
-- Revenue counts only realized statuses (paid, shipped); created/cancelled are
-- excluded via is_revenue_status. "Sweet" uses root_category (rolls up cat 1/2/3).
-- ===========================================================================
USE DATABASE ANALYTICS;
USE WAREHOUSE WH_BI;

-- ---------------------------------------------------------------------------
-- (a) Daily active users by region.
--     "Active user" = a customer who placed an order that day. Region comes from
--     the SCD2 customer version in effect on the order day.
-- ---------------------------------------------------------------------------
SELECT
    f.order_day,
    c.region,
    COUNT(DISTINCT f.cust_id) AS daily_active_users
FROM MART.FACT_ORDERS f
JOIN MART.DIM_CUSTOMER c
  ON  f.cust_id = c.cust_id
  AND f.order_day BETWEEN c.valid_from AND c.valid_to     -- point-in-time SCD2 join
GROUP BY f.order_day, c.region
ORDER BY f.order_day, c.region;


-- ---------------------------------------------------------------------------
-- (b) Revenue with conversion and order count, and quantity for the "Sweet"
--     category. Conversion rate = realized orders / all orders touching Sweet.
-- ---------------------------------------------------------------------------
SELECT
    SUM(CASE WHEN is_revenue_status THEN revenue_usd END)               AS revenue_usd,
    COUNT(DISTINCT CASE WHEN is_revenue_status THEN order_id END)       AS realized_orders,
    COUNT(DISTINCT order_id)                                            AS total_orders,
    ROUND(COUNT(DISTINCT CASE WHEN is_revenue_status THEN order_id END)
          / NULLIF(COUNT(DISTINCT order_id), 0), 4)                     AS conversion_rate,
    SUM(CASE WHEN is_revenue_status THEN quantity END)                  AS quantity_sold
FROM MART.FACT_ORDERS
WHERE root_category = 'Sweet';


-- ---------------------------------------------------------------------------
-- (c) Top 3 products by revenue, within each region. QUALIFY ranks per region.
-- ---------------------------------------------------------------------------
SELECT region, prod_name, revenue_usd, rnk
FROM (
    SELECT
        c.region,
        p.prod_name,
        SUM(f.revenue_usd) AS revenue_usd,
        RANK() OVER (PARTITION BY c.region ORDER BY SUM(f.revenue_usd) DESC) AS rnk
    FROM MART.FACT_ORDERS f
    JOIN MART.DIM_PRODUCT  p ON f.prod_id = p.prod_id
    JOIN MART.DIM_CUSTOMER c
      ON f.cust_id = c.cust_id
     AND f.order_day BETWEEN c.valid_from AND c.valid_to
    WHERE f.is_revenue_status
    GROUP BY c.region, p.prod_name
)
WHERE rnk <= 3
ORDER BY region, rnk;


-- ---------------------------------------------------------------------------
-- (d) Customer lifetime value proxy: total realized revenue per customer,
--     split by whether the customer is currently active or inactive.
-- ---------------------------------------------------------------------------
SELECT
    f.cust_id,
    MAX(CASE WHEN c.is_current THEN c.is_active END) AS currently_active,
    SUM(CASE WHEN f.is_revenue_status THEN f.revenue_usd END) AS lifetime_value_usd,
    COUNT(DISTINCT CASE WHEN f.is_revenue_status THEN f.order_id END) AS realized_orders
FROM MART.FACT_ORDERS f
JOIN MART.DIM_CUSTOMER c ON f.cust_id = c.cust_id
GROUP BY f.cust_id
ORDER BY lifetime_value_usd DESC NULLS LAST;


-- ---------------------------------------------------------------------------
-- (e) Find duplicate orders and faulty transactions.
--     Duplicates: same (order_id, prod_id, order_ts, quantity, status) > 1 time
--     in the RAW landing (the silver job dedups these; this surfaces what it caught).
--     Faulty: everything the silver job quarantined (orphan cust, fractional qty…).
-- ---------------------------------------------------------------------------
-- Duplicates still visible in the raw landing table:
SELECT order_id, prod_id, order_ts, quantity, status, COUNT(*) AS dup_count
FROM RAW.fact_orders_land
GROUP BY order_id, prod_id, order_ts, quantity, status
HAVING COUNT(*) > 1
ORDER BY dup_count DESC;

-- Faulty transactions (quarantined upstream, surfaced here for BI/audit):
-- (orders_quarantine is loaded into RAW alongside the facts)
SELECT dq_reason, COUNT(*) AS faulty_count
FROM RAW.orders_quarantine
GROUP BY dq_reason
ORDER BY faulty_count DESC;
