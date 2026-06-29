-- ===========================================================================
-- 02_model_streams_tasks.sql
-- Dimensional star schema + incremental load via Streams (CDC) and Tasks.
-- Grain of fact_orders = one order line item.
-- ===========================================================================
USE DATABASE ANALYTICS;
USE WAREHOUSE WH_LOAD;

-- ---------------------------------------------------------------------------
-- MART star schema
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MART.DIM_DATE (
  date_key DATE PRIMARY KEY, year INT, quarter INT, month INT, day INT,
  day_of_week INT, is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS MART.DIM_CUSTOMER (
  customer_sk STRING PRIMARY KEY, cust_id STRING, country STRING, region STRING,
  type STRING, zip STRING, is_active BOOLEAN,
  valid_from DATE, valid_to DATE, is_current BOOLEAN
);

CREATE TABLE IF NOT EXISTS MART.DIM_PRODUCT (
  prod_id INT PRIMARY KEY, cat_id INT, prod_name STRING, price NUMBER(18,2),
  currency STRING, category_name STRING, root_category STRING
);

CREATE TABLE IF NOT EXISTS MART.FACT_ORDERS (
  order_id STRING, cust_id STRING, prod_id INT, cat_id INT, root_category STRING,
  order_ts TIMESTAMP_NTZ, order_day DATE, quantity NUMBER(18,2), status STRING,
  is_revenue_status BOOLEAN, currency STRING, conversion NUMBER(18,8),
  price NUMBER(18,2), revenue_usd NUMBER(18,4)
)
CLUSTER BY (order_day);   -- prune by date at scale; see scaling notes

-- ---------------------------------------------------------------------------
-- Streams capture only NEW rows arriving via Snowpipe, so Tasks process deltas
-- instead of rescanning the whole landing table each run (essential at 1000x).
-- ---------------------------------------------------------------------------
CREATE STREAM IF NOT EXISTS RAW.fact_orders_stream ON TABLE RAW.fact_orders_land;
CREATE STREAM IF NOT EXISTS RAW.dim_customer_stream ON TABLE RAW.dim_customer_land;

-- ---------------------------------------------------------------------------
-- TASK 1: incremental append of new order facts. Idempotent via anti-join on the
-- natural grain so re-delivered Snowpipe files don't double-count.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK MART.load_fact_orders
  WAREHOUSE = WH_LOAD
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.fact_orders_stream')
AS
  INSERT INTO MART.FACT_ORDERS
  SELECT s.order_id, s.cust_id, s.prod_id, s.cat_id, s.root_category,
         s.order_ts, s.order_day, s.quantity, s.status, s.is_revenue_status,
         s.currency, s.conversion, s.price, s.revenue_usd
  FROM RAW.fact_orders_stream s
  WHERE NOT EXISTS (
    SELECT 1 FROM MART.FACT_ORDERS f
    WHERE f.order_id = s.order_id AND f.prod_id = s.prod_id
      AND f.order_ts = s.order_ts AND f.quantity = s.quantity
      AND f.status = s.status
  );

-- ---------------------------------------------------------------------------
-- TASK 2: SCD Type 2 MERGE for the customer dimension. New versions close out the
-- prior current row and insert the new one. Driven by the CDC stream.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK MART.merge_dim_customer
  WAREHOUSE = WH_LOAD
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.dim_customer_stream')
AS
  MERGE INTO MART.DIM_CUSTOMER t
  USING (SELECT * FROM RAW.dim_customer_stream) s
  ON  t.cust_id = s.cust_id AND t.valid_from = s.valid_from
  WHEN MATCHED THEN UPDATE SET
        t.country = s.country, t.region = s.region, t.type = s.type,
        t.zip = s.zip, t.is_active = s.is_active,
        t.valid_to = s.valid_to, t.is_current = s.is_current
  WHEN NOT MATCHED THEN INSERT
        (customer_sk, cust_id, country, region, type, zip, is_active,
         valid_from, valid_to, is_current)
        VALUES (s.customer_sk, s.cust_id, s.country, s.region, s.type, s.zip,
                s.is_active, s.valid_from, s.valid_to, s.is_current);

-- Resume tasks (created suspended by default).
ALTER TASK MART.load_fact_orders RESUME;
ALTER TASK MART.merge_dim_customer RESUME;
