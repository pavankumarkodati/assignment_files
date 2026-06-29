-- ===========================================================================
-- 01_setup_and_snowpipe.sql
-- Snowflake serving layer: database, warehouses, external stage on the S3 gold
-- layer, and Snowpipe for continuous auto-ingestion (the streaming path's tail).
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Warehouses: separate compute for load vs BI so heavy dashboards never block
-- ingestion. Multi-cluster on the BI warehouse absorbs concurrency spikes.
-- ---------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WH_LOAD
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE IF NOT EXISTS WH_BI
  WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 120 AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 4 SCALING_POLICY = 'STANDARD';

CREATE DATABASE IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.RAW;     -- landing copies from S3
CREATE SCHEMA IF NOT EXISTS ANALYTICS.MART;    -- dimensional model (facts/dims)

-- ---------------------------------------------------------------------------
-- Storage integration + external stage pointing at the S3 gold layer written by
-- Glue/EMR. (STORAGE_AWS_ROLE_ARN trust is configured once on the AWS side.)
-- ---------------------------------------------------------------------------
CREATE STORAGE INTEGRATION IF NOT EXISTS s3_gold_int
  TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = 'S3' ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<acct>:role/snowflake-gold-reader'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-lakehouse/gold/');

CREATE FILE FORMAT IF NOT EXISTS ANALYTICS.RAW.parquet_fmt TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS ANALYTICS.RAW.gold_stage
  STORAGE_INTEGRATION = s3_gold_int
  URL = 's3://my-lakehouse/gold/'
  FILE_FORMAT = ANALYTICS.RAW.parquet_fmt;

-- ---------------------------------------------------------------------------
-- Landing tables mirror the gold Parquet output from Spark.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ANALYTICS.RAW.fact_orders_land (
  order_id STRING, cust_id STRING, prod_id INT, cat_id INT, root_category STRING,
  order_ts TIMESTAMP_NTZ, order_day DATE, quantity NUMBER(18,2), status STRING,
  is_revenue_status BOOLEAN, currency STRING, conversion NUMBER(18,8),
  price NUMBER(18,2), revenue_usd NUMBER(18,4)
);

CREATE TABLE IF NOT EXISTS ANALYTICS.RAW.dim_customer_land (
  customer_sk STRING, cust_id STRING, country STRING, region STRING, type STRING,
  zip STRING, is_active BOOLEAN, valid_from DATE, valid_to DATE, is_current BOOLEAN
);

-- ---------------------------------------------------------------------------
-- Snowpipe: auto-ingest as soon as Glue/EMR writes a new Parquet file to gold.
-- AUTO_INGEST wires S3 -> SNS/SQS -> Snowpipe; near-real-time with no warehouse.
-- This is what carries the *streaming* micro-batches the rest of the way in.
-- ---------------------------------------------------------------------------
CREATE PIPE IF NOT EXISTS ANALYTICS.RAW.fact_orders_pipe AUTO_INGEST = TRUE AS
  COPY INTO ANALYTICS.RAW.fact_orders_land
  FROM @ANALYTICS.RAW.gold_stage/fact_orders/
  FILE_FORMAT = (FORMAT_NAME = ANALYTICS.RAW.parquet_fmt)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE PIPE IF NOT EXISTS ANALYTICS.RAW.dim_customer_pipe AUTO_INGEST = TRUE AS
  COPY INTO ANALYTICS.RAW.dim_customer_land
  FROM @ANALYTICS.RAW.gold_stage/dim_customer/
  FILE_FORMAT = (FORMAT_NAME = ANALYTICS.RAW.parquet_fmt)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- After creation, run SHOW PIPES and put the notification_channel (SQS ARN) into
-- the S3 bucket event config so new objects trigger Snowpipe automatically.
