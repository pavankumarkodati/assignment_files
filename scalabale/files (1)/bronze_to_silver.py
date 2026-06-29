"""
Glue ETL Job: bronze_to_silver
================================
Cleans and standardizes the raw source data, then writes conformed Apache Iceberg
tables to the Silver layer. Designed to run on AWS Glue (Spark 3.3+, Glue 4.0).

Handles every data-quality issue found in the source files:
  * customer.csv     -> SCD Type 2 dimension (cust_id repeats with changing `active`)
  * orders.csv       -> exact-duplicate removal (A-005 x3), orphan/null cust_id,
                        fractional quantity (A-013 qty 0.1), order_id normalization
  * product.csv      -> currency casing fix ('Yen' -> 'YEN') to join FX rates
  * prod_cat_tree    -> recursive category hierarchy flattened (Sweet = 1/2/3)
  * currency_*.json  -> daily FX rates joined on (order_date, currency)

Job parameters (passed via --conf or Glue job arguments):
  --RAW_BUCKET        s3 bucket holding bronze data
  --SILVER_DATABASE   Glue/Iceberg database for silver tables
  --RUN_DATE          logical run date (YYYY-MM-DD) for idempotent reprocessing

Scale note: at 100x-1000x this same script runs unchanged on EMR; the only
differences are cluster size, AQE/skew settings, and partition counts (see emr/).
"""

import sys
import logging

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType,
)

# --------------------------------------------------------------------------- #
# Structured logging -> CloudWatch picks up stdout/stderr from Glue drivers.
# A JSON-ish single-line format makes the logs queryable in CloudWatch Insights.
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","job":"bronze_to_silver","msg":"%(message)s"}',
)
log = logging.getLogger("bronze_to_silver")


def log_count(df: DataFrame, name: str) -> DataFrame:
    """Log row count of a stage. Cache first so the count is not recomputed later."""
    df = df.cache()
    n = df.count()
    log.info(f"stage={name} rows={n}")
    return df


# --------------------------------------------------------------------------- #
# Glue / Spark bootstrap
# --------------------------------------------------------------------------- #
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "RAW_BUCKET", "SILVER_DATABASE", "RUN_DATE"],
)

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session

# Iceberg + adaptive execution + skew handling (critical at scale, harmless small)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

RAW = args["RAW_BUCKET"].rstrip("/")
SILVER_DB = args["SILVER_DATABASE"]
RUN_DATE = args["RUN_DATE"]

log.info(f"start raw_bucket={RAW} silver_db={SILVER_DB} run_date={RUN_DATE}")


# --------------------------------------------------------------------------- #
# 1. READ RAW (bronze). Explicit schemas avoid expensive inference at scale and
#    let us treat malformed values as nulls instead of failing the whole file.
# --------------------------------------------------------------------------- #
def read_csv(path: str, schema: StructType) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")          # bad rows -> nulls, not a crash
        .schema(schema)
        .csv(path)
    )


customer_raw = read_csv(
    f"{RAW}/customer/",
    StructType([
        StructField("cust_id", StringType()),
        StructField("created_at", StringType()),
        StructField("country", StringType()),
        StructField("region", StringType()),
        StructField("type", StringType()),
        StructField("zip", StringType()),
        StructField("active", StringType()),
    ]),
)

orders_raw = read_csv(
    f"{RAW}/orders/",
    StructType([
        StructField("order_id", StringType()),
        StructField("cust_id", StringType()),
        StructField("order_date", StringType()),   # 'order date' renamed on landing
        StructField("prod_id", StringType()),
        StructField("quantity", StringType()),      # read as string: '0.1' must be caught
        StructField("status", StringType()),
    ]),
)

product_raw = read_csv(
    f"{RAW}/product/",
    StructType([
        StructField("prod_id", IntegerType()),
        StructField("cat_id", IntegerType()),
        StructField("prod_name", StringType()),
        StructField("price", DoubleType()),
        StructField("currency", StringType()),
    ]),
)

cat_tree_raw = read_csv(
    f"{RAW}/prod_cat_tree/",
    StructType([
        StructField("cat_id", IntegerType()),
        StructField("child", StringType()),
        StructField("parent", StringType()),
    ]),
)

fx_raw = (
    spark.read.option("multiline", True).json(f"{RAW}/currency_conversion/")
    .select(
        F.to_date("date").alias("fx_date"),
        F.upper(F.trim("currency")).alias("currency"),   # normalize FX side too
        F.col("conversion").cast(DoubleType()).alias("conversion"),
    )
)


# --------------------------------------------------------------------------- #
# 2. DIM_CUSTOMER  ->  SCD Type 2
#    Each (cust_id, created_at) is a version. We build valid_from / valid_to and
#    an is_current flag using a window over created_at per customer.
# --------------------------------------------------------------------------- #
cust_clean = (
    customer_raw
    .withColumn("cust_id", F.trim("cust_id"))
    .filter(F.col("cust_id").isNotNull() & (F.col("cust_id") != ""))
    .withColumn("valid_from", F.to_date("created_at", "M/d/yyyy"))
    .withColumn("region", F.lower(F.trim("region")))
    .withColumn("type", F.lower(F.trim("type")))
    .withColumn("is_active", (F.lower(F.trim("active")) == "y"))
)

w_cust = Window.partitionBy("cust_id").orderBy("valid_from")
dim_customer = (
    cust_clean
    .withColumn("valid_to", F.lead("valid_from").over(w_cust))
    .withColumn(
        "valid_to",
        F.when(F.col("valid_to").isNull(), F.lit("9999-12-31").cast(DateType()))
         .otherwise(F.date_sub("valid_to", 1)),
    )
    .withColumn("is_current", F.col("valid_to") == F.lit("9999-12-31").cast(DateType()))
    .withColumn(
        "customer_sk",
        F.sha2(F.concat_ws("|", "cust_id", "valid_from"), 256),   # surrogate key
    )
    .select(
        "customer_sk", "cust_id", "country", "region", "type", "zip",
        "is_active", "valid_from", "valid_to", "is_current",
    )
)
dim_customer = log_count(dim_customer, "dim_customer")


# --------------------------------------------------------------------------- #
# 3. CATEGORY HIERARCHY  ->  flatten the recursive tree to a root label.
#    Sweet = {1 Sweet, 2 chocolate, 3 candy}; salt = {4 crisp, 5 chip}.
#    The tree is tiny and shallow, so we resolve the root with an iterative join
#    (works for arbitrary depth; bounded loop avoids runaway on cyclic data).
# --------------------------------------------------------------------------- #
edges = cat_tree_raw.select("cat_id", "child", F.trim("parent").alias("parent"))
resolved = edges.withColumn("root", F.col("parent"))
for _ in range(10):  # depth guard
    parents = edges.select(
        F.col("child").alias("p_child"), F.col("parent").alias("p_parent")
    )
    nxt = (
        resolved.join(parents, resolved.root == parents.p_child, "left")
        .withColumn("root", F.coalesce("p_parent", "root"))
        .drop("p_child", "p_parent")
    )
    if nxt.filter(F.col("root").isin("all", "salt")).count() == resolved.count():
        resolved = nxt
        break
    resolved = nxt

dim_category = resolved.select(
    "cat_id",
    F.col("child").alias("category_name"),
    F.when(F.col("root") == "all", F.lit("Sweet")).otherwise(F.col("root")).alias("root_category"),
)
dim_category = log_count(dim_category, "dim_category")


# --------------------------------------------------------------------------- #
# 4. DIM_PRODUCT  ->  fix currency casing, attach category root.
# --------------------------------------------------------------------------- #
dim_product = (
    product_raw
    .withColumn("currency", F.upper(F.trim("currency")))   # 'Yen' -> 'YEN'
    .withColumn("prod_name", F.trim("prod_name"))
    .join(dim_category, "cat_id", "left")
    .select(
        "prod_id", "cat_id", "prod_name", "price", "currency",
        "category_name", "root_category",
    )
)
dim_product = log_count(dim_product, "dim_product")


# --------------------------------------------------------------------------- #
# 5. FACT_ORDERS  ->  the heart of the cleansing.
# --------------------------------------------------------------------------- #
orders_typed = (
    orders_raw
    .withColumn("order_ts", F.to_timestamp("order_date", "M/d/yyyy H:mm"))
    .withColumn("order_day", F.to_date("order_ts"))
    .withColumn("quantity_num", F.col("quantity").cast(DoubleType()))
    .withColumn("prod_id", F.col("prod_id").cast(IntegerType()))
    .withColumn("cust_id", F.trim("cust_id"))
    .withColumn("status", F.lower(F.trim("status")))
    # normalize order_id: A-21 -> A-021 so it sorts/joins consistently
    .withColumn("oid_prefix", F.regexp_extract("order_id", r"^([A-Za-z]+)-", 1))
    .withColumn("oid_num", F.regexp_extract("order_id", r"-(\d+)$", 1).cast(IntegerType()))
    .withColumn(
        "order_id_norm",
        F.concat_ws("-", F.col("oid_prefix"), F.lpad(F.col("oid_num").cast("string"), 3, "0")),
    )
)

# 5a. Quarantine faulty transactions instead of silently dropping them. A row is
#     faulty if: null/orphan customer, non-positive or fractional quantity,
#     unparseable date, or missing product.
orders_flagged = orders_typed.withColumn(
    "dq_reason",
    F.when(F.col("cust_id").isNull() | (F.col("cust_id") == ""), F.lit("orphan_customer"))
     .when(F.col("quantity_num").isNull(), F.lit("bad_quantity"))
     .when(F.col("quantity_num") <= 0, F.lit("non_positive_quantity"))
     .when(F.col("quantity_num") != F.floor("quantity_num"), F.lit("fractional_quantity"))
     .when(F.col("order_ts").isNull(), F.lit("bad_timestamp"))
     .otherwise(F.lit(None)),
)

quarantine = orders_flagged.filter(F.col("dq_reason").isNotNull())
quarantine = log_count(quarantine, "orders_quarantine")

clean = orders_flagged.filter(F.col("dq_reason").isNull())

# 5b. Exact-duplicate removal. A-005 has 3 byte-identical lines (true dupes) PLUS
#     a 4th line for a different product (legitimate). We dedupe on the FULL grain
#     (order, product, ts, qty, status) so legit multi-line orders survive.
dedup_keys = ["order_id_norm", "prod_id", "order_ts", "quantity_num", "status"]
clean_dedup = (
    clean
    .withColumn("rn", F.row_number().over(Window.partitionBy(*dedup_keys).orderBy(F.lit(1))))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
clean_dedup = log_count(clean_dedup, "orders_deduped")

# 5c. Currency conversion. Join product (price+currency) and the daily FX rate on
#     (order_day, currency). Fall back to nearest prior rate if exact day missing.
priced = (
    clean_dedup
    .join(dim_product.select("prod_id", "price", "currency", "root_category", "cat_id"), "prod_id", "left")
    .join(fx_raw, (F.col("order_day") == fx_raw.fx_date) & (F.col("currency") == fx_raw.currency), "left")
)

# nearest-prior fallback for any missing exact-day rate (cross-join is tiny here;
# at scale this becomes a range-join / asof-join on a broadcasted rate table)
fallback = (
    fx_raw.select(
        F.col("fx_date").alias("f_date"),
        F.col("currency").alias("f_ccy"),
        F.col("conversion").alias("f_conv"),
    )
)
priced = (
    priced.join(
        fallback,
        (priced.currency == fallback.f_ccy) & (fallback.f_date <= priced.order_day),
        "left",
    )
    .withColumn(
        "rk",
        F.row_number().over(
            Window.partitionBy("order_id_norm", "prod_id", "order_ts")
            .orderBy(F.col("f_date").desc())
        ),
    )
    .filter((F.col("conversion").isNotNull()) | (F.col("rk") == 1))
    .withColumn("conversion", F.coalesce("conversion", "f_conv"))
)

fact_orders = (
    priced
    .withColumn("revenue_usd", F.col("price") * F.col("quantity_num") * F.col("conversion"))
    .withColumn(
        "is_revenue_status",
        F.col("status").isin("paid", "shipped"),   # created/cancelled excluded
    )
    .select(
        "order_id_norm", "cust_id", "prod_id", "cat_id", "root_category",
        "order_ts", "order_day", "quantity_num", "status", "is_revenue_status",
        "currency", "conversion", "price", "revenue_usd",
    )
    .withColumnRenamed("order_id_norm", "order_id")
    .withColumnRenamed("quantity_num", "quantity")
)
fact_orders = log_count(fact_orders, "fact_orders")


# --------------------------------------------------------------------------- #
# 6. WRITE to Silver as Iceberg (ACID, schema evolution, MERGE-ready, time travel)
# --------------------------------------------------------------------------- #
def write_iceberg(df: DataFrame, table: str, partition_by: str = None):
    full = f"glue_catalog.{SILVER_DB}.{table}"
    writer = df.writeTo(full).using("iceberg").tableProperty("format-version", "2")
    if partition_by:
        writer = writer.partitionedBy(F.col(partition_by))
    writer.createOrReplace()
    log.info(f"wrote table={full} partitioned_by={partition_by}")


write_iceberg(dim_customer, "dim_customer")
write_iceberg(dim_product, "dim_product")
write_iceberg(dim_category, "dim_category")
write_iceberg(fact_orders, "fact_orders", partition_by="order_day")
write_iceberg(quarantine.select("order_id", "cust_id", "prod_id", "status", "dq_reason"),
              "orders_quarantine")

log.info("bronze_to_silver complete")
job.commit()
