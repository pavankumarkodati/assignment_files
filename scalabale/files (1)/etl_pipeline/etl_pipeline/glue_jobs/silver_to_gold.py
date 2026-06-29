"""
Glue ETL Job: silver_to_gold
=============================
Promotes conformed Silver Iceberg tables to the Gold serving layer as Parquet
(the layout Snowpipe auto-ingests). Builds dim_date and re-publishes the star
schema. Kept deliberately thin: all heavy cleansing already happened in
bronze_to_silver; this layer is about serving shape + partitioning.
"""

import sys
import logging

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","job":"silver_to_gold","msg":"%(message)s"}',
)
log = logging.getLogger("silver_to_gold")

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SILVER_DATABASE", "GOLD_BUCKET"])
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

SILVER = args["SILVER_DATABASE"]
GOLD = args["GOLD_BUCKET"].rstrip("/") + "/gold"


def read_silver(t):
    return spark.table(f"glue_catalog.{SILVER}.{t}")


fact = read_silver("fact_orders")
dim_customer = read_silver("dim_customer")
dim_product = read_silver("dim_product")
dim_category = read_silver("dim_category")

# Build dim_date from the actual order date range (generated once, cheap).
bounds = fact.agg(F.min("order_day").alias("lo"), F.max("order_day").alias("hi")).first()
dim_date = (
    spark.sql(
        f"SELECT explode(sequence(to_date('{bounds.lo}'), to_date('{bounds.hi}'), interval 1 day)) AS date_key"
    )
    .withColumn("year", F.year("date_key"))
    .withColumn("quarter", F.quarter("date_key"))
    .withColumn("month", F.month("date_key"))
    .withColumn("day", F.dayofmonth("date_key"))
    .withColumn("day_of_week", F.dayofweek("date_key"))
    .withColumn("is_weekend", F.dayofweek("date_key").isin(1, 7))
)


def write_gold(df, name, partition=None):
    path = f"{GOLD}/{name}/"
    w = df.write.mode("overwrite").format("parquet")
    if partition:
        w = w.partitionBy(partition)
    w.save(path)
    log.info(f"wrote gold table={name} path={path} partition={partition}")


write_gold(fact, "fact_orders", partition="order_day")
write_gold(dim_customer, "dim_customer")
write_gold(dim_product, "dim_product")
write_gold(dim_category, "dim_category")
write_gold(dim_date, "dim_date")

log.info("silver_to_gold complete")
job.commit()
