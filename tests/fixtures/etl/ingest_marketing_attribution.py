"""Pull marketing attribution from the ad-server export.

Source: s3://ad-server-exports/attribution/dt=YYYY-MM-DD/*.parquet
Target: ecommerce.raw_marketing_attribution
Owner: growth-engineering@example.com
Grain: one row per touchpoint
Schedule: hourly
Dependencies: (none — root ingest)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("ingest_marketing_attribution").getOrCreate()

SOURCE = "s3://ad-server-exports/attribution/"

raw = spark.read.parquet(SOURCE)

# The ad-server schema rotates a couple of column names every few months.
# Normalise here so downstream stays stable.
out_df = (
    raw
    .withColumnRenamed("touchTime", "touchpoint_at")
    .withColumnRenamed("orderRef", "order_id")
    .withColumnRenamed("campaignRef", "campaign_id")
    .select(
        F.monotonically_increasing_id().alias("attribution_id"),
        F.col("order_id").cast("bigint"),
        F.col("campaign_id").cast("bigint"),
        F.col("touchpoint_at").cast("timestamp"),
    )
    .dropDuplicates(["order_id", "campaign_id", "touchpoint_at"])
)

(out_df
    .write
    .mode("append")
    .saveAsTable("ecommerce.raw_marketing_attribution"))

spark.stop()
