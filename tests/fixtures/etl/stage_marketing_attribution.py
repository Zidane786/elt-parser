"""Deduplicate marketing attribution touchpoints on (order_id, campaign_id).

Source: ecommerce.raw_marketing_attribution
Target: ecommerce.staging_marketing_attribution
Owner: growth-engineering@example.com
Grain: one row per (order, campaign) — earliest touchpoint wins
Schedule: hourly
Dependencies: ingest_marketing_attribution
"""
from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.appName("stage_marketing_attribution").getOrCreate()

raw = spark.table("ecommerce.raw_marketing_attribution")

# Keep the earliest touchpoint per (order_id, campaign_id). row_number = 1
# wins; ties broken by attribution_id ascending.
w = Window.partitionBy("order_id", "campaign_id").orderBy(
    F.col("touchpoint_at").asc_nulls_last(),
    F.col("attribution_id").asc(),
)

deduped = (
    raw.withColumn("_rn", F.row_number().over(w))
       .filter(F.col("_rn") == 1)
       .drop("_rn")
       .select("attribution_id", "order_id", "campaign_id", "touchpoint_at")
)

(deduped
    .write
    .mode("overwrite")
    .saveAsTable("ecommerce.staging_marketing_attribution"))

spark.stop()
