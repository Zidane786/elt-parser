"""Materialise fact_orders from staging_orders.

Source: ecommerce.staging_orders
Target: analytics_warehouse.fact_orders
Owner: data-platform-team@example.com
Grain: one row per order
Schedule: hourly
Dependencies: stage_orders
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("fact_orders").getOrCreate()

staging = spark.table("ecommerce.staging_orders")

# Defensive: drop rows whose customer_id is NULL. Those are an OLTP corruption
# bug being tracked by data-platform — they should never reach the warehouse.
clean = staging.filter(F.col("customer_id").isNotNull())

out_df = (
    clean.select(
        "order_id",
        "customer_id",
        "order_date",
        "status",
        "total_usd_cents",
        "region",
        F.col("original_currency").alias("currency"),
    )
)

(out_df
    .write
    .mode("overwrite")
    .partitionBy("order_date")
    .saveAsTable("analytics_warehouse.fact_orders"))

spark.stop()
