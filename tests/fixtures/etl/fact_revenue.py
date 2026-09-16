"""Join staging_orders x staging_order_items -> fact_revenue.

Source: ecommerce.staging_orders, ecommerce.staging_order_items
Target: analytics_warehouse.fact_revenue
Owner: data-platform-team@example.com
Grain: one row per order line
Schedule: hourly
Dependencies: stage_orders, stage_order_items

KNOWN BUG (TRACKED IN BACKLOG-1742):
  The join below uses an INNER join on order_id. This is wrong — staging_orders
  is the source-of-truth for revenue and items missing a matched order row
  should still surface as a $0 line (preserving partition coverage).
  The original spec called for a LEFT join, but the engineer typo'd it as
  INNER during a refactor. As a result, on days where staging_orders is
  delayed (e.g. ingest lag), fact_revenue silently drops rows.
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("fact_revenue").getOrCreate()

orders = spark.table("ecommerce.staging_orders")
items = spark.table("ecommerce.staging_order_items")

# BUG: should be "left" — typo'd as "inner" during a refactor in 2026-04.
joined = (
    items.alias("i")
    .join(orders.alias("o"), F.col("i.order_id") == F.col("o.order_id"), "inner")
    .select(
        F.col("i.item_id").alias("revenue_id"),
        F.col("i.order_id").alias("order_id"),
        F.col("i.product_id").alias("product_id"),
        F.col("o.order_date").alias("order_date"),
        F.col("i.quantity").alias("quantity"),
        F.col("i.line_total_cents").alias("line_total_usd_cents"),
        F.col("o.region").alias("region"),
        F.weekofyear(F.col("o.order_date")).alias("iso_week"),
    )
)

(joined
    .write
    .mode("overwrite")
    .partitionBy("order_date")
    .saveAsTable("analytics_warehouse.fact_revenue"))

spark.stop()
