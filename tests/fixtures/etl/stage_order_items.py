"""Compute line_total = quantity * unit_price_cents.

Source: ecommerce.raw_order_items
Target: ecommerce.staging_order_items
Owner: data-platform-team@example.com
Grain: one row per order line
Schedule: hourly
Dependencies: stage_orders (logically — items are joined to orders downstream)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("stage_order_items").getOrCreate()

items = spark.table("ecommerce.raw_order_items")

out_df = (
    items
    .withColumn(
        "line_total_cents",
        (F.col("quantity") * F.col("unit_price_cents")).cast("bigint"),
    )
    .select(
        "item_id", "order_id", "product_id",
        "quantity", "unit_price_cents", "line_total_cents",
    )
)

(out_df
    .write
    .mode("overwrite")
    .saveAsTable("ecommerce.staging_order_items"))

spark.stop()
