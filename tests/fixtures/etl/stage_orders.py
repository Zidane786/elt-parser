"""Convert order totals to USD via dim_currency.

Source: ecommerce.raw_orders, analytics_warehouse.dim_currency
Target: ecommerce.staging_orders
Owner: data-platform-team@example.com
Grain: one row per order
Schedule: hourly
Dependencies: (none for raw_orders ingest; dim_currency is a slow-changing lookup)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("stage_orders").getOrCreate()

orders = spark.table("ecommerce.raw_orders")
currency = spark.table("analytics_warehouse.dim_currency")

joined = (
    orders.alias("o")
    .join(
        currency.alias("c"),
        F.col("o.currency") == F.col("c.currency_code"),
        "left",
    )
    .select(
        F.col("o.order_id").alias("order_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("o.order_date").alias("order_date"),
        F.col("o.status").alias("status"),
        (F.col("o.total_cents") * F.coalesce(F.col("c.usd_fx_rate"), F.lit(1.0)))
            .cast("bigint").alias("total_usd_cents"),
        F.col("o.currency").alias("original_currency"),
        F.col("o.region").alias("region"),
    )
)

(joined
    .write
    .mode("overwrite")
    .partitionBy("order_date")
    .saveAsTable("ecommerce.staging_orders"))

spark.stop()
