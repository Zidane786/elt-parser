"""Daily revenue + refunds + net.

Source: analytics_warehouse.fact_orders, analytics_warehouse.fact_refunds
Target: analytics_warehouse.mart_daily_revenue
Owner: data-platform-team@example.com
Grain: one row per calendar day
Schedule: daily
Dependencies: fact_orders, fact_refunds
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("mart_daily_revenue").getOrCreate()

orders = spark.table("analytics_warehouse.fact_orders")
refunds = spark.table("analytics_warehouse.fact_refunds")

orders_daily = (
    orders.groupBy("order_date")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_usd_cents").alias("gross_revenue_usd_cents"),
    )
    .withColumnRenamed("order_date", "revenue_date")
)

refunds_daily = (
    refunds.groupBy("refund_date")
    .agg(F.sum("amount_usd_cents").alias("refund_amount_usd_cents"))
    .withColumnRenamed("refund_date", "revenue_date")
)

out_df = (
    orders_daily.alias("o")
    .join(refunds_daily.alias("r"), "revenue_date", "left")
    .select(
        "revenue_date",
        "order_count",
        "gross_revenue_usd_cents",
        F.coalesce(F.col("refund_amount_usd_cents"), F.lit(0)).alias("refund_amount_usd_cents"),
        (F.col("gross_revenue_usd_cents")
            - F.coalesce(F.col("refund_amount_usd_cents"), F.lit(0))).alias("net_revenue_usd_cents"),
    )
)

(out_df
    .write
    .mode("overwrite")
    .partitionBy("revenue_date")
    .saveAsTable("analytics_warehouse.mart_daily_revenue"))

spark.stop()
