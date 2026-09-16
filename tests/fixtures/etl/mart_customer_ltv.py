"""Aggregate fact_orders + fact_refunds per customer.

Source: analytics_warehouse.fact_orders, analytics_warehouse.fact_refunds
Target: analytics_warehouse.mart_customer_lifetime_value
Owner: data-platform-team@example.com
Grain: one row per customer
Schedule: daily
Dependencies: fact_orders, fact_refunds
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("mart_customer_ltv").getOrCreate()

orders = spark.table("analytics_warehouse.fact_orders")
refunds = spark.table("analytics_warehouse.fact_refunds")
dim_cust = spark.table("analytics_warehouse.dim_customer").filter(F.col("is_current") == 1)

# Per-customer order aggregates.
order_agg = (
    orders.groupBy("customer_id")
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.count("order_id").alias("order_count"),
        F.sum("total_usd_cents").alias("gross_revenue_usd_cents"),
    )
)

# Per-customer refund aggregates — join refunds via fact_orders to map
# order_id → customer_id (refunds don't carry customer_id).
refund_agg = (
    refunds.alias("r")
    .join(orders.alias("o"), F.col("r.order_id") == F.col("o.order_id"), "left")
    .groupBy(F.col("o.customer_id").alias("customer_id"))
    .agg(F.sum(F.col("r.amount_usd_cents")).alias("refund_amount_usd_cents"))
)

ltv = (
    order_agg.alias("oa")
    .join(refund_agg.alias("ra"), "customer_id", "left")
    .join(dim_cust.select("customer_id", "tier").alias("c"), "customer_id", "left")
    .select(
        "customer_id",
        "first_order_date",
        "last_order_date",
        "order_count",
        F.col("gross_revenue_usd_cents"),
        F.coalesce(F.col("refund_amount_usd_cents"), F.lit(0)).alias("refund_amount_usd_cents"),
        (F.col("gross_revenue_usd_cents")
            - F.coalesce(F.col("refund_amount_usd_cents"), F.lit(0))).alias("net_ltv_usd_cents"),
        F.coalesce(F.col("c.tier"), F.lit("free")).alias("tier"),
    )
)

(ltv
    .write
    .mode("overwrite")
    .saveAsTable("analytics_warehouse.mart_customer_lifetime_value"))

spark.stop()
