"""Top-N products per calendar month.

Source: analytics_warehouse.fact_revenue
Target: analytics_warehouse.mart_top_products
Owner: merch-analytics@example.com
Grain: one row per (product, period_start)
Schedule: daily
Dependencies: fact_revenue
"""
from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.appName("mart_top_products").getOrCreate()

rev = spark.table("analytics_warehouse.fact_revenue")

per_month = (
    rev
    .withColumn("period_start", F.trunc("order_date", "MM"))
    .groupBy("period_start", "product_id")
    .agg(
        F.sum("quantity").alias("quantity_sold"),
        F.sum("line_total_usd_cents").alias("gross_revenue_usd_cents"),
    )
)

ranked = (
    per_month
    .withColumn(
        "rank",
        F.row_number().over(
            Window.partitionBy("period_start").orderBy(F.col("gross_revenue_usd_cents").desc())
        ),
    )
    .withColumn("period_end", F.add_months(F.col("period_start"), 1))
    .select(
        "product_id", "period_start", "period_end",
        "quantity_sold", "gross_revenue_usd_cents", "rank",
    )
)

(ranked
    .write
    .mode("overwrite")
    .partitionBy("period_start")
    .saveAsTable("analytics_warehouse.mart_top_products"))

spark.stop()
