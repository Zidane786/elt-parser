"""Allocate campaign budget across each active day.

Source: ecommerce.raw_marketing_campaigns
Target: analytics_warehouse.fact_marketing_spend
Owner: growth-engineering@example.com
Grain: one row per (campaign, spend_date)
Schedule: daily
Dependencies: (none — campaigns are mastered in OLTP)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("fact_marketing_spend").getOrCreate()

campaigns = spark.table("ecommerce.raw_marketing_campaigns")

# Explode the date range start..end into one row per day, then evenly split
# total spend across active days.
exploded = (
    campaigns
    .withColumn(
        "spend_date",
        F.explode(F.sequence(F.col("start_date"), F.col("end_date"), F.expr("INTERVAL 1 DAY"))),
    )
    .withColumn(
        "active_days",
        F.datediff(F.col("end_date"), F.col("start_date")) + F.lit(1),
    )
    .withColumn(
        "spend_usd_cents",
        (F.col("spend_cents") / F.col("active_days")).cast("bigint"),
    )
    .withColumn("impressions", (F.col("spend_usd_cents") * F.lit(15)).cast("bigint"))
    .withColumn("clicks", (F.col("spend_usd_cents") * F.lit(0.01)).cast("bigint"))
    .withColumn(
        "spend_id",
        F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"]).Window
            .orderBy("campaign_id", "spend_date")
        ),
    )
    .select(
        "spend_id", "campaign_id", "spend_date", "channel",
        "spend_usd_cents", "impressions", "clicks",
    )
)

(exploded
    .write
    .mode("overwrite")
    .partitionBy("spend_date")
    .saveAsTable("analytics_warehouse.fact_marketing_spend"))

spark.stop()
