"""Build the campaign dimension with is_active derived from today.

Source: ecommerce.raw_marketing_campaigns
Target: analytics_warehouse.dim_campaign
Owner: growth-engineering@example.com
Grain: one row per campaign
Schedule: daily
Dependencies: (none)
"""
from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.appName("dim_campaign").getOrCreate()

raw = spark.table("ecommerce.raw_marketing_campaigns")

out_df = (
    raw
    .withColumn(
        "is_active",
        F.when(
            F.current_date().between(F.col("start_date"), F.col("end_date")),
            F.lit(1),
        ).otherwise(F.lit(0)).cast("int"),
    )
    .withColumn("campaign_sk", F.row_number().over(Window.orderBy("campaign_id")))
    .select(
        "campaign_sk", "campaign_id", "name", "channel",
        "spend_cents", "start_date", "end_date", "is_active",
    )
)

(out_df
    .write
    .mode("overwrite")
    .saveAsTable("analytics_warehouse.dim_campaign"))

spark.stop()
