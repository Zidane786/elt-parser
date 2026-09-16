"""Compute transit_days = delivered_at - shipped_at.

Source: ecommerce.raw_shipments
Target: ecommerce.staging_shipments
Owner: fulfilment-eng@example.com
Grain: one row per shipment
Schedule: hourly
Dependencies: (none)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("stage_shipments").getOrCreate()

shipments = spark.table("ecommerce.raw_shipments")

out_df = (
    shipments
    .withColumn(
        "transit_days",
        F.datediff(F.col("delivered_at"), F.col("shipped_at")).cast("int"),
    )
    .select(
        "shipment_id", "order_id", "carrier", "status",
        "shipped_at", "delivered_at", "transit_days",
    )
)

(out_df
    .write
    .mode("overwrite")
    .saveAsTable("ecommerce.staging_shipments"))

spark.stop()
