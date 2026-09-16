"""Compute resolution_hours = closed_at - opened_at.

Source: ecommerce.raw_support_tickets
Target: ecommerce.staging_support_tickets
Owner: cx-eng@example.com
Grain: one row per support ticket
Schedule: hourly
Dependencies: ingest_support_tickets
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("stage_support_tickets").getOrCreate()

tickets = spark.table("ecommerce.raw_support_tickets")

out_df = (
    tickets
    .withColumn(
        "resolution_hours",
        ((F.unix_timestamp("closed_at") - F.unix_timestamp("opened_at")) / 3600).cast("int"),
    )
    .select(
        "ticket_id", "customer_id", "priority", "status",
        "opened_at", "closed_at", "resolution_hours",
    )
)

(out_df
    .write
    .mode("overwrite")
    .saveAsTable("ecommerce.staging_support_tickets"))

spark.stop()
