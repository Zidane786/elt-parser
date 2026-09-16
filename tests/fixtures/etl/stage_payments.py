"""Derive is_successful boolean from status.

Source: ecommerce.raw_payments
Target: ecommerce.staging_payments
Owner: payments-eng@example.com
Grain: one row per payment attempt
Schedule: hourly
Dependencies: (none — payments are streamed in)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("stage_payments").getOrCreate()

payments = spark.table("ecommerce.raw_payments")

out_df = (
    payments
    .withColumn(
        "is_successful",
        F.when(F.col("status") == "captured", F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )
    .select(
        "payment_id", "order_id", "method", "status",
        "amount_cents", "processed_at", "is_successful",
    )
)

(out_df
    .write
    .mode("overwrite")
    .saveAsTable("ecommerce.staging_payments"))

spark.stop()
