"""Compute duration_sec + is_authenticated.

Source: ecommerce.raw_sessions
Target: ecommerce.staging_sessions
Owner: web-platform@example.com
Grain: one row per web session
Schedule: hourly
Dependencies: (none)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("stage_sessions").getOrCreate()

sessions = spark.table("ecommerce.raw_sessions")

out_df = (
    sessions
    .withColumn(
        "duration_sec",
        (F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")).cast("int"),
    )
    .withColumn(
        "is_authenticated",
        F.when(F.col("customer_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )
    .select(
        "session_id", "customer_id",
        "started_at", "ended_at",
        "duration_sec", "is_authenticated",
    )
)

(out_df
    .write
    .mode("overwrite")
    .saveAsTable("ecommerce.staging_sessions"))

spark.stop()
