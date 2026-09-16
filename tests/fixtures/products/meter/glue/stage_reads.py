"""Dedup and gap-fill raw 15-minute interval reads.

Source: meter_raw.interval_reads
Target: meter_stg.reads_clean
Owner: smart-metering-team@utility.example.com
Grain: one row per meter per 15-minute interval (deduped)
Schedule: 0 1 * * *
Dependencies: ingest_interval_reads
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("stage_reads").getOrCreate()

raw = spark.table("meter_raw.interval_reads")

# Dedup on (meter_id, read_ts), keep the last-written kwh; drop NULL keys.
clean = spark.sql(
    """
    SELECT meter_id, read_ts, kwh, read_date
    FROM (
        SELECT
            meter_id,
            read_ts,
            kwh,
            read_date,
            ROW_NUMBER() OVER (
                PARTITION BY meter_id, read_ts ORDER BY kwh DESC
            ) AS rn
        FROM meter_raw.interval_reads
        WHERE meter_id IS NOT NULL AND read_ts IS NOT NULL
    )
    WHERE rn = 1
    """
)

# Gap-fill negative / null kwh to 0.0 (meter rollover artefacts).
out_df = clean.withColumn(
    "kwh",
    F.when(F.col("kwh").isNull() | (F.col("kwh") < 0), F.lit(0.0)).otherwise(F.col("kwh")),
).select("meter_id", "read_ts", "kwh", "read_date")

(out_df
    .write
    .mode("overwrite")
    .partitionBy("read_date")
    .saveAsTable("meter_stg.reads_clean"))

spark.stop()
