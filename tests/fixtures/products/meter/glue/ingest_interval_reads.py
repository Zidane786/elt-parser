"""Land 15-minute interval meter reads from the AMI head-end S3 drop.

Source: s3://utility-datalake/meter/landing/interval_reads/ (AMI head-end export)
Target: meter_raw.interval_reads
Owner: smart-metering-team@utility.example.com
Grain: one row per meter per 15-minute interval
Schedule: hourly
Dependencies: none
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("ingest_interval_reads").getOrCreate()

# External read: raw AMI head-end parquet drop on S3 (no upstream catalog table).
raw = (
    spark.read.format("parquet")
    .load("s3://utility-datalake/meter/landing/interval_reads/")
)

out_df = raw.select(
    F.col("meter_id").cast("bigint").alias("meter_id"),
    F.col("read_ts").cast("timestamp").alias("read_ts"),
    F.col("kwh").cast("double").alias("kwh"),
    F.to_date(F.col("read_ts")).alias("read_date"),
)

(out_df
    .write
    .mode("overwrite")
    .partitionBy("read_date")
    .saveAsTable("meter_raw.interval_reads"))

spark.stop()
