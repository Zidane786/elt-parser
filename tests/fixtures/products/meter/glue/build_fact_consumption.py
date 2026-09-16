"""Roll cleaned interval reads to daily kWh and join meter/account keys.

Source: meter_stg.reads_clean, asset_cur.dim_meter, cust_cur.dim_account
Target: meter_cur.fact_consumption
Owner: smart-metering-team@utility.example.com
Grain: one row per meter per calendar day
Schedule: 0 2 * * *
Dependencies: stage_reads
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("build_fact_consumption").getOrCreate()

reads = spark.table("meter_stg.reads_clean")
dim_meter = spark.table("asset_cur.dim_meter")
dim_account = spark.table("cust_cur.dim_account")

# Aggregate interval reads to daily kWh per meter.
daily = (
    reads.groupBy("meter_id", F.col("read_date").alias("day"))
    .agg(F.sum("kwh").alias("kwh"))
)

# Join dim_meter to attach account_id + premise_id for the meter.
meter_keys = dim_meter.select("meter_id", "account_id", "premise_id")
enriched = daily.join(meter_keys, on="meter_id", how="inner")

# Validate the account exists in the account dimension.
accounts = dim_account.select("account_id")
validated = enriched.join(accounts, on="account_id", how="inner")

out_df = validated.select(
    "account_id",
    "meter_id",
    "premise_id",
    "day",
    "kwh",
)

(out_df
    .write
    .mode("overwrite")
    .partitionBy("day")
    .saveAsTable("meter_cur.fact_consumption"))

spark.stop()
