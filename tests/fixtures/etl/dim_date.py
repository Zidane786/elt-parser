"""Generate the calendar dimension for 2024-2026.

Source: (none — generated)
Target: analytics_warehouse.dim_date
Owner: data-platform-team@example.com
Grain: one row per calendar day
Schedule: on-demand (regenerated when the calendar horizon extends)
Dependencies: (none)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("dim_date").getOrCreate()

START = "2024-01-01"
END = "2026-12-31"

# Use Spark's sequence() to mint one row per day, then derive calendar attrs.
calendar = (
    spark.sql(f"SELECT explode(sequence(DATE '{START}', DATE '{END}', interval 1 day)) AS full_date")
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    # dayofweek returns 1=Sun … 7=Sat; we want 0=Mon … 6=Sun.
    .withColumn("day_of_week", (F.dayofweek("full_date") + F.lit(5)) % F.lit(7))
    .withColumn(
        "is_weekend",
        F.when(F.col("day_of_week") >= 5, F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )
    .withColumn("iso_week", F.weekofyear("full_date"))
    .select(
        "date_key", "full_date", "year", "quarter", "month",
        "day", "day_of_week", "is_weekend", "iso_week",
    )
)

(calendar
    .write
    .mode("overwrite")
    .saveAsTable("analytics_warehouse.dim_date"))

spark.stop()
