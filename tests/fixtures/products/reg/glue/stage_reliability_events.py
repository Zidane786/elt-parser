"""Conform outage events into reliability inputs per feeder/period.

Source: outage_cur.fact_outage
Target: reg_stg.reliability_events
Owner: regulatory-reporting@utility.example.com
Grain: one row per outage event (feeder x period)
Schedule: 0 4 1 * *
Dependencies: none
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("stage_reliability_events").getOrCreate()

# Cross-product read: the curated outage fact owned by outage_management.
outages = spark.table("outage_cur.fact_outage")

# Derive the regulatory reporting period (YYYY-Qn) from the outage event_date,
# and treat each affected premise as one customer-interruption for SAIDI/SAIFI.
events = (
    outages
    .withColumn(
        "period",
        F.concat_ws(
            "-",
            F.year("event_date").cast("string"),
            F.concat(F.lit("Q"), F.quarter("event_date").cast("string")),
        ),
    )
    .withColumn("customers_affected", F.lit(1).cast("int"))
    .select(
        "feeder_id",
        "outage_id",
        "period",
        "customers_affected",
        F.col("minutes").cast("int").alias("minutes"),
    )
)

(events
    .write
    .mode("overwrite")
    .saveAsTable("reg_stg.reliability_events"))

spark.stop()
