"""Collections summary per reporting period, netting outage bill credits.

Source: bill_cur.fact_invoice, outage_cur.fact_outage
Target: reg_cur.mart_collections_summary
Owner: regulatory-reporting@utility.example.com
Grain: one row per reporting period
Schedule: 0 6 1 * *
Dependencies: none
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("mart_collections_summary").getOrCreate()

# Cross-product reads: billing invoices + outage events (for credit accruals).
invoices = spark.table("bill_cur.fact_invoice")
outages = spark.table("outage_cur.fact_outage")

# Reporting period (YYYY-Qn) on each side so they line up.
inv = (
    invoices
    .withColumn(
        "period",
        F.concat_ws(
            "-",
            F.year("issue_date").cast("string"),
            F.concat(F.lit("Q"), F.quarter("issue_date").cast("string")),
        ),
    )
    .withColumn("collected_amount", F.when(F.col("status") == "paid", F.col("amount")).otherwise(F.lit(0.0)))
)

# Outage minutes -> a small per-account goodwill credit, attributed by account.
out = (
    outages
    .withColumn(
        "period",
        F.concat_ws(
            "-",
            F.year("event_date").cast("string"),
            F.concat(F.lit("Q"), F.quarter("event_date").cast("string")),
        ),
    )
    .withColumn("credit", (F.col("minutes") * F.lit(0.02)))
    .groupBy("period", "account_id")
    .agg(F.sum("credit").alias("account_credit"))
)

# Join invoices to per-account outage credits on the shared account_id.
joined = inv.join(out, on="account_id", how="left")

summary = (
    joined
    .groupBy(inv["period"].alias("period"))
    .agg(
        F.sum("amount").alias("invoiced"),
        (F.sum("collected_amount") - F.coalesce(F.sum("account_credit"), F.lit(0.0))).alias("collected"),
    )
    .withColumn(
        "collection_rate",
        F.when(F.col("invoiced") > 0, F.col("collected") / F.col("invoiced")).otherwise(F.lit(0.0)),
    )
    .select("period", "invoiced", "collected", "collection_rate")
)

(summary
    .write
    .mode("overwrite")
    .partitionBy("period")
    .saveAsTable("reg_cur.mart_collections_summary"))

spark.stop()
