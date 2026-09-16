"""Monthly signup cohorts x months-since-signup retention.

Source: analytics_warehouse.dim_customer, analytics_warehouse.fact_orders
Target: analytics_warehouse.mart_cohort_retention
Owner: analytics-eng@example.com
Grain: one row per (signup_month, months_since_signup)
Schedule: daily
Dependencies: dim_customer_scd2, fact_orders

Per §12.0.1 of the analytics spec: cohorts are grouped by signup_month
(YYYY-MM) and retention is measured at fixed monthly offsets from cohort
start. Months-since-signup is computed as floor(months_between(order_date,
signup_date)).
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("mart_cohort_retention").getOrCreate()

# Use the SCD2 history's first appearance per customer as the signup date.
dim = (
    spark.table("analytics_warehouse.dim_customer")
    .groupBy("customer_id")
    .agg(F.min("effective_from").alias("signup_at"))
    .withColumn("signup_month", F.date_format("signup_at", "yyyy-MM"))
)

orders = spark.table("analytics_warehouse.fact_orders")

# Per-customer monthly activity flag.
activity = (
    orders.alias("o")
    .join(dim.alias("d"), "customer_id", "left")
    .withColumn(
        "months_since_signup",
        F.floor(F.months_between(F.col("o.order_date"), F.col("d.signup_at"))).cast("int"),
    )
    .filter(F.col("months_since_signup") >= 0)
    .select("customer_id", F.col("d.signup_month").alias("signup_month"), "months_since_signup")
    .distinct()
)

cohort_sizes = (
    dim.groupBy("signup_month").agg(F.countDistinct("customer_id").alias("cohort_size"))
)

cohort_active = (
    activity.groupBy("signup_month", "months_since_signup")
    .agg(F.countDistinct("customer_id").alias("retained"))
)

out_df = (
    cohort_active.alias("a")
    .join(cohort_sizes.alias("c"), "signup_month", "left")
    .select(
        "signup_month",
        "months_since_signup",
        "cohort_size",
        "retained",
        (F.col("retained") / F.col("cohort_size")).alias("retention_rate"),
    )
)

(out_df
    .write
    .mode("overwrite")
    .saveAsTable("analytics_warehouse.mart_cohort_retention"))

spark.stop()
