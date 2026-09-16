"""Lowercase email, concat full_name, derive is_active.

Source: ecommerce.raw_customers
Target: ecommerce.staging_customers
Owner: data-platform-team@example.com
Grain: one row per customer
Schedule: daily
Dependencies: (none — root of the customer lineage)
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("stage_customers").getOrCreate()

raw = spark.table("ecommerce.raw_customers")

out_df = (
    raw
    .withColumn("email_norm", F.lower(F.trim(F.col("email"))))
    .withColumn("full_name", F.concat_ws(" ",
                                         F.trim(F.col("first_name")),
                                         F.trim(F.col("last_name"))))
    .withColumn("is_active", F.lit(1).cast("int"))
    .select(
        "customer_id",
        "email_norm",
        "full_name",
        F.col("phone").cast(StringType()).alias("phone"),
        "signup_date",
        "country",
        "tier",
        "is_active",
    )
)

# Defensive: a customer row without an email is not usable downstream.
out_df = out_df.filter(F.col("email_norm").isNotNull() & (F.col("email_norm") != ""))

(out_df
    .write
    .mode("overwrite")
    .partitionBy("signup_date")
    .saveAsTable("ecommerce.staging_customers"))

spark.stop()
