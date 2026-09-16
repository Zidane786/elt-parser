"""DESTRUCTIVE: scrub PII columns for customers inactive > 7 years.

Source: ecommerce.raw_customers
Target: ecommerce.raw_customers  (in-place UPDATE)
Owner: privacy-eng@example.com
Grain: one row per affected customer
Schedule: on-demand (kicked off via the privacy team Jira queue)
Dependencies: (none — runs against the OLTP system directly)

WARNING:
  This script issues UPDATE statements that overwrite PII columns with
  placeholder values. It IS NOT IDEMPOTENT in the sense that the original
  values are gone after the job completes. The `bash_safety_guard` in the
  agent CLI should require a HIL confirmation before allowing any tool to
  invoke this script.
"""
from datetime import date, timedelta

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("purge_pii_after_retention").getOrCreate()

customers = spark.table("ecommerce.raw_customers")

# Cutoff: 7 years ago.
cutoff = (date.today() - timedelta(days=365 * 7)).isoformat()

eligible = customers.filter(F.col("signup_date") < cutoff)

print(f"[purge_pii_after_retention] eligible_count={eligible.count()}")

# Scrub PII columns. Keep customer_id, signup_date, country, tier — needed
# for retention analytics.
scrubbed = (
    customers
    .withColumn(
        "email",
        F.when(F.col("signup_date") < cutoff, F.concat(F.lit("scrubbed-"), F.col("customer_id"), F.lit("@example.invalid")))
         .otherwise(F.col("email")),
    )
    .withColumn(
        "first_name",
        F.when(F.col("signup_date") < cutoff, F.lit("[scrubbed]")).otherwise(F.col("first_name")),
    )
    .withColumn(
        "last_name",
        F.when(F.col("signup_date") < cutoff, F.lit("[scrubbed]")).otherwise(F.col("last_name")),
    )
    .withColumn(
        "phone",
        F.when(F.col("signup_date") < cutoff, F.lit(None).cast("string")).otherwise(F.col("phone")),
    )
)

(scrubbed
    .write
    .mode("overwrite")
    .saveAsTable("ecommerce.raw_customers"))

spark.stop()
