"""Tag refunds where amount > $500 AND reason in (chargeback, dispute_won).

Source: ecommerce.raw_refunds
Target: ecommerce.staging_refunds_with_flags
Owner: trust-and-safety@example.com
Grain: one row per refund
Schedule: hourly
Dependencies: (none — refunds are ingested directly)

Notes on soft-delete:
  raw_refunds has a `deleted_at` column. We carry it through here so the
  downstream mart can filter it; we do NOT filter at this stage because the
  fraud team still wants to score soft-deleted refunds for audit.
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("derive_refund_fraud_flag").getOrCreate()

refunds = spark.table("ecommerce.raw_refunds")

# Heuristic threshold: $500 = 50,000 cents. Combined with a reason in
# (chargeback, dispute_won) we flag the refund as likely fraud.
FRAUD_CENTS_THRESHOLD = 50_000
FRAUD_REASONS = ["chargeback", "dispute_won"]

out_df = (
    refunds
    .withColumn(
        "is_fraud_flagged",
        F.when(
            (F.col("amount_cents") > FRAUD_CENTS_THRESHOLD)
            & (F.col("reason").isin(FRAUD_REASONS)),
            F.lit(1),
        ).otherwise(F.lit(0)).cast("int"),
    )
    .select(
        "refund_id",
        "order_id",
        "reason",
        "amount_cents",
        "refund_date",
        "is_fraud_flagged",
        "deleted_at",
    )
)

(out_df
    .write
    .mode("overwrite")
    .partitionBy("refund_date")
    .saveAsTable("ecommerce.staging_refunds_with_flags"))

spark.stop()
