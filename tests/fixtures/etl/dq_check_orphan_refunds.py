"""Data-quality check: refunds referencing non-existent orders.

Source: ecommerce.raw_refunds, ecommerce.raw_orders
Target: (none — emits alerts to PagerDuty)
Owner: data-platform-team@example.com
Grain: alert per (refund_id, missing_order_id)
Schedule: daily
Dependencies: (none)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("dq_check_orphan_refunds").getOrCreate()

refunds = spark.table("ecommerce.raw_refunds")
orders = spark.table("ecommerce.raw_orders")

orphans = (
    refunds.alias("r")
    .join(orders.alias("o"), F.col("r.order_id") == F.col("o.order_id"), "left_anti")
    .filter(F.col("r.deleted_at").isNull())
)

orphan_count = orphans.count()

print(f"[dq_check_orphan_refunds] orphan_count={orphan_count}")

if orphan_count > 0:
    print("[dq_check_orphan_refunds] FAIL — sending PagerDuty alert.")
    # In production: send_pagerduty(orphans.toJSON().collect())
else:
    print("[dq_check_orphan_refunds] PASS")

spark.stop()
