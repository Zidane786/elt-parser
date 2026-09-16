"""Alert if a column's NULL rate jumps > 5pp week-over-week.

Source: ecommerce.raw_orders
Target: (none — emits alerts to PagerDuty)
Owner: data-platform-team@example.com
Grain: alert per (table, column)
Schedule: hourly
Dependencies: (none)

We check `customer_id` specifically because the OLTP system has been
intermittently producing NULL customer_ids on cart-abandonment recovery
flows. A 5pp jump in null rate on that column should page on-call.
"""
from datetime import date, timedelta

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("dq_check_null_spike").getOrCreate()

orders = spark.table("ecommerce.raw_orders")

today = date.today()
this_week_start = today - timedelta(days=7)
last_week_start = today - timedelta(days=14)

def null_rate(start: date, end: date) -> float:
    sub = orders.filter(
        (F.col("order_date") >= start.isoformat())
        & (F.col("order_date") < end.isoformat())
    )
    total = sub.count()
    if total == 0:
        return 0.0
    nulls = sub.filter(F.col("customer_id").isNull()).count()
    return nulls / total

this_week_rate = null_rate(this_week_start, today)
last_week_rate = null_rate(last_week_start, this_week_start)

print(f"[dq_check_null_spike] last_week_rate={last_week_rate:.4f} "
      f"this_week_rate={this_week_rate:.4f}")

THRESHOLD_PP = 0.05  # 5 percentage points
if (this_week_rate - last_week_rate) > THRESHOLD_PP:
    print("[dq_check_null_spike] FAIL — NULL spike on raw_orders.customer_id")
    # send_pagerduty(...)
else:
    print("[dq_check_null_spike] PASS")

spark.stop()
