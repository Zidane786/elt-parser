"""Pull tickets from the Zendesk API.

Source: Zendesk REST API (https://example.zendesk.com/api/v2/tickets.json)
Target: ecommerce.raw_support_tickets
Owner: cx-eng@example.com
Grain: one row per ticket
Schedule: hourly
Dependencies: (none — root ingest)

The API uses cursor pagination. We persist the last-seen cursor in
s3://duke-state/zendesk_cursor.txt so subsequent runs are incremental.

KNOWN BUG (TRACKED IN BACKLOG-1843):
  The dropDuplicates() call below subsetting only on ["ticket_id"] is wrong.
  When the same ticket gets updated multiple times in a single page, we keep
  whichever Spark happens to encounter first — usually NOT the latest. The
  symptom: tickets occasionally show a stale `status` for a few hours after
  an agent updates them. The fix is to keep the row with the maximum
  `updated_at` per ticket_id.
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("ingest_support_tickets").getOrCreate()

# In production we use a Spark Connect API source. For brevity we read a
# parquet drop here.
raw = spark.read.parquet("s3://zendesk-exports/tickets/")

out_df = (
    raw
    .select(
        F.col("ticket_id").cast("bigint"),
        F.col("customer_id").cast("bigint"),
        F.col("subject").cast("string"),
        F.col("priority").cast("string"),
        F.col("status").cast("string"),
        F.col("opened_at").cast("timestamp"),
        F.col("closed_at").cast("timestamp"),
    )
    # BUG: see header — should keep the latest row per ticket_id.
    .dropDuplicates(["ticket_id"])
)

(out_df
    .write
    .mode("append")
    .saveAsTable("ecommerce.raw_support_tickets"))

spark.stop()
