"""Build the SCD-2 customer dimension.

Source: ecommerce.staging_customers
Target: analytics_warehouse.dim_customer
Owner: data-platform-team@example.com
Grain: one row per (customer_id × effective_from)
Schedule: daily
Dependencies: stage_customers

Logic:
  Compare incoming staging_customers to the current dim_customer.is_current=1
  slice. If `tier` or `country` has changed, expire the old row (set
  is_current=0, effective_to=now) and insert a new is_current=1 row.
"""
from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.appName("dim_customer_scd2").getOrCreate()

stage = spark.table("ecommerce.staging_customers")
existing = spark.table("analytics_warehouse.dim_customer").filter(F.col("is_current") == 1)

# 1. Build candidate "current" rows from staging.
cand = (
    stage.select(
        "customer_id",
        "email_norm",
        "full_name",
        "country",
        "tier",
        F.current_timestamp().alias("effective_from"),
        F.lit(None).cast("timestamp").alias("effective_to"),
        F.lit(1).cast("int").alias("is_current"),
    )
)

# 2. Compare against existing current rows. A change is any difference in
#    country or tier (the two slowly-changing attributes we track).
joined = (
    cand.alias("new")
    .join(existing.alias("old"), "customer_id", "left")
    .withColumn(
        "is_changed",
        (F.col("new.country") != F.col("old.country"))
        | (F.col("new.tier") != F.col("old.tier"))
        | F.col("old.customer_id").isNull(),
    )
)

# 3. Expire any old row that changed.
expired = (
    joined.filter(F.col("is_changed") & F.col("old.customer_id").isNotNull())
    .select(
        F.col("old.customer_sk").alias("customer_sk"),
        F.col("old.customer_id").alias("customer_id"),
        F.col("old.email_norm").alias("email_norm"),
        F.col("old.full_name").alias("full_name"),
        F.col("old.country").alias("country"),
        F.col("old.tier").alias("tier"),
        F.col("old.effective_from").alias("effective_from"),
        F.current_timestamp().alias("effective_to"),
        F.lit(0).cast("int").alias("is_current"),
    )
)

# 4. New current rows: assign a fresh surrogate key starting after the max.
max_sk = (
    spark.table("analytics_warehouse.dim_customer")
    .agg(F.max("customer_sk").alias("m"))
    .collect()[0]["m"]
) or 0

w = Window.orderBy("customer_id")
new_rows = (
    joined.filter(F.col("is_changed"))
    .select(
        F.col("new.customer_id").alias("customer_id"),
        F.col("new.email_norm").alias("email_norm"),
        F.col("new.full_name").alias("full_name"),
        F.col("new.country").alias("country"),
        F.col("new.tier").alias("tier"),
        F.col("new.effective_from").alias("effective_from"),
        F.col("new.effective_to").alias("effective_to"),
        F.col("new.is_current").alias("is_current"),
    )
    .withColumn("customer_sk", F.row_number().over(w) + F.lit(max_sk))
)

# 5. Unchanged rows are passed through as-is.
unchanged = (
    joined.filter(~F.col("is_changed"))
    .select(
        F.col("old.customer_sk").alias("customer_sk"),
        F.col("old.customer_id").alias("customer_id"),
        F.col("old.email_norm").alias("email_norm"),
        F.col("old.full_name").alias("full_name"),
        F.col("old.country").alias("country"),
        F.col("old.tier").alias("tier"),
        F.col("old.effective_from").alias("effective_from"),
        F.col("old.effective_to").alias("effective_to"),
        F.col("old.is_current").alias("is_current"),
    )
)

out_df = expired.unionByName(new_rows).unionByName(unchanged)

(out_df
    .write
    .mode("overwrite")
    .saveAsTable("analytics_warehouse.dim_customer"))

spark.stop()
