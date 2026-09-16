"""Visit -> product_view -> add_to_cart -> checkout -> purchase funnel.

Source: analytics_warehouse.fact_sessions, ecommerce.raw_page_views
Target: analytics_warehouse.mart_funnel_conversion
Owner: analytics-eng@example.com
Grain: one row per (funnel_date, step)
Schedule: daily
Dependencies: (none)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("mart_funnel_conversion").getOrCreate()

sessions = spark.table("analytics_warehouse.fact_sessions")
page_views = spark.table("ecommerce.raw_page_views")

# Map each pageview URL onto a funnel step.
mapped = (
    page_views
    .withColumn(
        "step",
        F.when(F.col("url").startswith("/products/"), F.lit("product_view"))
         .when(F.col("url") == "/cart", F.lit("add_to_cart"))
         .when(F.col("url") == "/checkout", F.lit("checkout_start"))
         .when(F.col("url").startswith("/order/confirmation"), F.lit("purchase"))
         .otherwise(F.lit("visit")),
    )
    .withColumn("funnel_date", F.to_date("viewed_at"))
)

step_order_map = {
    "visit": 1, "product_view": 2, "add_to_cart": 3,
    "checkout_start": 4, "purchase": 5,
}
step_order_expr = F.create_map(
    *[item for k, v in step_order_map.items() for item in (F.lit(k), F.lit(v))]
)

step_users = (
    mapped.groupBy("funnel_date", "step")
    .agg(F.countDistinct("session_id").alias("users_entered"))
    .withColumn("step_order", step_order_expr[F.col("step")])
)

# users_completed is the next step's users (lead by 1 within each funnel_date).
from pyspark.sql.window import Window  # noqa: E402

w = Window.partitionBy("funnel_date").orderBy("step_order")
out_df = (
    step_users
    .withColumn("users_completed",
                F.lead("users_entered", 1).over(w).cast("int"))
    .withColumn(
        "conversion_rate",
        (F.col("users_completed") / F.col("users_entered")).cast("double"),
    )
    .select(
        "funnel_date", "step", "step_order",
        "users_entered", "users_completed", "conversion_rate",
    )
)

(out_df
    .write
    .mode("overwrite")
    .partitionBy("funnel_date")
    .saveAsTable("analytics_warehouse.mart_funnel_conversion"))

spark.stop()
