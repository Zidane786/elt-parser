"""Snapshot product x warehouse stock.

Source: ecommerce.staging_product_inventory
Target: analytics_warehouse.fact_inventory
Owner: fulfilment-eng@example.com
Grain: one row per (product, warehouse, snapshot_date)
Schedule: daily
Dependencies: (none)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("fact_inventory").getOrCreate()

inv = spark.table("ecommerce.staging_product_inventory")

out_df = (
    inv
    # allocated = max(0, on_hand - reorder_level) is a placeholder until
    # we wire OMS reservations into this snapshot.
    .withColumn(
        "allocated",
        F.greatest(F.lit(0), F.col("on_hand") - F.col("reorder_level")).cast("int"),
    )
    .withColumn(
        "available",
        (F.col("on_hand") - F.col("allocated")).cast("int"),
    )
    .withColumn(
        "snapshot_id",
        F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"]).Window
            .orderBy("snapshot_date", "warehouse_id", "product_id")
        ),
    )
    .select(
        "snapshot_id", "product_id", "warehouse_id",
        "snapshot_date", "on_hand", "allocated", "available",
    )
)

(out_df
    .write
    .mode("overwrite")
    .partitionBy("snapshot_date")
    .saveAsTable("analytics_warehouse.fact_inventory"))

spark.stop()
