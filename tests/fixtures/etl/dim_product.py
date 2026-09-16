"""Denormalise products with supplier and variants.

Source: ecommerce.raw_products, ecommerce.raw_suppliers, ecommerce.raw_product_variants
Target: analytics_warehouse.dim_product
Owner: catalog-eng@example.com
Grain: one row per product
Schedule: daily
Dependencies: (none — products are mastered in OLTP)
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("dim_product").getOrCreate()

products = spark.table("ecommerce.raw_products")
suppliers = spark.table("ecommerce.raw_suppliers")

# Aggregate variant counts per product so dim_product carries `variant_count`.
variants = (
    spark.table("ecommerce.raw_product_variants")
    .groupBy("product_id")
    .agg(F.count("variant_id").alias("variant_count"))
)

out_df = (
    products.alias("p")
    .join(suppliers.alias("s"),
          F.col("p.supplier_id") == F.col("s.supplier_id"),
          "left")
    .join(variants.alias("v"),
          F.col("p.product_id") == F.col("v.product_id"),
          "left")
    .select(
        F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"]).Window.orderBy("p.product_id")
        ).alias("product_sk"),
        F.col("p.product_id").alias("product_id"),
        F.col("p.sku").alias("sku"),
        F.col("p.name").alias("name"),
        F.col("p.category").alias("category"),
        F.col("p.list_price_cents").alias("list_price_cents"),
        F.coalesce(F.col("s.name"), F.lit("unknown")).alias("supplier_name"),
        F.col("p.is_active").alias("is_active"),
    )
)

(out_df
    .write
    .mode("overwrite")
    .saveAsTable("analytics_warehouse.dim_product"))

spark.stop()
