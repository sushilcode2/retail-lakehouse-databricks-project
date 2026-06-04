# Databricks notebook source
# Silver Notebook: Clean Bronze Delta tables and create Silver Delta tables

print("Silver notebook started")

customers_df = spark.table("retail_catalog.retail_lakehouse.bronze_customers")
products_df = spark.table("retail_catalog.retail_lakehouse.bronze_products")
orders_df = spark.table("retail_catalog.retail_lakehouse.bronze_orders")

print("Bronze tables loaded successfully")

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, to_date

silver_customers_df = customers_df \
    .dropDuplicates(["customer_id"]) \
    .withColumn("customer_name", trim(col("customer_name"))) \
    .withColumn("city", upper(trim(col("city")))) \
    .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))

silver_products_df = products_df \
    .dropDuplicates(["product_id"]) \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("category", upper(trim(col("category")))) \
    .withColumn("price", col("price").cast("double"))

silver_orders_df = orders_df \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("quantity", col("quantity").cast("int"))

print("Silver transformations completed")

# COMMAND ----------

silver_customers_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_catalog.retail_lakehouse.silver_customers")

silver_products_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_catalog.retail_lakehouse.silver_products")

silver_orders_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_catalog.retail_lakehouse.silver_orders")

print("Silver Delta tables created successfully")

# COMMAND ----------

spark.sql("""
SHOW TABLES IN retail_catalog.retail_lakehouse
""").show()

print("Silver layer completed successfully")