# Databricks notebook source
# Bronze Notebook: Read raw CSV files and create Bronze Delta tables

base_path = "/Volumes/retail_catalog/retail_lakehouse/raw_files"
bronze_path = f"{base_path}/bronze"

customers_path = f"{bronze_path}/customers"
products_path = f"{bronze_path}/products"
orders_path = f"{bronze_path}/orders"

print("Bronze notebook started")
print(customers_path)
print(products_path)
print(orders_path)

# COMMAND ----------

customers_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(customers_path)

products_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(products_path)

orders_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(orders_path)

print("Raw CSV files loaded successfully")

# COMMAND ----------

customers_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_catalog.retail_lakehouse.bronze_customers")

products_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_catalog.retail_lakehouse.bronze_products")

orders_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_catalog.retail_lakehouse.bronze_orders")

print("Bronze Delta tables created successfully")

# COMMAND ----------

spark.sql("""
SHOW TABLES IN retail_catalog.retail_lakehouse
""").show()

print("Bronze layer completed successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN retail_lakehouse