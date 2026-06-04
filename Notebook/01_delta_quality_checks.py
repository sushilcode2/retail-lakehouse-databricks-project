# Databricks notebook source
# DBTITLE 1,Check Delta Table History
# MAGIC %sql
# MAGIC DESCRIBE HISTORY retail_catalog.retail_lakehouse.gold_order_details;

# COMMAND ----------

# DBTITLE 1,Read Previous Version
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_catalog.retail_lakehouse.gold_order_details
# MAGIC VERSION AS OF 0;

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
customers_df = spark.table("retail_catalog.retail_lakehouse.silver_customers")
products_df = spark.table("retail_catalog.retail_lakehouse.silver_products")
orders_df = spark.table("retail_catalog.retail_lakehouse.silver_orders")

dq_results = {
    "null_customer_id": customers_df.filter("customer_id IS NULL").count(),
    "null_product_id": products_df.filter("product_id IS NULL").count(),
    "negative_price": products_df.filter("price < 0").count(),
    "null_order_id": orders_df.filter("order_id IS NULL").count(),
    "invalid_quantity": orders_df.filter("quantity <= 0").count()
}

dq_results

# COMMAND ----------

# DBTITLE 1,Fail Pipeline if Bad Data  Exists
failed_checks = {k: v for k, v in dq_results.items() if v > 0}

if failed_checks:
    raise Exception(f"Data Quality Failed: {failed_checks}")
else:
    print("All Data Quality Checks Passed")

# COMMAND ----------

# DBTITLE 1,Create Data quality Log Table
from pyspark.sql.functions import current_timestamp

dq_log_data = [(k, v) for k, v in dq_results.items()]

dq_log_df = spark.createDataFrame(dq_log_data, ["check_name", "failed_count"]) \
    .withColumn("checked_at", current_timestamp())

display(dq_log_df)

# COMMAND ----------

# DBTITLE 1,Save DQ Logs
dq_log_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("retail_catalog.retail_lakehouse.dq_validation_log")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_catalog.retail_lakehouse.dq_validation_log
# MAGIC ORDER BY checked_at DESC;

# COMMAND ----------

print("Data quality checks completed successfully")