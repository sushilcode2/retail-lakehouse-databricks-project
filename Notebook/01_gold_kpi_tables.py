# Databricks notebook source
# DBTITLE 1,read silver table
customers_df = spark.table("retail_catalog.retail_lakehouse.silver_customers")
products_df = spark.table("retail_catalog.retail_lakehouse.silver_products")
orders_df = spark.table("retail_catalog.retail_lakehouse.silver_orders")

# COMMAND ----------

# DBTITLE 1,Import functions
from pyspark.sql.functions import col, sum, count, round

# COMMAND ----------

# DBTITLE 1,create order details table
order_details_df = orders_df \
    .join(customers_df, "customer_id", "left") \
    .join(products_df, "product_id", "left") \
    .withColumn("total_amount", col("quantity") * col("price"))

display(order_details_df)

# COMMAND ----------

# DBTITLE 1,Save Gold Order Details Table
order_details_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_catalog.retail_lakehouse.gold_order_details")

# COMMAND ----------

# DBTITLE 1,daily revenue KPI
daily_revenue_df = order_details_df \
    .groupBy("order_date") \
    .agg(
        round(sum("total_amount"), 2).alias("daily_revenue"),
        count("order_id").alias("total_orders")
    ) \
    .orderBy("order_date")

display(daily_revenue_df)

# COMMAND ----------

# DBTITLE 1,save daily revenue table
daily_revenue_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_catalog.retail_lakehouse.gold_daily_revenue")

# COMMAND ----------

# DBTITLE 1,Revenue by Category
category_revenue_df = order_details_df \
    .groupBy("category") \
    .agg(
        round(sum("total_amount"), 2).alias("category_revenue"),
        count("order_id").alias("total_orders")
    ) \
    .orderBy(col("category_revenue").desc())

display(category_revenue_df)

# COMMAND ----------

# DBTITLE 1,save Category Revenue table
category_revenue_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_catalog.retail_lakehouse.gold_category_revenue")

# COMMAND ----------

# DBTITLE 1,Top Customers
top_customers_df = order_details_df \
    .groupBy("customer_id", "customer_name", "city") \
    .agg(
        round(sum("total_amount"), 2).alias("customer_revenue"),
        count("order_id").alias("total_orders")
    ) \
    .orderBy(col("customer_revenue").desc())

display(top_customers_df)

# COMMAND ----------

# DBTITLE 1,Save Top Customer Table
top_customers_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_catalog.retail_lakehouse.gold_top_customers")

# COMMAND ----------

# DBTITLE 1,Verify all gold tables
spark.sql("""
SHOW TABLES IN retail_catalog.retail_lakehouse
""").show()

# COMMAND ----------

# DBTITLE 1,time travel table history versioning
# MAGIC %sql
# MAGIC DESCRIBE HISTORY retail_catalog.retail_lakehouse.gold_order_details

# COMMAND ----------

# DBTITLE 1,data quality check
customers_df.filter("customer_id is null").count()
products_df.filter("price < 0").count()
orders_df.filter("quantity <= 0").count()

# COMMAND ----------

print("Gold layer completed successfully")