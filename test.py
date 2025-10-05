# Databricks notebook source
# Create a new volume in the specified catalog and schema
spark.sql("""
CREATE VOLUME aqi_cat.bronze_schema.bronze_raw_new
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aqi_cat.bronze_schema.aqi_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aqi_cat.bronze_schema.mumbai_combined where Timestamp = '03-04-2020'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aqi_cat.bronze_schema.visakhapatnam_combined where `PM2.5` is null;