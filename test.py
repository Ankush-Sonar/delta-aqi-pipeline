# Databricks notebook source
# MAGIC %sql
# MAGIC select * from aqi_cat.bronze_schema.aqi_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aqi_cat.bronze_schema.delhi_combined where `PM2.5` is  null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aqi_cat.bronze_schema.visakhapatnam_combined where `PM2.5` is null;