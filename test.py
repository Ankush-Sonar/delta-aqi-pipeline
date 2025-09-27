# Databricks notebook source
# MAGIC %sql
# MAGIC select * from aqi_cat.bronze_schema.ka0007 where from_date between '2022-05-19' and '2022-05-30' and pm25 is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aqi_cat.bronze_schema.bengaluru_combined where `PM2.5` is null;