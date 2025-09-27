# Databricks notebook source
from pyspark.sql.functions import *

df_main = spark.table("aqi_cat.bronze_schema.bengaluru_combined")

df_ref = spark.table("aqi_cat.bronze_schema.ka0007")  

df_joined = df_main.alias("m").join(
    df_ref.alias("r"),
    on="Date",
    how="left"
)

df_joined = df_joined.withColumn(
    "PM2.5",  # keep original name or rename later
    coalesce(col("m.`PM2.5`"), col("r.pm25"))
)

df_joined.createOrReplaceTempView("joined_aqi")

display(spark.sql("""
SELECT *
FROM joined_aqi
WHERE `PM2.5` IS NULL
"""))

