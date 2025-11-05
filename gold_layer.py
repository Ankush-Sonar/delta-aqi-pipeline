# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F, Window


# Initialize an empty DataFrame with schema
schema = StructType([
    StructField("loc", StringType(), True),
    StructField("month", StringType(), True),
    StructField("avg_aqi", DoubleType(), True),
    StructField("aqi_category", StringType(), True),
])
df_union = spark.createDataFrame([], schema)
df_category = spark.table("aqi_cat.silver_schema.aqi_category")

# Create Table to see Monthly Average AQI of Each City - Monthly AQI
from pyspark.sql import functions as F

for table in spark.catalog.listTables("aqi_cat.silver_schema"):
    if table.name != "aqi_category":
        df = spark.table(f"aqi_cat.silver_schema.{table.name}")
        
        # Aggregate avg_aqi per loc and month
        df_res = (
            df.withColumn("month", F.date_format("date", "MM-yyyy"))
              .groupBy("loc", "month")
              .agg(F.round(F.avg("aqi_pm25"), ).cast(DecimalType(5, 2))
              .alias("avg_aqi"))
        )
        
        # Join with aqi_category table using range condition
        df_joined_new = df_res.join(
            df_category,
            df_res["avg_aqi"].between(df_category["aqi_min"], df_category["aqi_max"]),
            "left"
        ).select("loc", "month", "avg_aqi", "aqi_category")

        # Append to df_union
        df_union = df_union.union(df_joined_new)

df_union.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("aqi_cat.gold_schema.union")
display(df_union)