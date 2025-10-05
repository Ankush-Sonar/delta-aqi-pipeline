# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

for entity in dbutils.fs.ls("/Volumes/aqi_cat/bronze_schema/bronze_raw/"):
    df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"/Volumes/aqi_cat/bronze_schema/bronze_raw/{entity.name}")
    )

    table_name = entity.name.replace(".csv", "")
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(f"aqi_cat.bronze_schema.{table_name}")
    )

    df.show(5)

# COMMAND ----------

from pyspark.sql.functions import col

for entity in dbutils.fs.ls("/Volumes/aqi_cat/bronze_schema/bronze_raw_hourly/"):
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"/Volumes/aqi_cat/bronze_schema/bronze_raw_hourly/{entity.name}")
        )


    from pyspark.sql.functions import col

    # Mapping of original to new column names
    rename_map = {
        "date": "Date",
        "`PM2.5 (ug/m3)`": "pm25",
        "PM10 (ug/m3)": "pm10",
        "NO2 (ug/m3)": "no2",
        "Ozone (ug/m3)": "ozone",
        "SO2 (ug/m3)": "so2",
        "CO (mg/m3)": "co",
    }

    df_selected = df.select([
        col(orig).alias(new) for orig, new in rename_map.items()
    ])

    table_name = entity.name.replace(".csv", "")

    (
    df_selected.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"aqi_cat.bronze_schema.{table_name}")
    )
 

# df_selected.head(5)

# COMMAND ----------

from pyspark.sql.functions import col

for entity in dbutils.fs.ls("/Volumes/aqi_cat/bronze_schema/bronze_raw_new/"):
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"/Volumes/aqi_cat/bronze_schema/bronze_raw_new/{entity.name}")
        )


    from pyspark.sql.functions import col

    # Mapping of original to new column names
    rename_map = {
        "date": "Date",
        "`PM2.5`": "pm25",
        "PM10": "pm10",
        "NO2": "no2",
        "O3": "ozone",
        "SO2": "so2",
        "CO": "co",
    }

    df_selected = df.select([
        col(orig).alias(new) for orig, new in rename_map.items()
    ])

    table_name = entity.name.replace(".csv", "").lower() #Bangalore_AQI_Dataset

    (
    df_selected.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"aqi_cat.bronze_schema.{table_name}")
    )
    
    display(table_name)
