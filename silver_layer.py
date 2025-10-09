# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F, Window

df_combined_list=[]
df_hourly_list=[]
df_final_list=[]
df_category = spark.table("aqi_cat.bronze_schema.aqi_category")

def safe_float(colname):
        return when(
            col(colname).rlike("^[0-9]*\\.?[0-9]+$"), round(col(colname).cast("float"),0)
        ).otherwise(None)

df_category = (
    df_category
    .withColumn("pm25_min", safe_float("pm25_min"))
    .withColumn("pm25_max", safe_float("pm25_max"))
    .withColumn("pm10_min", safe_float("pm10_min"))
    .withColumn("pm10_max", safe_float("pm10_max"))
    .withColumn("no2_min", safe_float("no2_min"))
    .withColumn("no2_max", safe_float("no2_max"))
    .withColumn("so2_min", safe_float("so2_min"))
    .withColumn("so2_max", safe_float("so2_max"))
    .withColumn("o3_min", safe_float("o3_min"))
    .withColumn("o3_max", safe_float("o3_max"))
    .withColumn("co_min", safe_float("co_min")) 
    .withColumn("co_max", safe_float("co_max"))
    .withColumn("nh3_min", safe_float("nh3_max"))
    .withColumn("nh3_max", safe_float("nh3_max"))
    .withColumn("pb_min", safe_float("pb_min"))
    .withColumn("pb_max", safe_float("pb_max"))
)

display(df_category)

for table in spark.catalog.listTables("aqi_cat.bronze_schema"):
    if "_hourly" in table.name:
        df_hourly_list.append(table.name.replace("_hourly",""))
    if "_aqi_dataset" in table.name:
        df_final_list.append(table.name.replace("_aqi_dataset",""))
    elif "_combined" in table.name:
        df_combined_list.append(table.name.replace("_combined",""))

print(df_combined_list)
print(df_hourly_list)
print(df_final_list)

for table in df_combined_list:
    if table == "aqi_category":
        continue

    df_combined = spark.table("aqi_cat.bronze_schema."+table+"_combined")

    combined_transformed = (
    df_combined.withColumnRenamed("Timestamp", "date")
    .withColumnRenamed("Location", "loc")
    .withColumnRenamed("PM2.5", "pm25")
    .withColumnRenamed("PM10", "pm10")
    .withColumnRenamed("NO2", "no2")
    .withColumnRenamed("NH3", "nh3")
    .withColumnRenamed("SO2", "so2")
    .withColumnRenamed("CO", "co")
    .withColumnRenamed("O3", "o3")
    .withColumn("date", to_date("date", "dd-MM-yyyy"))
    .withColumn("loc", trim(split("loc", "-").getItem(0)))
    .withColumn("pm25", safe_float("pm25"))
    .withColumn("pm10", safe_float("pm25"))
    .withColumn("no2", safe_float("pm25"))
    .withColumn("nh3", safe_float("pm25"))
    .withColumn("so2", safe_float("pm25"))
    .withColumn("co", safe_float("pm25"))
    .withColumn("o3", safe_float("pm25"))
    )

    if table in df_hourly_list:
        df_hourly = spark.table("aqi_cat.bronze_schema."+table+"_hourly")

        hourly_transformed = (
        df_hourly.withColumnRenamed("Date", "date")
        .withColumnRenamed("pm25", "pm25_hourly")
        .withColumnRenamed("pm10", "pm10_hourly")
        .withColumnRenamed("no2", "no2_hourly")
        .withColumnRenamed("so2", "so2_hourly")
        .withColumnRenamed("co", "co_hourly")
        .withColumnRenamed("ozone", "o3_hourly")
        .withColumn("date", to_date("date", "dd-MM-yyyy"))
        .withColumn("pm25_hourly", safe_float("pm25_hourly"))
        .withColumn("pm10_hourly", safe_float("pm10_hourly"))
        .withColumn("no2_hourly", safe_float("no2_hourly"))
        .withColumn("so2_hourly", safe_float("so2_hourly"))
        .withColumn("co_hourly", safe_float("co_hourly"))
        .withColumn("o3_hourly", safe_float("o3_hourly"))
        )   

        df_joined = (
        combined_transformed
        .join(hourly_transformed.alias("r"), on="date", how="left")
        .withColumn("pm25", coalesce(col("pm25"), col("pm25_hourly")))
        .withColumn("pm25", format_number("pm25",2))
        .withColumn("pm10", format_number("pm10",2))
        .withColumn("no2", format_number("no2",2))
        .withColumn("so2", format_number("so2",2))
        .withColumn("co", format_number("co",2))
        .withColumn("o3", format_number("o3",2))
        .select(
            col("date"),
            col("loc"),
            col("pm25"),
            col("pm10"),
            col("no2"),
            col("so2"),
            col("co"),
            col("o3")
                )
        )

        if table in df_final_list:
            df_final = spark.table("aqi_cat.bronze_schema."+table+"_aqi_dataset")

            final_transformed = (
            df_final.withColumnRenamed("Date", "date")
            .withColumnRenamed("pm25", "pm25_final")
            .withColumnRenamed("pm10", "pm10_final")
            .withColumnRenamed("no2", "no2_final")
            .withColumnRenamed("so2", "so2_final")
            .withColumnRenamed("co", "co_final")
            .withColumnRenamed("ozone", "o3_final")
            .withColumn("date", to_date("date", "dd-MM-yyyy"))
            .withColumn("pm25_final", safe_float("pm25_final"))
            .withColumn("pm10_final", safe_float("pm10_final"))
            .withColumn("no2_final", safe_float("no2_final"))
            .withColumn("so2_final", safe_float("so2_final"))
            .withColumn("co_final", safe_float("co_final"))
            .withColumn("o3_final", safe_float("o3_final"))
            )

            df_joined_final = (
            df_joined
            .join(final_transformed, on="date", how="left")
            .withColumn("pm25", coalesce(col("pm25"), col("pm25_final")))
            .withColumn("pm25", format_number("pm25",2))
            .select(
                col("date"),
                col("loc"),
                col("pm25"),
                col("pm10"),
                col("no2"),
                col("so2"),
                col("co"),
                col("o3")
                    )
            )

            df_joined = df_joined_final

    else:
        df_joined = combined_transformed

    # Define window: all rows before current date
    windowSpec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Compute average of all previous non-null values
    df_joined = df_joined.withColumn(
        "prev_avg",
        F.avg(F.col("pm25")).over(windowSpec)
    )

    # Fill nulls with the computed average
    df_joined = df_joined.withColumn(
        "pm25",
        F.when(F.col("pm25").isNull(), F.col("prev_avg"))
        .otherwise(F.col("pm25"))
    ).drop("prev_avg")

    df_joined_new = df_joined.join(df_category,(df_joined["pm25"].between(df_category["pm25_min"], df_category["pm25_max"])),"left") 
       
    df_joined_new = df_joined_new.withColumn("aqi_pm25", 
        round(((col("aqi_max") - col("aqi_min")) / (col("pm25_max") - col("pm25_min")) 
        * (col("pm25") - col("pm25_min")) + col("aqi_min")),0))

    df_joined_new=df_joined_new.select(
        "date",
        "loc",
        "pm25",
        "aqi_pm25",
        "aqi_category",
        "aqi_min",
        "aqi_max",
        "pm25_min",
        "pm25_max"
    )

    table_name = table+"_aqi_dataset"

    print(table_name)

    (
    df_joined_new.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"aqi_cat.silver_schema.{table_name}")
    )

    
        