# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([
    # StructField("name", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("PM2.5", FloatType(), True),
    StructField("PM10", FloatType(), True),
    StructField("NO2", FloatType(), True),
    StructField("NH3", FloatType(), True),
    StructField("SO2", FloatType(), True),
    StructField("CO", FloatType(), True),
    StructField("O3", FloatType(), True),
])

for entity in dbutils.fs.ls("/Volumes/aqi_cat/bronze_schema/bronze_raw/"):
    df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load(f"/Volumes/aqi_cat/bronze_schema/bronze_raw/{entity.name}")
    )

    df = df.withColumn("Date", to_date("Date", "dd-MM-yyyy"))
    
    table_name = entity.name.replace(".csv", "")
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable(f"aqi_cat.bronze_schema.{table_name}")
    )

    df.show(5)



# inferSchema la define krr dar 

# from delta.tables import DeltaTable
# delta_table = DeltaTable.forName(spark, "aqi_cat.bronze_schema.bengaluru_bronze")
# history_df = delta_table.history()  # default shows all commits
# display(history_df)

# SELECT * FROM aqi_cat.bronze_schema.bengaluru_bronze VERSION AS OF 2;

# COMMAND ----------

daqi = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"/Volumes/aqi_cat/bronze_schema/bronze_raw/AQI_pollution_type.csv")
    )

daqi.show(5)

# COMMAND ----------

from pyspark.sql.functions import col

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/aqi_cat/bronze_schema/bronze_raw/KA001_new.csv")
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

(
df_selected.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("aqi_cat.bronze_schema.KA0007")
)


df_selected.head(5)