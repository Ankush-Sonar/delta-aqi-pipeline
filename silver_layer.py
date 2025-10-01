# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

df_bronze = spark.table("aqi_cat.bronze_schema.bengaluru_combined")
df_ref = spark.table("aqi_cat.bronze_schema.bengaluru_hourly")


def safe_float(colname):
    return when(
        col(colname).rlike("^[0-9]*\\.?[0-9]+$"), col(colname).cast("float")
    ).otherwise(None)


bengaluru_combined_transformed = (
    df_bronze.withColumnRenamed("Timestamp", "date")
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

bengaluru_hourly_transformed = (
    df_ref.withColumnRenamed("Date", "date")
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
    bengaluru_combined_transformed.alias("m")
    .join(bengaluru_hourly_transformed.alias("r"), on="Date", how="left")
    .withColumn("m.pm25", coalesce(col("m.pm25"), col("r.pm25_hourly")))
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


display(df_joined)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

df_category = spark.table("aqi_cat.bronze_schema.aqi_category")

def safe_float(colname):
    return when(
        col(colname).rlike("^[0-9]*\\.?[0-9]+$"), col(colname).cast("float")
    ).otherwise(None)

# numeric_cols = [
#     'pm25_min','pm25_max','pm10_min','pm10_max','no2_min','no2_max',
#     'so2_min','so2_max','o3_min','o3_max','co_min','co_max',
#     'nh3_min','nh3_max','pb_min','pb_max'
# ]

# for c in numeric_cols:
#     df = df.withColumn(c, safe_float(c))


df = (
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


display(df)

print(df.dtypes)


# COMMAND ----------

# -- select * from aqi_cat.bronze_schema.aqi_category;
# -- select * from aqi_cat.bronze_schema.ka0007
# -- select * from aqi_cat.bronze_schema.bengaluru_combined

# from pyspark.sql.types import *
# from pyspark.sql.functions import *

# schema = StructType([
#     StructField("Date", StringType(), True),
#     StructField("Location", StringType(), True),
#     StructField("PM2.5", FloatType(), True),
#     StructField("PM10", FloatType(), True),
#     StructField("NO2", FloatType(), True),
#     StructField("NH3", FloatType(), True),
#     StructField("SO2", FloatType(), True),
#     StructField("CO", FloatType(), True),
#     StructField("O3", FloatType(), True),
# ])

# for table in spark.catalog.listTables("aqi_cat.bronze_schema"):
#     if table.name == "aqi_category" or table.name == "ka0007" or table.name == "_sqldf":
#         continue

#     df_bronze = spark.table(f"aqi_cat.bronze_schema.{table.name}")

#     df_transformed = (
#         df_bronze
#         .withColumnRenamed("Timestamp", "date")
#         .withColumnRenamed("Location", "loc")
#         .withColumnRenamed("PM2.5", "pm25")
#         .withColumnRenamed("PM10", "pm10")
#         .withColumnRenamed("NO2", "no2")
#         .withColumnRenamed("NH3", "nh3")
#         .withColumnRenamed("SO2", "so2")
#         .withColumnRenamed("CO", "co")
#         .withColumnRenamed("O3", "o3")
#         .withColumn("date", to_date("date", "dd-MM-yyyy"))
#         .withColumn("loc", trim(split("loc", "-").getItem(0)))
#         .withColumn("pm25", when((col("pm25") == "None") | (col("pm25") == "NA"), None).otherwise(col("pm25")).cast("float"))
#         .withColumn("pm10", when((col("pm10") == "None") | (col("pm10") == "NA"), None).otherwise(col("pm10")).cast("float"))
#         .withColumn("no2", when((col("no2") == "None") | (col("no2") == "NA"), None).otherwise(col("no2")).cast("float"))
#         .withColumn("nh3", when((col("nh3") == "None") | (col("nh3") == "NA"), None).otherwise(col("nh3")).cast("float"))
#         .withColumn("so2", when((col("so2") == "None") | (col("so2") == "NA"), None).otherwise(col("so2")).cast("float"))
#         .withColumn("co", when((col("co") == "None") | (col("co") == "NA"), None).otherwise(col("co")).cast("float"))
#         .withColumn("o3", when((col("o3") == "None") | (col("o3") == "NA"), None).otherwise(col("o3")).cast("float"))
#      )
    
    # table_name=table.name.split("_")[0]+"_silver"
    # print(table_name)
    # (
    #     df.write
    #       .format("delta")
    #       .mode("overwrite")
    #       .schema(schema)
    #       .saveAsTable(f"aqi_cat.silver_schema.{table_name}")
    # )

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# schema = StructType([
#     # StructField("name", StringType(), True),
#     StructField("Date", StringType(), True),
#     StructField("Location", StringType(), True),
#     StructField("PM2.5", FloatType(), True),
#     StructField("PM10", FloatType(), True),
#     StructField("NO2", FloatType(), True),
#     StructField("NH3", FloatType(), True),
#     StructField("SO2", FloatType(), True),
#     StructField("CO", FloatType(), True),
#     StructField("O3", FloatType(), True),
# ])

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

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/aqi_cat/bronze_schema/bronze_raw/AQI_pollution_type.csv")
    )

# rename_map = {
#     "Category":"category",
#     "`PM2.5 (ug/m3)`": "pm25",
#     "PM10 (ug/m3)": "pm10",
#     "Indian Range for NO2 (ug/m3)(24 hours)": "no2",
#     "Indian Range for O3 (ug/m3)": "ozone",
#     "Indian Range for SO2 (ug/m3)": "so2",
#     "Indian Range CO (mg/m3)": "co",
#     "Indian Range for NH3 (ug/m3)(24 hours)":"nh3",
#     "Indian Range for Pb (ug/m3)(24 hours)":"pb"
# }

# df_selected = df.select([
#     col(orig).alias(new) for orig, new in rename_map.items()
# ])

# schema = StructType([
#     # StructField("name", StringType(), True),
#     StructField("Date", StringType(), True),
#     StructField("Location", StringType(), True),
#     StructField("PM2.5", FloatType(), True),
#     StructField("PM10", FloatType(), True),
#     StructField("NO2", FloatType(), True),
#     StructField("NH3", FloatType(), True),
#     StructField("SO2", FloatType(), True),
#     StructField("CO", FloatType(), True),
#     StructField("O3", FloatType(), True),
# ])

# for c in ["pm25"]:
#     df_selected = df_selected.withColumn(f"{c.split('_')[0]}_min", split(col(c), "-").getItem(0).cast("float")) \
#                              .withColumn(f"{c.split('_')[0]}_max", split(col(c), "-").getItem(1).cast("float"))



# df.head(5)

# (
# df.write
#     .format("delta")
#     .mode("overwrite")
#     .saveAsTable("aqi_cat.bronze_schema.aqi_category")
# )


df_selected.display()

# COMMAND ----------

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
    coalesce(col("`PM2.5`"), col("pm25"))
)

df_joined.createOrReplaceTempView("joined_aqi")

# display(spark.sql("""
# SELECT *
# FROM joined_aqi
# WHERE `PM2.5` IS NULL
# """))

df_joined.display()
