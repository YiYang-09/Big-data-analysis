#!/usr/bin/env python3
"""BDA2 Assignment 4 (Spark SQL / DataFrame API).

Stations whose maximum temperature is between 25 and 30 degrees and whose
maximum daily precipitation is between 100 and 200 mm.
Input: temperature-readings.csv, precipitation-readings.csv
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Create a SparkSession
spark = SparkSession.builder.appName("exercise 4").getOrCreate()
sc = spark.sparkContext

# Read and parse temperature dataset
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
temp_lines = temperature_file.map(lambda x: x.split(";")).map(lambda x: (
    x[0],                   # station_num
    x[1],                   # date
    float(x[3])             # temperature
))

temp_schema = StructType([
    StructField("station_num", StringType()),
    StructField("date", StringType()),
    StructField("temp", FloatType()),
])

df_temp = spark.createDataFrame(temp_lines, schema=temp_schema)

# Get maximum temperature per station
max_temp_per_station = df_temp.groupBy("station_num") \
                              .agg(spark_max("temp").alias("max_temp")) \
                              .filter((col("max_temp") >= 25) & (col("max_temp") <= 30))

# Read and parse precipitation dataset
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
prec_lines = precipitation_file.map(lambda x: x.split(";")).map(lambda x: (
    x[0],                   # station_num
    x[1],                   # date
    float(x[3])             # precipitation
))

prec_schema = StructType([
    StructField("station_num", StringType()),
    StructField("date", StringType()),
    StructField("prec", FloatType()),
])

df_prec = spark.createDataFrame(prec_lines, schema=prec_schema)

# Sum daily precipitation per station
daily_prec = df_prec.groupBy("station_num", "date") \
                    .agg(spark_sum("prec").alias("daily_prec"))

# Max daily precipitation per station
max_prec_per_station = daily_prec.groupBy("station_num") \
                                 .agg(spark_max("daily_prec").alias("max_prec")) \
                                 .filter((col("max_prec") >= 100) & (col("max_prec") <= 200))

# Join filtered temp and prec on station number
final_result = max_temp_per_station.join(max_prec_per_station, on="station_num", how="inner") \
                                   .select("station_num", "max_temp", "max_prec") \
                                   .orderBy("station_num", ascending=False)

# Save result
final_result.write.csv("BDA/output/exercise4", header=True)
