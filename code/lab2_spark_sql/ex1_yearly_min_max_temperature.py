#!/usr/bin/env python3
"""BDA2 Assignment 1 (Spark SQL / DataFrame API).

Lowest and highest temperature each year, 1950-2014, with the station that
recorded it, sorted by temperature. Input: temperature-readings.csv
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, split, avg, max as spark_max, min as spark_min, countDistinct, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# create a SparkContext which tells Spark how to access a cluster
spark = SparkSession.builder.appName("exercise 1").getOrCreate()
sc = spark.sparkContext

# create distributed datasets
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
temps = lines.map(lambda x: (
    x[0],                   # station number
    int(x[1][0:4]),         # year
    int(x[1][5:7]),         # month
    x[2],                   # time
    float(x[3]),            # temperature
    x[4]                    # quality
    ))

# schema for the dataframe
schema = StructType([
    StructField("station_num", StringType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("time", StringType()),
    StructField("temp", FloatType()),
    StructField("quality", StringType()),
])
df = spark.createDataFrame(temps, schema = schema)

# filter the years
filtered_df = df.where((col("year") >= 1950) & (col("year") <= 2014))

# get max temperature per year
max_temps = filtered_df.groupBy("year") \
                        .agg(spark_max("temp").alias("temp"))

# get the station number for the max temperatures
max_temps = max_temps.join(filtered_df, ["year", "temp"]) \
                     .select("year", "station_num", "temp") \
                     .sort("temp", ascending = False)

# get min temperature per year
min_temps = filtered_df.groupBy("year") \
                        .agg(spark_min("temp").alias("temp"))

# get the station number for the min temperatures
min_temps = min_temps.join(filtered_df, ["year", "temp"]) \
                     .select("year", "station_num", "temp") \
                     .sort("temp", ascending = False)

# save result
max_temps.write.csv("BDA/output/max_temps", header=True)
min_temps.write.csv("BDA/output/min_temps", header=True)
