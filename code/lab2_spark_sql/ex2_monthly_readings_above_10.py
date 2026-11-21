#!/usr/bin/env python3
"""BDA2 Assignment 2 (Spark SQL / DataFrame API).

Count readings above 10 degrees per month in 1950-2014, and count distinct
stations per month. Input: temperature-readings.csv
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, split, avg, max as spark_max, min as spark_min, countDistinct, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# create a SparkContext which tells Spark how to access a cluster
spark = SparkSession.builder.appName("exercise 2").getOrCreate()
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

# filter years and temperature over 10ºC
filtered_df = df.where((col("year") >= 1950) & (col("year") <= 2014) & (col("temp") > 10))

# count
count_temp = filtered_df.groupBy("year", "month") \
                        .count().alias("count")

count_temp = count_temp.sort("count", ascending = False)

count_temp.write.csv("BDA/output/count")

# count distinct
count_temp = filtered_df.groupBy("year", "month") \
                        .agg(countDistinct("station_num").alias("count_dist"))

count_temp = count_temp.sort("count_dist", ascending = False)

count_temp.write.csv("BDA/output/count_distinct")
