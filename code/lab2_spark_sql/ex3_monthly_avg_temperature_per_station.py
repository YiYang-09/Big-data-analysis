#!/usr/bin/env python3
"""BDA2 Assignment 3 (Spark SQL / DataFrame API).

Average monthly temperature for each station, 1960-2014.
Input: temperature-readings.csv
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, split, avg, max as spark_max, min as spark_min, countDistinct, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# create a SparkContext which tells Spark how to access a cluster
spark = SparkSession.builder.appName("exercise 3").getOrCreate()
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

# filter years
filtered_df = df.where((col("year") >= 1960) & (col("year") <= 2014))

# average monthly temperature per station
avg_temp = filtered_df.groupBy("station_num", "year", "month") \
                      .agg(avg("temp").alias("avg_temp"))

avg_temp = avg_temp.sort("avg_temp", ascending = False)

# save result
avg_temp.write.csv("BDA/output/avg_temp")
