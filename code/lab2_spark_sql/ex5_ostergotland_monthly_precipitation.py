#!/usr/bin/env python3
"""BDA2 Assignment 5 (Spark SQL / DataFrame API).

Average monthly precipitation for the Östergötland region, 1993-2016:
total monthly precipitation per station, then averaged over stations.
Input: precipitation-readings.csv, stations-Ostergotland.csv
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, split, avg, max as spark_max, min as spark_min, countDistinct, count, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# create a SparkContext which tells Spark how to access a cluster
spark = SparkSession.builder.appName("exercise 5").getOrCreate()
sc = spark.sparkContext

# create distributed datasets
# This path is to the file on hdfs

# precipitation file
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
prec_lines =precipitation_file.map(lambda line: line.split(";"))

prec_lines = prec_lines.map(lambda x: (
    x[0],                   # stat_num
    int(x[1][0:4]),         # year
    int(x[1][5:7]),         # month
    x[2],                   # time
    float(x[3]),            # precipitation (mm)
    x[4]                    # quality
    ))

# precipitation dataset schema
prec_schema = StructType([
    StructField("station_num", StringType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("time", StringType()),
    StructField("prec", FloatType()),
    StructField("quality", StringType()),
])
df_prec = spark.createDataFrame(prec_lines, schema = prec_schema)

# stations Ostergotland dataset
oster_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
oster_lines = oster_file.map(lambda line: line.split(";"))

oster_lines = oster_lines.map(lambda x: (
    x[0],                   # stat_num
    x[1]                    # stat_name
    ))

# stations Ostergotland dataset schema
oster_schema = StructType([
    StructField("station_num", StringType()),
    StructField("station_name", StringType()),
])
df_oster = spark.createDataFrame(oster_lines, schema = oster_schema)

# filter years
filtered_df = df_prec.where((col("year") >= 1993) & (col("year") <= 2016))

# get only Ostergotland stations
stat_oster = filtered_df.join(df_oster, ["station_num"])

# average precipitation per station
avg_prec = stat_oster.groupBy("station_num", "year", "month") \
                      .agg(sum("prec").alias("avg_prec"))

# average precipitation per month
avg_prec_month = avg_prec.groupBy("year", "month") \
                .agg(avg("avg_prec").alias("avg_prec_month")) \
                .orderBy(["year", "month"], ascending = False)

# save results
avg_prec_month.write.csv("BDA/output/avg_prec")
