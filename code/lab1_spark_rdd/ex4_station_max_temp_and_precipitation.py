#!/usr/bin/env python3
"""BDA1 Assignment 4 (Spark RDD).

Stations whose maximum temperature is between 25 and 30 degrees and whose
maximum daily precipitation is between 100 and 200 mm.
Input: temperature-readings.csv, precipitation-readings.csv
"""

from pyspark import SparkContext

#step 1 : create a SparkContext which tells Spark how to access a cluster
sc = SparkContext(appName = "exercise 1")

#step 2 : create distributed datasets
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
tem_lines = temperature_file.map(lambda line: line.split(";"))

precipitation_file= sc.textFile("BDA/input/precipitation-readings.csv")
pre_lines =precipitation_file.map(lambda line: line.split(";"))

#step 3 : RDD operation

#1.temperature:
#(key, value) = (station,temperature)
#(102170,6.3)
temperature = tem_lines.map(lambda x: ((x[0]), float(x[3])))
#get maximum temperature for stations
tem_max = temperature.reduceByKey(lambda a,b: max(a,b))
print("Top 10 max temp:")
print(tem_max.take(10))
#filter: temp[25,30]
tem_max_filter = tem_max.filter(lambda x: (x[1])>=25 and (x[1])<=30)

#2.precipitation:
#(key, value) = (station-date,precipitation)
#((103100-1995-08-01),0.0)
precipitation=pre_lines.map(lambda x: ((x[0],x[1]), float(x[3])))

#compute daily precipitation
precipitation=precipitation.reduceByKey(lambda a,b: a+b)
#converte to (station, precipitation)
precipitation=precipitation.map(lambda x : (x[0][0],x[1]))

#get maximum precipitation for stations
pre_max = precipitation.reduceByKey(lambda a,b: max(a,b))
print("Top 10 max precipitation:")
print(pre_max.take(10))
#filter: precipitation[100,200]
pre_max_filter =pre_max.filter(lambda x: (x[1])>=100 and (x[1])<=200)

joined_result=tem_max_filter.join(pre_max_filter)


print("Maximum temperature and precipitation:")
print(joined_result.take(10))

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
joined_result.saveAsTextFile("BDA/output/exercise4")
