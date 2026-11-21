#!/usr/bin/env python3
"""BDA1 Assignment 1 (Spark RDD).

Lowest and highest temperature measured each year, 1950-2014,
sorted in descending order of temperature. Input: temperature-readings.csv
"""

from pyspark import SparkContext

#step 1 : create a SparkContext which tells Spark how to access a cluster
sc = SparkContext(appName = "exercise 1")

#step 2 : create distributed datasets
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

#step 3 : RDD operation
# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: (x[1][0:4], (x[0],float(x[3]))))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

#Get max
#max_temperatures = year_temperature.reduceByKey(lambda a,b: (a[0], max(a[1], b[1])))
#max_temperatures =year_temperature.reduceByKey(max)
max_temperatures = year_temperature.reduceByKey(lambda a,b: a if a[1]>= b[1] else b )
max_temperatures = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1][1])

#Get min
min_temperatures = year_temperature.reduceByKey(lambda a,b: a if a[1] <= b[1] else b )
min_temperatures = min_temperatures.sortBy(ascending = False, keyfunc= lambda k : k[1][1])


print(" Maxmimum : ")
print(max_temperatures.collect())

print("Minimum : ")
print(min_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
max_temperatures.saveAsTextFile("BDA/output")
min_temperatures.saveAsTextFile("BDA/output")
