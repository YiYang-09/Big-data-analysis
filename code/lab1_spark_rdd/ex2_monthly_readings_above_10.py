#!/usr/bin/env python3
"""BDA1 Assignment 2 (Spark RDD).

Count readings above 10 degrees for each month in 1950-2014, then count
again taking only distinct stations per month. Input: temperature-readings.csv
"""

from pyspark import SparkContext

#step 1: create a SparkContext which tells Spark how to access a cluster
sc = SparkContext(appName = "exercise 1")

# Step 2: Create distributed datasets
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

#step 3: RDD operation
# (key, value) = (year,temperature)
#year_temperature = lines.map(lambda x: (x[1][0:4], (x[0],float(x[3]))))

yearMonth_temperature = lines.map(lambda x: (x[1][0:7], (x[0],float(x[3]))))

#filter: year[1950,2014],temp>10
yearMonth_temperature = yearMonth_temperature.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014)
yearMonth_temperature = yearMonth_temperature.filter(lambda x : x[1][1]> 10)

#1.
#(year-month,1)
yearMonth = yearMonth_temperature.map(lambda x : (x[0],1))
#count per year
count=yearMonth.reduceByKey(lambda a,b: a+b)


#2.
#count distinct readings from each station per(year, month)
yearMonth_distinct = yearMonth_temperature.map(lambda x: (x[0], x[1][0]))
#using distinct by stationn_id for each month
yearMonth_distinct =yearMonth_distinct.distinct()
#(year-month,1)
yearMonth_distinct=yearMonth_distinct.map(lambda x : (x[0],1))
#count per year
count_distinct=yearMonth_distinct.reduceByKey(lambda a,b: a+b)



print("Count of readings(temperature higher than 10 degree) per month: ")
print(count.collect())

print("Count of distinct readings(temperature higher than 10 degree) per month:")
print(count_distinct.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
count.saveAsTextFile("BDA/output/task1")
count_distinct.saveAsTextFile("BDA/output/task2")
