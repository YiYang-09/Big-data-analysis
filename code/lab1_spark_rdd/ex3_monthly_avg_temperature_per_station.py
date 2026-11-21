#!/usr/bin/env python3
"""BDA1 Assignment 3 (Spark RDD).

Average monthly temperature for each station, 1960-2014. The daily average
is the mean of the daily max and min. Input: temperature-readings.csv
"""

from pyspark import SparkContext

#step 1 : create a SparkContext which tells Spark how to access a cluster
sc = SparkContext(appName = "exercise 1")

#step 2 : create distributed datasets
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

#step 3 : RDD operation
# (key, value) = (station-year-month-day,temperature)
#((102170,2013-11-01),6.3)

temperature = lines.map(lambda x: ((x[0],x[1]), float(x[3])))

#filter: year[1960,2014]
temperature = temperature.filter(lambda x: int(x[0][1][0:4])>=1960 and int(x[0][1][0:4])<=2014)

#get daily maximum and minimum
daily_max = temperature.reduceByKey(lambda a,b: max(a,b))
daily_min = temperature.reduceByKey(lambda a,b : min(a,b))

#((station,date),max,min)
daily_avg = daily_max.join(daily_min)
#daily average: (station,date),average_temperature)
daily_avg = daily_avg.map(lambda x: (x[0],((x[1][0]+x[1][1])/2)))

#monthly temperature
#((station,year,month),(avg,1))
month = daily_avg.map(lambda x:((x[0][0],x[0][1][0:4],x[0][1][5:7]),(x[1],1)))
#count the number of days and temperature: ((station,year,month),(sum_temp,sum_dates))
month_sum =month.reduceByKey(lambda a,b : (a[0]+b[0],a[1]+b[1]))
#average temp of a month
month_avg = month_sum.map(lambda x: ((x[0][1],x[0][2],x[0][0]),round(x[1][0]/x[1][1],2)))


print("Average temperature:")
print(month_avg.take(10))

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
month_avg.saveAsTextFile("BDA/output/exercise3")
