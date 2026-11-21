#!/usr/bin/env python3
"""BDA1 Assignment 5 (Spark RDD).

Average monthly precipitation for the Östergötland region, 1993-2016.
The small station list is broadcast to all nodes instead of joined.
Input: precipitation-readings.csv, stations-Ostergotland.csv
"""

from pyspark import SparkContext

#step 1 : create a SparkContext which tells Spark how to access a cluster
sc = SparkContext(appName = "exercise 1")

#step 2 : create distributed datasets
# This path is to the file on hdfs
stations_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
stations_lines = stations_file.map(lambda line: line.split(";"))
stations_lines = stations_lines.map(lambda x: x[0])         #(station,1)
stations_lines = stations_lines.collect()
#set and broadcast stations-Ostergotland.csv
stations_set = set(stations_lines)
broadcast_stations = sc.broadcast(stations_set)


precipitation_file= sc.textFile("BDA/input/precipitation-readings.csv")
pre_lines =precipitation_file.map(lambda line: line.split(";"))

#filter precipitation
#stations belong to Ostergotland
pre_filtered = pre_lines.filter(lambda x : x[0] in broadcast_stations.value)
pre_filtered = pre_filtered.map(lambda x: ((x[0],x[1][0:4],x[1][5:7]),float(x[3]))) #[(103100,1995,08),0.0]
#years within [1993-2016]
pre_filtered = pre_filtered.filter(lambda x: 1993<=int(x[0][1])<=2016)


#sum of monthly precipitation
stations_monthlyPre = pre_filtered.reduceByKey(lambda a,b:a+b) #[(station,year,month),StationMonthPrecipitation]
#convert to [(year,month),StationMonthPrecipitation] ,each Key-value pair represent one station
stations_monthlyPre = stations_monthlyPre.map(lambda x: ((x[0][1],x[0][2]),x[1])) #[(year,month),StationMonthPrecipitation]

# compute total precipitation per month and sum up the number of stations

zeroValue = (0.0,0)         #initial value(total precipitation:0,num of stations:0), acc
seq_func = lambda acc,val: (acc[0]+val,acc[1]+1)    #acc:(total precipitation:0,num of stations:0),val:the new precipitation to be aggregated
comb_func = lambda acc1,acc2: (acc1[0]+acc2[0],acc1[1]+acc2[1]) #(sum1,count1),(sum2,count2) sum up
sum_count_perMonth = stations_monthlyPre.aggregateByKey(
    zeroValue,
    seq_func,
    comb_func
)

average_prec_permonth =sum_count_perMonth.mapValues(lambda x: x[0]/x[1])
average_prec_permonth = average_prec_permonth.sortByKey()


print("average precipitation:")
print(average_prec_permonth.take(10))

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
average_prec_permonth.saveAsTextFile("BDA/output/exercise5")
