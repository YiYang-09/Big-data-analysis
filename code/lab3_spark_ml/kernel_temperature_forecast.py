#!/usr/bin/env python3
"""BDA3 (Spark RDD): kernel model for hourly temperature forecasts.

Predicts the temperature from 04:00 to 24:00 every 2 hours for a given date
and place in Sweden, combining three Gaussian kernels: distance between the
station and the point of interest, seasonal distance in days, and distance
in hours. The report compares the sum of the kernels with their product;
this script uses the product (the sum is left as a comment).
Input: temperature-readings.csv, stations.csv
"""

from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext

sc = SparkContext(appName="lab_kernel")

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

def gaussian_kernel(dist, h):
    return exp(-(dist ** 2) / (2 * (h ** 2)))

def hour_of_aday(time):
    return int(time.split(":")[0])

def seasonal_date_diff(date1, date2):
    doy1 = date1.timetuple().tm_yday
    doy2 = date2.timetuple().tm_yday
    diff = abs(doy1 - doy2)
    return min(diff, 365 - diff)

# Parameters
h_distance = 200000
h_date = 20
h_time = 2
a = 58.4274 # Latitude of point of interest
b = 14.826   # Longitude of point of interest
target_date = "2013-07-04"
targetTime = datetime.strptime(target_date, "%Y-%m-%d")

# Load and prepare stations
stations = sc.textFile("BDA/input/stations.csv")
stations_lines = stations.map(lambda line: line.split(";"))
stations_lines = stations_lines.map(lambda x: (
    x[0], (
        float(x[3]), # latitude
        float(x[4])   # longitude
    )))
stations_dict = dict(stations_lines.collect())
broadcast_stations = sc.broadcast(stations_dict)

# Load and process temperature data
temps = sc.textFile("BDA/input/temperature-readings.csv")
temp_lines = temps.map(lambda line: line.split(";"))
temp_lines = temp_lines.map(lambda x: (
    x[0], # station number
    datetime.strptime(x[1], "%Y-%m-%d"), # date
    x[2], # time
    float(x[3]), # temperature
    broadcast_stations.value.get(x[0], (None, None))[0], # lat
    broadcast_stations.value.get(x[0], (None, None))[1]   # lon
)).filter(lambda x: x[4] is not None)

# Filter readings before the target date
temp_lines = temp_lines.filter(lambda x: x[1] < targetTime)

# Compute physical and date kernels
def physical_date_kernels(x):
    distance = haversine(b, a, x[5], x[4])
    # date_diff = (targetTime - x[1]).days
    date_diff = seasonal_date_diff(targetTime, x[1])
    dist_kernel = gaussian_kernel(distance, h_distance)
    date_kernel = gaussian_kernel(date_diff, h_date)
    return (x[0], x[1], x[2], x[3], dist_kernel, date_kernel)

kernelCache = temp_lines.map(physical_date_kernels).cache()

def hour_kernels_factory(time_str):
    target_hour = int(time_str.split(":")[0]) % 24

    def hour_kernels(x):
        input_hour = int(x[2].split(":")[0]) % 24
        hour_diff = abs(target_hour - input_hour)
        if hour_diff > 12:
            hour_diff = 24 - hour_diff
        hour_kernel = gaussian_kernel(hour_diff, h_time)
        #combined_kernel = x[4] + x[5] + hour_kernel
        combined_kernel = x[4] * x[5] * hour_kernel # product of all kernels
        return (time_str, (x[3] * combined_kernel, combined_kernel))

    return hour_kernels


# Estimate temperature for different hours
results = []
for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
             "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:

    hour_kernels = hour_kernels_factory(time)
    mapped = kernelCache.map(hour_kernels)
    reduced = mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    estimated = reduced.mapValues(lambda x: x[0] / x[1] if x[1] != 0 else None)
    results.append(estimated)

# Combine results and output
joined_result = sc.union(results)

joined_result.saveAsTextFile("BDA/output/temperature_prediction")
