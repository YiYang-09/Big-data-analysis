# Big Data Analysis — Lab Reports

Lab reports for **732A54 / TDDE31 Big Data Analysis** at Linköping University. All analyses are written in **PySpark** and executed on the **Sigma 1 HPC cluster** (NSC), using Swedish climate open data (station temperature readings and precipitation).

| Lab | Report | Topic |
|---|---|---|
| 1 | [BigDataAnalysis_BDA1.pdf](BigDataAnalysis_BDA1.pdf) | Spark analytics: yearly min/max temperatures 1950–2014, per-station and per-month aggregations, monthly precipitation for Östergötland |
| 2 | [BigDataAnalysis_BDA2.pdf](BigDataAnalysis_BDA2.pdf) | The same assignment set **redone in Spark SQL** — DataFrame API vs. SQL queries |
| 3 | [BigDataAnalysis_BDA3.pdf](BigDataAnalysis_BDA3.pdf) | Machine learning in PySpark: a kernel model predicting hourly temperature for a given date and location in Sweden |

## Tech Stack

| Layer | Technology | Used for |
|---|---|---|
| Language | Python (PySpark) | All lab code |
| Distributed processing | **Apache Spark** — RDD API (`SparkContext`) | Lab 1: `map` / `filter` / `reduceByKey` / `sortBy` pipelines, joins and broadcast variables |
| Structured processing | **Spark SQL / DataFrame API** (`SparkSession`) | Lab 2: `groupBy` / `agg` / `join`, explicit schemas via `StructType` |
| Machine learning | Kernel regression implemented by hand on RDDs | Lab 3: sum and product of three Gaussian kernels (distance, date, hour) |
| Python stdlib | `datetime`, `math` | Lab 3: date differences and haversine distance between stations |
| Storage | **HDFS** | Input CSVs (`temperature-readings.csv`, `precipitation-readings.csv`, `stations.csv`) and job output |
| Compute | Sigma 1 HPC cluster (NSC) | Job execution |
| Reporting | LaTeX | The PDF lab reports |

**Data:** Swedish climate open data from SMHI — semicolon-separated CSV files of station temperature and precipitation readings.

## Code

The PySpark scripts from the reports are in [`code/`](code):

| Folder | Contents |
|---|---|
| [`code/lab1_spark_rdd`](code/lab1_spark_rdd) | Assignments 1–5 with the Spark RDD API |
| [`code/lab2_spark_sql`](code/lab2_spark_sql) | The same assignments with Spark SQL / DataFrames |
| [`code/lab3_spark_ml`](code/lab3_spark_ml) | Kernel model for hourly temperature forecasts |

The scripts read their input from HDFS (`BDA/input/...`) and were run on the Sigma cluster, for example with `spark-submit code/lab1_spark_rdd/ex1_yearly_min_max_temperature.py`. The data files are not included in this repository.

The PDF reports contain the results and written answers.

[`LabCompendium/`](LabCompendium) contains the official course lab instructions, including cluster setup and a Spark SQL introduction.
