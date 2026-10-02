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

> This repository contains only the PDF reports (which embed the code and results) and the course instructions, not standalone source files.

[`LabCompendium/`](LabCompendium) contains the official course lab instructions, including cluster setup and a Spark SQL introduction.
