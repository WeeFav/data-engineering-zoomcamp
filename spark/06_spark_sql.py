#!/usr/bin/env python
# coding: utf-8

# 1. Go to %SPARK_HOME%\bin or .venv\Lib\site-packages\pyspark\bin
# 2. Run spark-class org.apache.spark.deploy.master.Master
# 3. Run spark-class org.apache.spark.deploy.worker.Worker spark://ip:port

### python .\06_spark_sql.py --input_green .\data\green_tripdata_2021-01\ --input_yellow .\data\yellow_tripdata_2021-01\ --output ./data/tmp/
### spark-submit --master="spark://10.165.13.137:7077" D:\data-engineering-zoomcamp\spark\06_spark_sql.py --input_green D:\data-engineering-zoomcamp\spark\data\green_tripdata_2021-01 --input_yellow D:\data-engineering-zoomcamp\spark\data\yellow_tripdata_2021-01 --output D:\data-engineering-zoomcamp\spark\data\tmp
# --input_green=gs://spark-zoomcamp-nyc-taxi/green_tripdata_2021-01 
# --input_yellow=gs://spark-zoomcamp-nyc-taxi/yellow_tripdata_2021-01 
# --output=gs://spark-zoomcamp-nyc-taxi/tmp

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

# Without using spark submit
# spark = SparkSession.builder \
#     .master("spark://10.165.13.137:7077") \
#     .appName('test') \
#     .getOrCreate()
    
spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

df_green = spark.read.parquet(input_green)
df_green.show()

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = spark.read.parquet(input_yellow)

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

common_colums = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]

df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.registerTempTable('trips_data')

df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

df_result.coalesce(1) \
    .write.parquet(output, mode='overwrite')