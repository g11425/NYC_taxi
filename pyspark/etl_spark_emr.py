from pyspark.sql import SparkSession, functions as f
from config import etl_settings as s
import os
import functools
from pyspark.sql.types import (StructType, StructField, StringType, LongType, IntegerType,
                               DoubleType, TimestampType)
from smart_open import open
import argparse


def parse_args():

    parser = argparse.ArgumentParser(description='ETL for NYC Taxi data')
    parser.add_argument('--run-date', type=str, required=True, help='Run date in YYYY-MM format')
    return parser.parse_args()




def read_file_names():

    if s.EXEC_ENV == "emr":
        raw = os.path.join(s.S3_BUCKET, s.S3_RAW_KEY, s.FILE_NAME)
        processed = os.path.join(s.S3_BUCKET, s.S3_PRCSD_KEY, s.FILE_NAME)
    else:
        raw = os.path.join(s.LOCAL_RAW_DATA_PATH, s.FILE_NAME)
        processed = os.path.join(s.LOCAL_PRCSD_DATA_PATH, s.FILE_NAME)
    return (raw, processed)

def read_extras_names():
    if s.EXEC_ENV == "emr":
        processed = os.path.join(s.S3_BUCKET, s.S3_EXTRAS)
    else:
        processed = os.path.join(s.LOCAL_PRCSD_DATA_PATH, s.LOCAL_EXTRAS ,s.FILE_NAME)
    return (processed)


def parse_lookups(file_name):
    out = {}
    with open(file_name, "r") as f:
        line = f.readline()
        for line in f:
            arr = line.split(",")
            print(arr)
            out[int(arr[0])] = arr[1]            
    return out

                
trips_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_code", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True)
])

spark = SparkSession.builder.appName("NYC_taxi_ETL").getOrCreate()
spark.sparkContext.setLogLevel("error")

src_data_path = "/home/ec2-user/NYC_taxi/data/raw/"
des_data_path = "/home/ec2-user/NYC_taxi/data/processed/"
file_name = "yellow_tripdata_2025-01.parquet"

raw_file, processed_file = read_file_names()
dq_stats = read_extras_names()

print(read_extras_names())

import_config = {
    "header":"true",
    "mode":"FAILFAST"
    }

df = spark.read.schema(trips_schema).options(**import_config).parquet(raw_file)
df.printSchema()
df.show(10)
df = df.filter((f.col("fare_amount") != 0) & (f.col("PULocationID") != 0) & (f.col("DOLocationID") != 0))
df = df.withColumn("IsWeekend", f.when(f.dayofweek(f.col("tpep_pickup_datetime")).isin(1,7), 1).otherwise(0))

print("s.S3_PAYMENT_TYPE_FILE:", s.S3_PAYMENT_TYPE_FILE)

paymenttype_dict = parse_lookups(s.S3_PAYMENT_TYPE_FILE)
ratecode_dict = parse_lookups(s.S3_RATE_CODE_FILE)
taxizone_dict = parse_lookups(s.S3_TAXI_ZONE_FILE)
vendor_dict = parse_lookups(s.S3_VENDOR_LOOKUP_FILE)

payment_type = spark.sparkContext.broadcast(paymenttype_dict)
rate_code = spark.sparkContext.broadcast(ratecode_dict)
taxi_zone = spark.sparkContext.broadcast(taxizone_dict)
vendor_name = spark.sparkContext.broadcast(vendor_dict)

def lookup_payment(lookup_dict, key):
    return lookup_dict.get(key)

udf_payment_type = f.udf(functools.partial(lookup_payment, payment_type.value))
udf_rate_code = f.udf(functools.partial(lookup_payment, rate_code.value))
udf_taxi_zone = f.udf(functools.partial(lookup_payment, taxi_zone.value))
udf_vendor_name = f.udf(functools.partial(lookup_payment, vendor_name.value))

df = df.withColumn("payment_type", udf_payment_type(f.col("payment_code")))\
    .withColumn("rate_type", udf_rate_code(f.col("RatecodeID")))\
    .withColumn("PULocation", udf_taxi_zone(f.col("PULocationID")))\
    .withColumn("DOLocation", udf_taxi_zone(f.col("DOLocationID")))\
    .withColumn("Vendor", udf_taxi_zone(f.col("VendorID")))


columns_to_hash = [
    "VendorID", 
    "tpep_pickup_datetime", 
    "tpep_dropoff_datetime", 
    "PULocationID"
]

df = df.withColumn("trip_id", f.sha2(f.concat_ws("||", **columns_to_hash), 256))

stats_df = df.agg(
    f.sum("total_amount").alias("total_amount"),
    f.count("VendorID").alias("row_count")
    )



print(df.count())

print("Writing processed data to: ", dq_stats)

stats_df.write.mode("overwrite").option("header", "true").csv(dq_stats)

print("Writing processed data to: ", processed_file)

df.write.mode("overwrite").parquet(processed_file)

df.printSchema()

spark.stop()