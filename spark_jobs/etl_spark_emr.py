from pyspark.sql import SparkSession, functions as f
import config
import os
import functools
from pyspark.sql.types import (StructType, StructField, StringType, LongType, IntegerType,
                               DoubleType, TimestampType, DecimalType)
from pyspark.ml.feature import Bucketizer
from smart_open import open
import argparse
from datetime import datetime


def parse_args():

    parser = argparse.ArgumentParser(description='ETL for NYC Taxi data')
    parser.add_argument('--run-date', type=str, required=True, help='Run date in YYYY_MM format')
    return parser.parse_args()



def parse_lookups(file_name):
    out = {}
    with open(file_name, "r") as f:
        line = f.readline()
        for line in f:
            arr = line.split(",")
            print(arr)
            out[int(arr[0])] = arr[1]            
    return out

def lookup_payment(lookup_dict, key):
    return lookup_dict.get(key)


def create_dq_stats( df):
    stats = df.groupBy(f.col("Run_Date_Short")).agg(f.sum("total_amount").alias("total_amount"),
                                                      f.count("trip_id").alias("row_count"))
    stats = stats.select("Run_Date_Short",
                         f.expr("stack(2, 'total_amount', total_amount, 'row_count', CAST(row_count AS DOUBLE)) as (metric_name, metric_value)"))    
    return stats


def bucketize_distance(df, splits):
    bucketizer = Bucketizer(splits=splits, inputCol="trip_distance", outputCol="distance_bucket")
    df = bucketizer.setHandleInvalid("keep").transform(df)
    return df


if __name__ == "__main__":


    parser = parse_args()
    run_date = parser.run_date
    run_date_formatted = datetime.strptime(run_date, "%Y-%m-%d")
    run_date_short = "-".join(run_date.split("-")[:2])

    src_data_path = "/home/ec2-user/NYC_taxi/data/raw/"
    des_data_path = "/home/ec2-user/NYC_taxi/data/processed/"
    file_name = "yellow_tripdata_2025-01.parquet"

    s = config.refresh_etl_settings(RUN_DATE=run_date)

    raw_file, processed_file = s.DATA_FILE_NAMES
    dq_stats = s.EXTRAS_FILE_NAMES

    print(s.DATA_FILE_NAMES)

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


    import_config = {
        "header":"true",
        "mode":"FAILFAST"
        }
    

    df = spark.read.schema(trips_schema).options(**import_config).parquet(raw_file)
    df.printSchema()
    df.show(10)
    df = df.filter((f.col("fare_amount") != 0) & (f.col("PULocationID") != 0) & (f.col("DOLocationID") != 0))
    df = df.filter((f.col("trip_distance") > 0) & (f.col("trip_distance") < 200))

    df = df.withColumn("IsWeekend", f.when(f.dayofweek(f.col("tpep_pickup_datetime")).isin(1,7), 1).otherwise(0))
    df = df.withColumn("IsNight", f.when((f.hour(f.col("tpep_pickup_datetime")) >= 20) | (f.hour(f.col("tpep_pickup_datetime")) < 6), 1).otherwise(0))

    print("s.S3_PAYMENT_TYPE_FILE:", s.S3_PAYMENT_TYPE_FILE)

    paymenttype_dict = parse_lookups(s.S3_PAYMENT_TYPE_FILE)
    ratecode_dict = parse_lookups(s.S3_RATE_CODE_FILE)
    taxizone_dict = parse_lookups(s.S3_TAXI_ZONE_FILE)
    vendor_dict = parse_lookups(s.S3_VENDOR_LOOKUP_FILE)

    payment_type = spark.sparkContext.broadcast(paymenttype_dict)
    rate_code = spark.sparkContext.broadcast(ratecode_dict)
    taxi_zone = spark.sparkContext.broadcast(taxizone_dict)
    vendor_name = spark.sparkContext.broadcast(vendor_dict)


    udf_payment_type = f.udf(functools.partial(lookup_payment, payment_type.value))
    udf_rate_code = f.udf(functools.partial(lookup_payment, rate_code.value))
    udf_taxi_zone = f.udf(functools.partial(lookup_payment, taxi_zone.value))
    udf_vendor_name = f.udf(functools.partial(lookup_payment, vendor_name.value))

    df = df.withColumn("payment_type", udf_payment_type(f.col("payment_code")))\
        .withColumn("rate_type", udf_rate_code(f.col("RatecodeID")))\
        .withColumn("PULocation", udf_taxi_zone(f.col("PULocationID")))\
        .withColumn("DOLocation", udf_taxi_zone(f.col("DOLocationID")))\
        .withColumn("Vendor", udf_taxi_zone(f.col("VendorID")))\
        .withColumn("Run_Date", f.lit(run_date_formatted))\
        .withColumn("Run_Date_Short", f.lit(run_date_short))


    columns_to_hash = [
        "VendorID", 
        "tpep_pickup_datetime", 
        "tpep_dropoff_datetime", 
        "PULocationID"
    ]

    df = df.withColumn("trip_id", f.sha2(f.concat_ws("||", *columns_to_hash), 256))

    df = bucketize_distance(df, splits=[-float("inf"), 0, 1, 3, 5, 10, 20, 40, 60, 200, float("inf")])


    df.select(f.max(f.col("trip_distance"))).show(10)

    bucket_test = df.groupBy("distance_bucket").agg(f.count("trip_id")).orderBy(f.col("distance_bucket").desc())

    bucket_test.show()

    df_check  = df.orderBy(f.col("trip_distance").desc()).limit(20)

    df_check.show()

    stats_df = create_dq_stats(df)

    print(df.count())

    print("Writing processed data to: ", dq_stats)

    stats_df.write.mode("overwrite").option("header", "true").csv(dq_stats)

    print("Writing processed data to: ", processed_file)

    df.write.mode("overwrite").parquet(processed_file)

    df.printSchema()

    spark.stop()