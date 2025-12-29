from pyspark.sql import SparkSession, functions as f
from config import etl_settings
import os


def read_file_names():

    if etl_settings.EXEC_ENV == "emr":
        raw = os.path.join(etl_settings.S3_BUCKET, etl_settings.S3_RAW_KEY, etl_settings.FILE_NAME)
        processed = os.path.join(etl_settings.S3_BUCKET, etl_settings.S3_PRCSD_KEY, etl_settings.FILE_NAME)
    else:
        raw = os.path.join(etl_settings.LOCAL_RAW_DATA_PATH, etl_settings.FILE_NAME)
        processed = os.path.join(etl_settings.LOCAL_PRCSD_DATA_PATH, etl_settings.FILE_NAME)
    return (raw, processed)



spark = SparkSession.builder.appName("NYC_taxi_ETL").getOrCreate()
spark.sparkContext.setLogLevel("error")

src_data_path = "/home/ec2-user/NYC_taxi/data/raw/"
des_data_path = "/home/ec2-user/NYC_taxi/data/processed/"
file_name = "yellow_tripdata_2025-01.parquet"

raw_file, processed_file = read_file_names()

df = spark.read.parquet(raw_file)
df.printSchema()
df.show(10)
df = df.filter((f.col("fare_amount") != 0) & (f.col("PULocationID") != 0) & (f.col("DOLocationID") != 0))
df = df.withColumn("IsWeekend", f.when(f.dayofweek(f.col("tpep_pickup_datetime")).isin(1,7), 1).otherwise(0))

print(df.count())

df.write.mode("overwrite").parquet(processed_file)

spark.stop()