from pyspark.sql import SparkSession, functions as f

spark = SparkSession.builder.appName("NYC_taxi_ETL").getOrCreate()

data_path = "G:/DE/Projects/NYC_taxi/data/"
file_name = "yellow_tripdata_2025-01.parquet"

df = spark.read.parquet(data_path + file_name)
df.printSchema()

spark.stop()