from pyspark.sql import SparkSession, functions as f

spark = SparkSession.builder.appName("NYC_taxi_ETL").getOrCreate()
spark.sparkContext.setLogLevel("error")

src_data_path = "/home/ec2-user/NYC_taxi/data/raw/"
des_data_path = "/home/ec2-user/NYC_taxi/data/processed/"
file_name = "yellow_tripdata_2025-01.parquet"

df = spark.read.parquet(src_data_path + file_name)
df.printSchema()
df.show(10)
df = df.filter((f.col("fare_amount") != 0) & (f.col("PULocationID") != 0) & (f.col("DOLocationID") != 0))
df = df.withColumn("IsWeekend", f.when(f.dayofweek(f.col("tpep_pickup_datetime")).isin(1,7), 1).otherwise(0))

print(df.count())

df.write.mode("overwrite").parquet(des_data_path+file_name)

spark.stop()