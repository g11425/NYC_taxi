#!bin/bassh

source ~/etl/bin/activate
export PYTHONPATH=/home/ec2-user/NYC_taxi

spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"  etl_spark_emr.py --run-date 2025-01-01
