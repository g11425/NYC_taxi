import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
S3_TEST_BUCKET = "s3-giam-bucket-001"
S3_TEST_RAW_KEY = "NYC_taxi/raw/2025/01/"
S3_RAW_TEST_FILE="yellow_tripdata_2025-01.parquet"
S3_RAW_TEST_FILE_KEY=S3_TEST_RAW_KEY+S3_RAW_TEST_FILE
TEST_RAW_FILE_URL='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet'
