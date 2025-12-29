import os
from pydantic_settings import BaseSettings, SettingsConfigDict



EXEC_ENV = "emr" # emr or local
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
S3_TEST_BUCKET = "s3-giam-bucket-001"
S3_TEST_RAW_KEY = "NYC_taxi/raw/2025/01/"
S3_RAW_TEST_FILE="yellow_tripdata_2025-01.parquet"
S3_RAW_TEST_FILE_KEY=S3_TEST_RAW_KEY+S3_RAW_TEST_FILE
TEST_RAW_FILE_URL='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet'
S3_BUCKET = "s3-giam-bucket-001"
LOCAL_RAW_DATA_PATH = "/home/ec2-user/NYC_taxi/data/raw/"
LOCAL_PRCSD_DATA_PATH = "/home/ec2-user/NYC_taxi/data/processed/"

S3_RAW_KEY = "NYC_taxi/raw/2025/01/"
S3_PRCSD_KEY = "NYC_taxi/processed/2025/01/"
FILE_NAME = "yellow_tripdata_2025-01.parquet"

class Settings(BaseSettings):
    S3_RAW_TEST_FILE_KEY:str =S3_TEST_RAW_KEY+S3_RAW_TEST_FILE
    PROJECT_ROOT:str = os.path.dirname(os.path.abspath(__file__))

    model_config = SettingsConfigDict(env_file="conf/.env", env_file_encoding="utf-8", extra='allow')
    
etl_settings = Settings()

print(etl_settings.model_dump())