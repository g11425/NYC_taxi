import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field



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
    PROJECT_ROOT:str = os.path.dirname(os.path.abspath(__file__))
    EXEC_ENV:str = "emr" # emr or local
    DATA_STORE:str = "s3" # s3 or local

    S3_TEST_BUCKET:str = "s3-giam-bucket-002"
    S3_TEST_RAW_KEY:str = "NYC_taxi/raw/2025/01/"
    S3_RAW_TEST_FILE:str ="yellow_tripdata_2025-01.parquet"
    TEST_RAW_FILE_URL:str ='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet'
    S3_BUCKET_SIMPLE:str = "s3-giam-bucket-002"
    LOCAL_RAW_DATA_PATH:str = "/home/ec2-user/NYC_taxi/data/raw/"
    LOCAL_PRCSD_DATA_PATH:str = "/home/ec2-user/NYC_taxi/data/processed/"


    S3_RAW_KEY:str = "NYC_taxi/raw/"
    S3_PRCSD_KEY:str = "NYC_taxi/processed/"
    FILE_NAME:str = "yellow_tripdata_2025-01.parquet"
    ETL_PY_PATH_REL:str = "airflow"
    ETL_PY_FILE_NAME:str = "etl_spark_emr.py"
    S3_ETL_PY_PREFIX:str = "NYC_taxi"
    PAYMENT_TYPE_FILE:str = "paymenttype_lookup.csv"
    RATE_CODE_FILE:str = "ratecode_lookup.csv"
    TAXI_ZONE_FILE:str = "taxi_zone_lookup.csv"
    VENDOR_LOOKUP_FILE:str = "vendor_lookup.csv"
    S3_EXTRAS:str = "extras"
    LOCAL_EXTRAS:str = "extras"
    S3_EMR_BOOTSTRAP_SCRIPT_KEY:str = "NYC_taxi/emr_bootstrap.sh"
    RUN_DATE:str = "2025-01-01"
    REDSHIFT_WORKGROUP:str = "awsuser"
    REDSHIFT_DATABASE:str = "dev"
    DATA_HOST:str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    FILE_NAME_PATTERN:str = "yellow_tripdata_{{get_data_period(ds, '%Y-%m', -3)}}.parquet"
    FILE_NAME_TEMPLATE:str = "yellow_tripdata_yyyy-mm.parquet"



    @computed_field(return_type=str)
    @property
    def S3_BUCKET(self):
        if self.EXEC_ENV == "emr":
            return "s3://" + self.S3_BUCKET_SIMPLE
        else:
            return "s3a://" + self.S3_BUCKET_SIMPLE


    @computed_field(return_type=str)
    @property
    def S3_RAW_TEST_FILE_KEY(self):
        return os.path.join( self.S3_TEST_RAW_KEY, self.S3_RAW_TEST_FILE)
    
    @computed_field(return_type=str)
    @property
    def S3_STORE_PREFIX(self):
        return os.path.join( self.S3_BUCKET , self.S3_ETL_PY_PREFIX)

    @computed_field(return_type=str)
    @property
    def S3_PAYMENT_TYPE_FILE(self):
        return os.path.join(self.S3_STORE_PREFIX, self.PAYMENT_TYPE_FILE)

    @computed_field(return_type=str)
    @property
    def S3_RATE_CODE_FILE(self):
        return os.path.join(self.S3_STORE_PREFIX, self.RATE_CODE_FILE)

    @computed_field(return_type=str)
    @property
    def S3_TAXI_ZONE_FILE(self):
        return os.path.join(self.S3_STORE_PREFIX, self.TAXI_ZONE_FILE)

    @computed_field(return_type=str)
    @property
    def S3_VENDOR_LOOKUP_FILE(self):
        return os.path.join(self.S3_STORE_PREFIX, self.VENDOR_LOOKUP_FILE)

    @computed_field(return_type=str)

    @property
    def S3_CONF_FILE(self):
        return os.path.join(self.S3_BUCKET, self.S3_ETL_PY_PREFIX, "env")

    @computed_field(return_type=str)
    @property
    def S3_EMR_PY_FILE(self):
        return os.path.join(self.S3_BUCKET, self.S3_ETL_PY_PREFIX, "config.py")

    @computed_field(return_type=str)
    @property
    def FILE_NAME_RENDERED(self):
        arr =  self.RUN_DATE.split("-")
        return self.FILE_NAME_TEMPLATE.replace("yyyy", arr[0]).replace("mm", arr[1])


    @computed_field(return_type=tuple[str, str])
    @property
    def DATA_FILE_NAMES(self):


        arr =  self.RUN_DATE.split("-")
        print(self.RUN_DATE)
        print(arr)
        run_date = os.path.join(arr[0], arr[1])

        if self.DATA_STORE == "s3":
            raw = os.path.join(self.S3_BUCKET, self.S3_RAW_KEY, run_date, self.FILE_NAME_RENDERED)
            processed = os.path.join(self.S3_BUCKET, self.S3_PRCSD_KEY, run_date, self.FILE_NAME_RENDERED)
        else:
            raw = os.path.join(self.LOCAL_RAW_DATA_PATH, run_date, self.FILE_NAME_RENDERED)
            processed = os.path.join(self.LOCAL_PRCSD_DATA_PATH, run_date, self.FILE_NAME_RENDERED)
        return (raw, processed)

    @computed_field(return_type=str)
    @property
    def EXTRAS_FILE_NAMES(self):
        arr =  self.RUN_DATE.split("-")
        run_date = os.path.join(arr[0], arr[1])
        if self.DATA_STORE == "s3":
            processed = os.path.join(self.S3_BUCKET, self.S3_PRCSD_KEY, run_date, self.S3_EXTRAS)
        else:
            processed = os.path.join(self.LOCAL_PRCSD_DATA_PATH, run_date, self.LOCAL_EXTRAS, self.FILE_NAME_RENDERED)
        return (processed)


    @computed_field(return_type=str)
    @property
    def S3_EMR_PY_SCRIPT(self):
        return os.path.join(self.S3_BUCKET, self.S3_ETL_PY_PREFIX, self.ETL_PY_FILE_NAME)

    model_config = SettingsConfigDict(env_file=os.path.join("~/NYC_taxi/conf/", "env"), env_file_encoding="utf-8", extra='allow')
    
etl_settings = Settings()

def refresh_etl_settings(**kwargs)-> Settings: 
    global etl_settings
    new_data = etl_settings.model_dump()| kwargs
    print(new_data)
    etl_settings = etl_settings.model_validate(new_data)
    return etl_settings

def refresh_etl_settings1(**kwargs): 
    global etl_settings
    etl_settings = Settings(**kwargs)
    return etl_settings


print(etl_settings.model_dump())