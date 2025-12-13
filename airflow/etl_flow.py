from datetime import datetime, timedelta
import logging
import os.path
from urllib import response

# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow.sdk import DAG
import os
from urllib.request import urlretrieve
import boto3

from airflow.FileOps import FileOps
from config import PROJECT_ROOT


def upload_to_S3_raw(fileOps:FileOps, file_name, **kwargs):

    bucket_name = "s3-giam-bucket-001"
    save_key = "NYC_taxi/raw/2025/01/"

    s3 = boto3.client("s3")

    try:
        response = s3.upload_file(Filename= os.path.join(fileOps.data_path_raw, file_name),
                                  Bucket=bucket_name,
                                  Key=save_key + file_name)
        return True
    except Exception as e:
        logging.exception("error occured while uploading")
        return False


def upload_to_s3_processed(fileOps:FileOps, file_name, **kwargs):
    bucket_name = "s3-giam-bucket-001"
    save_key = "NYC_taxi/processed/2025/01/"

    s3 = boto3.client("s3")

    try:

        for dir, sub_dir, files in os.walk(os.path.join(fileOps.data_path_processed,file_name)):
            for file in files:
                file_name = os.path.join(dir, file)
                key = os.path.join(save_key, os.path.relpath(file_name,fileOps.data_path_processed))
                response = s3.upload_file(Filename= file_name,
                                          Bucket=bucket_name,
                                          Key= key)
        return True
    except Exception as e:
        logging.exception("error occured while uploading")
        return False

def upload_processed(**kwargs):
    file_name = "yellow_tripdata_2025-01.parquet"
    f = FileOps(PROJECT_ROOT)
    upload_to_s3_processed(fileOps=f, file_name=file_name)

def fetch_data(**kwargs):

    file_name = "yellow_tripdata_2025-01.parquet"
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet'

    f = FileOps(PROJECT_ROOT)

    try:

        f.setup_data_paths()
        
        urlretrieve(url, os.path.join(f.data_path_raw, file_name))
        print(f.data_path_raw)
        if upload_to_S3_raw(fileOps=f, file_name=file_name):
            print("successfully saved data")
            return True
        else:
            print("failed. Data not saved to S3")
        return False
    except Exception as e:
        logging.exception("fetch_data: failed")
        return False



with DAG(
    dag_id="NYC_taxi_flow",
    default_args={
        "depends_on_past":False,
        "retries": 1,
        "retry_delay":timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
        }
    ) as dag:


    # t1 = BashOperator(
    #     task_id="download_NYC_taxi_data",
    #     bash_command="""wget -O /home/ec2-user/NYC_taxi/data/raw/yellow_tripdata_2025-01.parquet 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet' \

    #     aws s3 cp /home/ec2-user/NYC_taxi/data/raw/ s3://s3-giam-bucket-001/NYC_taxi/raw/2025/01/ --recursive"""
    #     )
    t2 = BashOperator(
        task_id = "perform_etl",
        bash_command="spark-submit /home/ec2-user/NYC_taxi/pyspark/etl_spark.py"
        )
    # t3 = BashOperator(
    #     task_id = "save_processed",
    #     bash_command="aws s3 cp /home/ec2-user/NYC_taxi/data/processed/ s3://s3-giam-bucket-001/NYC_taxi/processed/2025/01/ --recursive"
    #     )
    t4 = PythonOperator(
        task_id="python_fetch_data",
        python_callable=fetch_data)
    t5 = PythonOperator(
        task_id="python_save_processed",
        python_callable=upload_processed)

    t4 >> t2 >> t5
