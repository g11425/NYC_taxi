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
from botocore.exceptions import ClientError
import requests
# from airflow.providers.amazon.aws.operators.redshift_sql import RedshfitSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator

from airflow.FileOps import FileOps
from config import PROJECT_ROOT



S3_EMR_PY_SCRIPT = "s3://s3-giam-bucket-001/NYC_taxi/etl_spark_emr.py"


def upload_to_s3(bucket_name, save_key, file_name, file_path):
    
    s3 = boto3.client("s3")

    try:
        response = s3.upload_file(Filename= os.path.join(file_path, file_name),
                                  Bucket=bucket_name,
                                  Key=save_key + file_name)
        return True
    except Exception as e:
        logging.exception("error occured while uploading")
        return False


def check_if_exists_s3(bucket, key):
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise

def delete_s3(bucket, key):
    s3 = boto3.client("s3")
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        logging.exception(e)
        return False
        raise

def fetch_to_s3(file_name, url, bucket_name, save_key):
    # file_name = "yellow_tripdata_2025-01.parquet"
    # url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet'

    # bucket_name = "s3-giam-bucket-001"
    # save_key = "NYC_taxi/raw/2025/01/"


    s3 = boto3.client("s3")
    
    try:

        with requests.get(url=url, stream=True) as r:
            r.raise_for_status()
            s3.upload_fileobj(
                r.raw,
                bucket_name,
                save_key + file_name
                )

    except Exception as e:
        logging.exception(e)
        raise

def task_fetch_to_s3(**kwargs):
    file_name = "yellow_tripdata_2025-01.parquet"
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet'

    bucket_name = "s3-giam-bucket-001"
    save_key = "NYC_taxi/raw/2025/01/"
    fetch_to_s3(file_name, url, bucket_name, save_key)




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
    # t2 = BashOperator(
    #     task_id = "perform_etl",
    #     bash_command="spark-submit /home/ec2-user/NYC_taxi/pyspark/etl_spark.py"
    #     )
    # t3 = BashOperator(
    #     task_id = "save_processed",
    #     bash_command="aws s3 cp /home/ec2-user/NYC_taxi/data/processed/ s3://s3-giam-bucket-001/NYC_taxi/processed/2025/01/ --recursive"
    #     )
    # t4 = PythonOperator(
    #     task_id="python_fetch_data",
    #     python_callable=fetch_data)

    # t5 = PythonOperator(
    #     task_id="python_save_processed",
    #     python_callable=upload_processed)

    # t6 = RedshfitSQLOperator(
    #     task_id="copy_to_redshfit",
    #     redshift_conn_id="redshfit_default",
    #     aws_conn_id="aws_default",
    #     sql=f"""
    #     COPY dev.test_table
    #     FROM 's3-giam-bucket-001/NYC_taxi/processed/2025/01/'
    #     FORMAT AS PARQUET
    #     STATUPDATE ON;
    #     """
    #     )

    t7 = S3ToRedshiftOperator(
        task_id="copy_to_redshfit",
        redshift_conn_id="redshfit_default",
        aws_conn_id="aws_default",
        table="yellow_taxi_trips",
        s3_bucket="s3-giam-bucket-001",
        s3_key="NYC_taxi/raw/2025/01/",
        method="REPLACE", #APPEND. UPSERT, REPLACE
        schema="public",
        copy_options=["parquet"]
        )

    JOB_FLOW_OVERRIDES = {
        "Name": "NYC_taxi_cluster",
        "ReleaseLabel": "emr-7.12.0",
        "Applications": [{"Name": "Spark"}],
        "Instances": {
            "InstanceGroups": [
                {"Name": "Master", "Market": "ON_DEMAND", "InstanceRole": "MASTER", "InstanceType": "r8g.xlarge", "InstanceCount": 1},
                {"Name": "Core", "Market": "ON_DEMAND", "InstanceRole": "CORE", "InstanceType": "r8g.xlarge", "InstanceCount": 2},
            ],
            "KeepJobFlowAliveWhenNoSteps": True, # Important: Don't kill cluster before we add steps!
            "TerminationProtected": False,
        },
        "JobFlowRole": "EC2_NYC_taxi",
        "ServiceRole": "EMR_service",
    }

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        region_name="eu-north-1"

        # emr_conn_id="emr_default",
    )

    # Task B: Add the Step (points to Task A for Cluster ID)


    SPARK_STEPS = [
        {
            'Name':'Spark_ETL_EMR',
            'ActionOnFailure':"CONTINUE",
            'HadoopJarStep':{
                'Jar':'command-runner.jar',
                'Args':['spark-submit', 
                        '--deploy-mode', 'cluster',
                        S3_EMR_PY_SCRIPT]
                }
            }
        ]


    add_step = EmrAddStepsOperator(
        task_id="add_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        steps=SPARK_STEPS,
        aws_conn_id="aws_default",
        region_name="eu-north-1"

    )

    # Task C: Wait for Step Completion (points to Task B for Step ID)
    wait_for_step = EmrStepSensor(
        task_id="wait_for_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        # EmrAddStepsOperator returns a LIST of IDs, so we grab the first one [0]
        step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        region_name="eu-north-1"
    )

    # Task D: Terminate Cluster (Cleanup)
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done", # Run even if the step failed
        region_name="eu-north-1"
    )


    # t8 = EmrAddStepsOperator(
    #     task_id="add_ETL_step",
    #     job_flow_id="j-294F5S1L47QLY",
    #     steps=SPARK_STEPS,
    #     region_name="eu-north-1"
    #     )


    t9 = PythonOperator(
        task_id="fetch_data_to_s3",
        python_callable=task_fetch_to_s3)


    t9 >> create_cluster >> add_step >> wait_for_step >> terminate_cluster >> t7



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

