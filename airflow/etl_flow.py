from datetime import datetime, timedelta
# Operators; we need this to operate!
from airflow.providers.standard.operators.bash import BashOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow.sdk import DAG

with DAG(
    "NYC_taxi_flow",
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
    t1 = BashOperator(
        task_id="download_NYC_taxi_data",
        bash_command="""wget -O /home/ec2-user/NYC_taxi/data/raw/yellow_tripdata_2025-01.parquet 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet' \

        aws s3 cp /home/ec2-user/NYC_taxi/data/raw/ s3://s3-giam-bucket-001/NYC_taxi/raw/2025/01/ --recursive"""
        )
    t2 = BashOperator(
        task_id = "perform_etl",
        bash_command="spark-submit /home/ec2-user/NYC_taxi/pyspark/etl_spark.py"
        )
    t3 = BashOperator(
        task_id = "save_processed",
        bash_command="aws s3 cp /home/ec2-user/NYC_taxi/data/processed/ s3://s3-giam-bucket-001/NYC_taxi/processed/2025/01/ --recursive"
        )

    t1 >> t2 >> t3
