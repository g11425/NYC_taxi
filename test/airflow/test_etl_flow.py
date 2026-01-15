import airflow.etl_flow as flw
import config as tc
from airflow.FileOps import FileOps
from datetime import datetime

import shutil
import os
import pytest

from airflow.models import TaskInstance, DagBag
from airflow.utils.state import State   

def test_fetch_data():
    f = FileOps(tc.PROJECT_ROOT)
    if(f.check_raw_path()):
        shutil.rmtree(f.data_path_raw)
    file_name = "yellow_tripdata_2025-01.parquet"
    flw.fetch_data()
    full_file_name = os.path.join( f.data_path_raw, file_name)
    assert os.path.exists(full_file_name) == True



@pytest.fixture
def set_up_dummy_raw():
    f = FileOps(tc.PROJECT_ROOT)
    f.create_dummy_raw()
    yield f
    f.clean_dummy_raw()

def test_upload_to_s3_raw(set_up_dummy_raw):
	f = set_up_dummy_raw
	assert flw.fetch_data() == True
	assert flw.upload_to_S3_raw(fileOps=f, file_name = f.test_file_raw) == True

@pytest.fixture
def set_up_dummy_processed():
    f = FileOps(tc.PROJECT_ROOT)
    f.create_dummy_processed()
    yield f
    f.clean_dummy_processed()

def test_upload_to_s3_processed(set_up_dummy_processed):
    f = set_up_dummy_processed
    assert flw.upload_to_s3_processed(fileOps= f, file_name="") == True

def test_upload_to_s3(set_up_dummy_raw):
    f = set_up_dummy_raw
    flw.upload_to_s3(tc.S3_TEST_BUCKET, tc.S3_TEST_RAW_KEY, f.test_file_raw, f.data_path_raw)
    assert flw.check_if_exists_s3(tc.S3_TEST_BUCKET, tc.S3_TEST_RAW_KEY + f.test_file_raw)

def test_delete_s3(set_up_dummy_raw):
    f = set_up_dummy_raw
    flw.delete_s3(tc.S3_TEST_BUCKET, tc.S3_TEST_RAW_KEY + f.test_file_raw)
    assert flw.check_if_exists_s3(tc.S3_TEST_BUCKET, tc.S3_TEST_RAW_KEY + f.test_file_raw) == False


def test_fetch_to_s3():
    flw.fetch_to_s3(tc.S3_RAW_TEST_FILE, tc.TEST_RAW_FILE_URL, tc.S3_TEST_BUCKET, tc.S3_TEST_RAW_KEY)
    assert flw.check_if_exists_s3(tc.S3_TEST_BUCKET, tc.S3_RAW_TEST_FILE_KEY)
    flw.delete_s3(tc.S3_TEST_BUCKET, tc.S3_RAW_TEST_FILE_KEY)

def test_get_data_period():
    assert flw.get_data_period_ym("2025-01-01") == "2025_01"
    assert flw.get_data_period_ym("2025-01-01", months_offset=1) == "2025_02"
    assert flw.get_data_period_ym("2025-12-01", months_offset=1) == "2026_01"
    assert flw.get_data_period_ym("2025-01-01", months_offset=-1) == "2024_12"
    assert flw.get_data_period_ym(datetime(2025, 1, 1), months_offset=-2) == "2024_11"


def test_redshift_create_table_sql():
    op = flw.dag.get_task("redshift_create_table")
    
    # 2. Manually define the "Context" that Jinja needs
    # This mimics what Airflow generates during a real run
    logical_date = datetime(2025, 3, 1)
    context = {
        "dag": flw.dag,
        "ds": logical_date.strftime("%Y-%m-%d"),
        "ds_nodash": logical_date.strftime("%Y%m%d"),
        "logical_date": logical_date,
        "params": {},
        "task": op,
    }

    # 3. Use the Task's own renderer directly (Bypassing TaskInstance init issues)
    op.render_template_fields(context)
    
    # 4. Verify your logic
    print(f"Rendered SQL: {op.sql}")
    assert "yellow_taxi_trips_2024_12" in op.sql


def test_fetch_to_s3_task():
    dag_bag = DagBag()
    dag = dag_bag.get_dag("NYC_taxi_flow")
    task = dag.get_task("fetch_data_to_s3")
    dag.user_defined_macros = {'get_data_period': flw.get_data_period}

    logical_date = datetime(2025, 3, 1)
    context = {
        "ds": logical_date.strftime("%Y-%m-%d"),
        "ds_nodash": logical_date.strftime("%Y%m%d"),
        "logical_date": logical_date,
        "params": {}
        }

    task.render_template_fields(context)
    task.execute(context=context)
    s = tc.etl_settings
    expected_key = os.path.join(
        s.S3_RAW_KEY,
        "2024/12/",
        s.FILE_NAME_PATTERN.replace("{{get_data_period(ds, '%Y-%m', -3)}}", "2024-12")
    )

    assert flw.check_if_exists_s3(s.S3_BUCKET_SIMPLE, expected_key)
    flw.delete_s3(s.S3_BUCKET_SIMPLE, expected_key)



def test_url():
    op = flw.dag.get_task("fetch_data_to_s3")
    
    # 2. Manually define the "Context" that Jinja needs
    # This mimics what Airflow generates during a real run
    logical_date = datetime(2025, 3, 1)
    context = {
        "dag": flw.dag,
        "ds": logical_date.strftime("%Y-%m-%d"),
        "ds_nodash": logical_date.strftime("%Y%m%d"),
        "logical_date": logical_date,
        "params": {},
        "task": op,
    }

    # 3. Use the Task's own renderer directly (Bypassing TaskInstance init issues)
    op.render_template_fields(context)
    
    # 4. Verify your logic
    print(f"Rendered SQL: {op.op_kwargs.get('file_name')}")
    assert "yellow_tripdata_2024-12.parquet" in op.op_kwargs.get('file_name')
