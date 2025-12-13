import airflow.etl_flow as flw
from config import PROJECT_ROOT
from airflow.FileOps import FileOps

import shutil
import os
import pytest

def test_fetch_data():
    f = FileOps(PROJECT_ROOT)
    if(f.check_raw_path()):
        shutil.rmtree(f.data_path_raw)
    file_name = "yellow_tripdata_2025-01.parquet"
    flw.fetch_data()
    full_file_name = os.path.join( f.data_path_raw, file_name)
    assert os.path.exists(full_file_name) == True



@pytest.fixture
def set_up_dummy_raw():
    f = FileOps(PROJECT_ROOT)
    f.create_dummy_raw()
    yield f
    f.clean_dummy_raw()

def test_upload_to_s3_raw(set_up_dummy_raw):
	f = set_up_dummy_raw
	assert flw.fetch_data() == True
	assert flw.upload_to_S3_raw(fileOps=f, file_name = f.test_file_raw) == True

@pytest.fixture
def set_up_dummy_processed():
    f = FileOps(PROJECT_ROOT)
    f.create_dummy_processed()
    yield f
    f.clean_dummy_processed()

def test_upload_to_s3_processed(set_up_dummy_processed):
    f = set_up_dummy_processed
    assert flw.upload_to_s3_processed(fileOps= f, file_name="") == True
