import airflow.etl_flow as flw
import config as tc
from airflow.FileOps import FileOps

import shutil
import os
import pytest

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
    assert flw.check_if_exists_s3(tc.S3_TEST_BUCKET, tc.S3_TEST_RAW_KEY + f.test_file_raw)


def test_fetch_to_s3():
    flw.fetch_to_s3(tc.S3_RAW_TEST_FILE, tc.TEST_RAW_FILE_URL, tc.S3_TEST_BUCKET, tc.S3_TEST_RAW_KEY)
    assert flw.check_if_exists_s3(tc.S3_TEST_BUCKET, tc.S3_RAW_TEST_FILE_KEY)
    flw.delete_s3(tc.S3_TEST_BUCKET, tc.S3_RAW_TEST_FILE_KEY)
