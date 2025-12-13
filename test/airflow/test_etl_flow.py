import airflow.etl_flow as flw
from config import PROJECT_ROOT
from airflow.FileOps import FileOps

import shutil
import os

def test_fetch_data():
    f = FileOps(PROJECT_ROOT)
    if(f.check_raw_path()):
        shutil.rmtree(f.data_path_raw)
    file_name = "yellow_tripdata_2025-01.parquet"
    flw.fetch_data()
    full_file_name = os.path.join( f.data_path_raw, file_name)
    assert os.path.exists(full_file_name) == True

def test_upload_to_s3_raw():
	f = FileOps(PROJECT_ROOT)
	assert flw.fetch_data() == True
	assert flw.upload_to_S3(fileOps=f, file_name = "yellow_tripdata_2025-01.parquet") == True
