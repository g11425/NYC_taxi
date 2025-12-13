import airflow.etl_flow as flw
from config import PROJECT_ROOT
from airflow.FileOps import FileOps

import shutil
import os

def test_fetch_data():
    f = FileOps(PROJECT_ROOT)
    if(f.check_raw_path):
        shutil.rmtree(f.data_path_raw)
    file_name = "yellow_tripdata_2025-01.parquet"
    flw.fetch_data()
    assert os.path.exists(os.path.join( f.data_path_raw, file_name))
