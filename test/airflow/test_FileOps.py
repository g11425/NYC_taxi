from airflow.FileOps import FileOps
import airflow.etl_flow as flw
from config import PROJECT_ROOT
import os


def test_initiate_FileOps():
	f = FileOps(PROJECT_ROOT)
	assert f.setup_data_paths() == True
	assert f.clear_data_paths() == True

def test_dummy_raw():
	f = FileOps(PROJECT_ROOT)
	if (os.path.exists(f.test_file_raw)):
		f.clean_dummy_raw()
	f.create_dummy_raw()
	assert os.path.exists(f.test_file_raw)
	f.clean_dummy_raw()
	assert os.path.exists(f.test_file_raw) == False



