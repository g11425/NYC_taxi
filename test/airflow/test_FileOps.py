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
	if (os.path.exists(f.get_test_raw_file_name_abs())):
		f.clean_dummy_raw()
	f.create_dummy_raw()
	assert os.path.exists(f.get_test_raw_file_name_abs())
	f.clean_dummy_raw()
	assert os.path.exists(f.get_test_raw_file_name_abs()) == False

	
def test_dummy_processed():
	f = FileOps(PROJECT_ROOT)
	f.clean_dummy_processed()

	f.create_dummy_processed()
	for file in f.get_test_processed_file_names_abs():
		assert os.path.exists(file)

	f.clean_dummy_processed()
	for file in f.get_test_processed_file_names_abs():
		assert os.path.exists(file) == False



