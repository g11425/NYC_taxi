from airflow.FileOps import FileOps
from config import PROJECT_ROOT

def test_initiate_FileOps():
	f = FileOps(PROJECT_ROOT)
	assert f.check_for_paths() == True
	assert f.clear_data_paths() == True