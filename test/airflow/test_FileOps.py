from airflow.FileOps import FileOps

def test_initiate_FileOps():
	f = FileOps("/home/ec2-user/NYC_taxi")
	assert f.check_for_paths() == True