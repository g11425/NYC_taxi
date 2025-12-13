from airflow.FileOps import FileOps

def test_initiate_FileOps():
	f = FileOps.initialize("/home/ec2-user/NYC_taxi")