import airflow.etl_flow as flw 
import pytest
import os


@pytest.fixture
def setup_dummy_data():
    projectPath = os.environ.get("PYTHONPATH")
    filePath = "/data/raw/"
    f = open(projectPath + filePath + "test_file_tos3.csv")


def test_fetch_data():
    assert flw.fetch_data() == True

def test_upload_to_s3():
    assert flw.upload_to_S3() == True



