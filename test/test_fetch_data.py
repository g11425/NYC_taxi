import airflow.etl_flow as flw 

def test_fetch_data():
    assert flw.fetch_data() == True

