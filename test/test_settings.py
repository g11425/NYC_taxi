import config
import os


def test_read_file_names_emr_s3():
    s = config.refresh_etl_settings(EXEC_ENV='emr', RUN_DATE='2024-05-01', DATA_STORE='s3')
    test_date = '2024-05-01'
    arr = s.RUN_DATE.split("_")
    test_date = os.path.join(arr[0], arr[1])
    expected_raw = os.path.join(s.S3_BUCKET, s.S3_RAW_KEY, test_date, s.FILE_NAME)
    expected_processed = os.path.join(s.S3_BUCKET, s.S3_PRCSD_KEY, test_date, s.FILE_NAME)

    raw, processed = s.DATA_FILE_NAMES
    assert raw == expected_raw
    assert processed == expected_processed


def test_read_file_names_local():

    s = config.refresh_etl_settings(EXEC_ENV='local', RUN_DATE='2024-05-01', DATA_STORE='local')
    test_date = '2024-05-01'
    arr = s.RUN_DATE.split("_")
    test_date = os.path.join(arr[0], arr[1])
    expected_raw = os.path.join(s.LOCAL_RAW_DATA_PATH, test_date, s.FILE_NAME)
    expected_processed = os.path.join(s.LOCAL_PRCSD_DATA_PATH, test_date, s.FILE_NAME) 

    raw, processed = s.DATA_FILE_NAMES
    assert raw == expected_raw
    assert processed == expected_processed

def test_read_file_names_local_s3():

    s = config.refresh_etl_settings(EXEC_ENV='local', RUN_DATE='2024-05-01', DATA_STORE='s3')
    test_date = '2024-05-01'
    arr = s.RUN_DATE.split("_")
    test_date = os.path.join(arr[0], arr[1])
    
    expected_raw = os.path.join("s3a://" , s.S3_BUCKET_SIMPLE, s.S3_RAW_KEY, test_date, s.FILE_NAME)
    expected_processed = os.path.join("s3a://" , s.S3_BUCKET_SIMPLE, s.S3_PRCSD_KEY, test_date, s.FILE_NAME)

    raw, processed = s.DATA_FILE_NAMES
    assert raw == expected_raw
    assert processed == expected_processed


def test_read_extras_names_emr():
    s = config.refresh_etl_settings(EXEC_ENV='emr', RUN_DATE='2024-05-01', DATA_STORE='s3')
    test_date = '2024-05-01'
    arr = s.RUN_DATE.split("_")
    test_date = os.path.join(arr[0], arr[1])
    expected_processed = os.path.join(s.S3_BUCKET, s.S3_PRCSD_KEY, test_date ,s.S3_EXTRAS)

    processed = s.EXTRAS_FILE_NAMES
    assert processed == expected_processed

def test_read_extras_names_local():
    s = config.refresh_etl_settings(EXEC_ENV='local', RUN_DATE='2024-05-01', DATA_STORE='local')
    test_date = '2024-05-01'
    arr = s.RUN_DATE.split("_")
    test_date = os.path.join(arr[0], arr[1])
    expected_processed = os.path.join(s.LOCAL_PRCSD_DATA_PATH, test_date , s.LOCAL_EXTRAS ,s.FILE_NAME)

    processed = s.EXTRAS_FILE_NAMES
    assert processed == expected_processed

def test_read_extras_names_local_s3():
    s = config.refresh_etl_settings(EXEC_ENV='local', RUN_DATE='2024-05-01', DATA_STORE='s3')
    test_date = '2024-05-01'
    arr = s.RUN_DATE.split("_")
    test_date = os.path.join(arr[0], arr[1])
    expected_processed = os.path.join("s3a://" , s.S3_BUCKET_SIMPLE, s.S3_PRCSD_KEY, test_date ,s.S3_EXTRAS)

    processed = s.EXTRAS_FILE_NAMES
    assert processed == expected_processed