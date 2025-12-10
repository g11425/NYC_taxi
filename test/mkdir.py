import os
from urllib.request import urlretrieve


file_path_raw = os.path.expanduser("~/NYC_taxi/data/raw/")
file_path_processed = os.path.expanduser("~/NYC_taxi/data/processed/")
file_name = "yellow_tripdata_2025-01.parquet"
url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet'

if not os.path.exists(file_path_raw):
    os.mkdir(file_path_raw)
if not os.path.exists(file_path_processed):
    os.mkdir(file_path_processed)
    
urlretrieve(url, file_path_raw+file_name)
print("successfully saved data")
