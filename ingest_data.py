from google.cloud import storage
import requests
from io import BytesIO

storage_client = storage.Client()
bucket = storage_client.get_bucket('dtc-de-course-412311-mage')

base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'

for month in range(0, 12):
    file_name = f'green_tripdata_2022-{str(month+1).zfill(2)}.parquet'
    src_url = f'{base_url}/{file_name}'
    # Make a request to get the file content
    response = requests.get(src_url)
    if response.status_code == 200:
        # Upload the file content to GCS
        blob = bucket.blob(f'green_taxi_2022/{file_name}')
        blob.upload_from_string(response.content)
    else:
        print(f'Failed to download {src_url}')