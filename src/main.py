import pathlib
from api.gupy import gupy_fetch_data

from modules.GBigquery import GBigQuery, get_bqclient
from modules.GCStorage import GCStorage, get_gclient
from modules.DataProcessor import DataProcessor

from google.cloud import bigquery

working_dir = pathlib.Path.cwd()
files_folder = working_dir.joinpath('data/files_from_script')
processor = DataProcessor(files_folder)

#fetching data
gupy_fetch_data()

#Upload to CloudStorage
storage_client = get_gclient()
gcs = GCStorage(storage_client)
bkt_name = 'bstone-dgupy'

try:
    if not bkt_name in gcs.list_buckets():
        bucket_gcs = gcs.create_bucket('bstone-dgupy', storage_class='STANDARD')
    else:
        bucket_gcs = gcs.get_bucket(bkt_name)

    for file_path in files_folder.glob('*.*'):
        gcs.upload_file(bucket_gcs, file_path.name, str(file_path))
    
    print(f'upload of {file_path.name} to GCS was done!')

except Exception as e:
    print(e)

# #Streaming to BigQuery
bq_client = get_bqclient()
gbq = GBigQuery(bq_client)
project_id = 'blackstone-446301'
dataset_id = 'dgupy'

data = {
    'bucket_name': 'bstone-dgupy',
    'file_name': 'gupy_data.parquet',
    'time_created': '2025-01-15T05:18:37Z'
}

gbq.stream_to_bq(data, project_id = project_id, dataset_id = dataset_id)