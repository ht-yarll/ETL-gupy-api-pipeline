import os
import pathlib

from modules.GBigquery import GBigQuery, get_bqclient
from modules.GCStorage import GCStorage, get_gclient
from modules.DataProcessor import DataProcessor

import pandas as pd

working_dir = pathlib.Path.cwd()
files_folder = working_dir.joinpath('data/files_from_script')
processor = DataProcessor(files_folder)

#fetching data
labels = ['dados', 'UX/UI', 'administração', 'rh', 'frontend']

all_jobs = []
for l in labels:
    jobs = all_jobs.append(processor.fetch_gupy_data(l))

df = pd.concat(all_jobs, ignore_index=True)
df = processor.treat_data(df)
processor.save_to_parquet(df)

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

#Download from CloudStorage
gcs_demo_blobs = gcs.list_blobs(bucket_gcs)
downloads_folder = working_dir.joinpath('data/downloads')

for blob in gcs_demo_blobs:
    path_download = downloads_folder.joinpath(blob.name)
    if not path_download.parent.exists():
        path_download.parent.mkdir(parents=True)

    try:    
        blob.download_to_filename(str(path_download))
        print(f'Download of {blob.name} was done!')
        blob.delete()
        print(f'Disposing of {blob.name}')
    except Exception as e:
        print(e)

#Sending to BigQuery
bq_client = get_bqclient()
bqc = GBigQuery(bq_client)
project_id = 'blackstone-446301'
dataset_id = 'user_data'
table_name = 'gupy_data'
destination_table = f'{project_id}.{dataset_id}.{table_name}'

for file in downloads_folder.iterdir():
    try:
        bqc.up_to_bigquery(file, destination_table=destination_table)
        print(f'Upload of {file.name} was done!')
    except Exception as e:
        print(f'Error in upload:{e}')