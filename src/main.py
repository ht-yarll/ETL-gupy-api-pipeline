import os
import pathlib
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

#sending to CloudStorage
storage_client = get_gclient()
gcs = GCStorage(storage_client)
bucket_name = 'ht-churn-bstone'

try:
    if not bucket_name in gcs.list_buckets():
        bucket_gcs = gcs.create_bucket('bstone-dgupy', storage_class='STANDARD')
    else:
        bucket_gcs = gcs.get_bucket(bucket_name)

    for file_path in files_folder.glob('*.*'):
        gcs.upload_file(bucket_gcs, file_path.name, str(file_path))
    
    print(f'upload of {file_path.name} was done!')

except Exception as e:
    print(e)
