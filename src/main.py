import pathlib
from api.gupy import gupy_fetch_data
from modules.GBigquery import GBigQuery, get_bqclient
from modules.GCStorage import GCStorage, get_gclient
from modules.DataProcessor import DataProcessor

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

#Querying BigQuery
excluded_columns = [
    'description', 'careerpagelogo', 'city', 'joburl', 'badges', 'careerpageurl'
    ]
table_id = 'blackstone-446301.user_data.gupy_data'
table = bq_client.get_table(table_id)
selected_columns = [
    field.name for field in table.schema 
    if field.name not in excluded_columns
    ]

query_job = f"""
SELECT {', '.join(selected_columns)}
FROM   `blackstone-446301.user_data.gupy_data`
WHERE country = ('Brasil')
"""
new_table_name = 'gupy_data_analyzed'

if not new_table_name in bq_client.list_tables(dataset_id):
    print(f'Creating table {table_name} on {dataset_id}...')
    bqc.query(query_job, destination_table=f'{project_id}.{dataset_id}.{new_table_name}')   
else:
    print(f'Table {new_table_name} already exists on {dataset_id}')