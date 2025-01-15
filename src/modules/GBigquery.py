import os
import pathlib

from google.cloud import bigquery
import pandas as pd

def get_bqclient():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
        '/home/ht-yarll/Documents/keys/ht-churn-bstone.json'
        )
    return bigquery.Client()

working_dir = pathlib.Path.cwd()

class GBigQuery:
    def __init__(self, bigquery_client):
        self.client = bigquery_client

    def up_to_bigquery(self, file, destination_table, if_exists='replace'):

        job_config = bigquery.LoadJobConfig(source_format = bigquery.SourceFormat.PARQUET
                                            , write_disposition = 'WRITE_TRUNCATE')
        if isinstance(file, pd.DataFrame):
            data = self.client.load_table_from_dataframe(
                file, destination_table, job_config = job_config
            )
        else:
            with open(file, 'rb') as file_obj:
                data = self.client.load_table_from_file(
                    file_obj, destination_table, job_config = job_config
                )
        data.result()
        return data

    def query(self, query_string: str, 
              destination_table: str = None, 
              ):
        try:
            job_config = bigquery.QueryJobConfig()
            if destination_table:
                print(f'Setting destination table to: {destination_table}')
                job_config.destination = destination_table

            query_job = self.client.query(query_string, job_config = job_config)
            query_job.result()
            
            print(f'Query was done!')

            if destination_table:
                print(f'Results saved to table: {destination_table}')

            return query_job
        
        except Exception as e:
            print(f'Error during query: {e}')

    def stream_to_bq(self, data: dict, project_id: str, dataset_id: str):
        bkt_name = data['bucket_name']
        file_name = data['file_name']
        time_created = data['time_created']
        print(f'Processing file: {file_name} from bkt: {bkt_name}')

        try:
            table_name = file_name.split('.')[0]
            print(f"Derived table name: {table_name}")

            self.load_table_from_uri(
                bucketname = bkt_name, 
                filename = file_name,
                project_id = project_id,
                dataset_id = dataset_id, 
                table_name = table_name
                )

        except Exception as e:
            print(f'Error during query: {e}')

    def load_table_from_uri(self, bucketname, filename, project_id, dataset_id, table_name):
        uri = f"gs://{bucketname}/{filename}"
        table_id = f"{project_id}.{dataset_id}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format = bigquery.SourceFormat.PARQUET,
            write_disposition = 'WRITE_TRUNCATE'            
            )

        try:
            load_job = self.client.load_table_from_uri(
                uri,
                table_id,
                job_config = job_config
                )
            
            load_job.result()
            print(f"Table {table_id} loaded from {uri} with success")

        except Exception as e:    
            print(f'Error during loading data into BQ: {e}')