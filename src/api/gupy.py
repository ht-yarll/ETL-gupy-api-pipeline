import pathlib
from modules.DataProcessor import DataProcessor
import pandas as pd
import requests

working_dir = pathlib.Path.cwd()
files_folder = working_dir.joinpath('data/files_from_script')
processor = DataProcessor(files_folder)

#fetching data
def fetch_gupy_data(label: str) -> pd.DataFrame:
        offset = 0
        all_data = []
        print(f'Fetching data for {label}...')

        try:
            while True:
                url_template = (
                    f"https://portal.api.gupy.io/api/job?name={label}&offset={offset}&limit=400"
                    )
                print(f'Fetching page {offset}...')

                response = requests.get(url_template)
                data = response.json()
                
                for i in data['data']:
                    all_data.append(i)

                if not data['data']:
                    break

                offset += 10
            
            result = pd.DataFrame(all_data)
            print('All data fetched with success')
            return result
           
        except Exception as e:
            print(f'Failed to fetch data: {e}')
            return pd.DataFrame()

def gupy_fetch_data():
    # labels = ['dados', 'UX/UI', 'administração', 'rh', 'frontend']
    labels = ['python']

    all_jobs = []
    for l in labels:
        all_jobs.append(fetch_gupy_data(l))

    df_jobs = pd.concat(all_jobs, ignore_index=True)

    return df_jobs

  