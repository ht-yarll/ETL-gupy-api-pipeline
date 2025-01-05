import json

import pandas as pd
import requests


class DataProcessor:
    def __init__(self, output_path: str) -> None:
        self.output_folder = output_path

    def fetch_gupy_data(self, label: str) -> pd.DataFrame:
        base_url = f"https://portal.api.gupy.io/api/job"
        offset = 0
        limit = 50
        all_data = []
        
        print(f'Fetching data for {label}...')

        try:
            while True:
                url = f'{base_url}?name={label}&offset={offset}&limit={limit}'
                r = requests.get(url)
                response = r.json()
                data = response.get('data', [])

                if not data:
                    break

                all_data.extend(data)
                offset += limit

                print(f'Fetched {len(data)} items, total so far: {len(all_data)}')

                response = pd.DataFrame(all_data)
                print('All data fetched successfully')
                return response
           
        except Exception as e:
            print(f'Failed to fetch data: {e}')

    def treat_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            print('Treating data...')
            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(r"[^a-z0-9_]", "_", regex=True)

            )
            df = df.fillna({
                col: "N/A" if df[col].dtype == "object" else 0
                for col in df.columns
            })
            df = df.convert_dtypes()
            
        except Exception as e:
            print(f'Error during treating data: {e}')

        return df

    def save_to_parquet(self, df: pd.DataFrame, file_name: str = 'table.parquet'):
        try:
            output_path = self.output_folder.joinpath(file_name)
            print('saving df to parquet on folder')
            df.to_parquet(
                output_path,
                compression = None
            )
            print(f'File saved on {output_path} with success')
        
        except Exception as e:
            print(f'Error during save: {e}')