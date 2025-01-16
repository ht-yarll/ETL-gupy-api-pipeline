import pandas as pd
import requests


class DataProcessor:
    def __init__(self, output_path: str) -> None:
        self.output_folder = output_path

    def fetch_gupy_data(self, label: str) -> pd.DataFrame:
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

    def treat_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            print('Treating data...')
            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(r"[^a-z0-9_]", "_", regex=True)

            )
            df = df.map(
            lambda x: str(x) if isinstance(x, dict) else x
        )
            df = df.fillna({
                col: "N/A" if df[col].dtype == "object" else 0
                for col in df.columns
            })
            df = df.drop_duplicates()
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