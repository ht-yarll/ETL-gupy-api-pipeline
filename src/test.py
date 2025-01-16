from modules.DataProcessor import DataProcessor 
from api.gupy import fetch_gupy_data

processor = DataProcessor('data/files_from_script')
df = fetch_gupy_data('python')
df = processor.treat_data(df)

print(df.head())
print(df.dtypes)