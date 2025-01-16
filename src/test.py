from modules.DataProcessor import DataProcessor

processor = DataProcessor('data/files_from_script')
df = processor.fetch_gupy_data('python')
df = processor.treat_data(df)

print(df.head())
print(df.dtypes)