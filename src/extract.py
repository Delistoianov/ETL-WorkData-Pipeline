import pandas as pd
import os

def extract_data(parquet_path: str):
    files = [f for f in os.listdir(parquet_path) if f.endswith('.parquet')]
    dataframes = []
    for file in files:
        df = pd.read_parquet(os.path.join(parquet_path, file))
        dataframes.append(df)
    return dataframes

