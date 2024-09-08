from src.extract import extract_data
from src.transform import transform_data
from src.load import connect_to_clickhouse, load_data_to_clickhouse
from src.convert import csv_to_parquet 

def run_etl():
    tmp_dir = 'tmp/'

    table_names = csv_to_parquet(tmp_dir, tmp_dir)
    
    dataframes = extract_data(tmp_dir)
    
    transformed_data = [transform_data(df) for df in dataframes]
    
    client = connect_to_clickhouse()
    
    if client:
        for df, table_name in zip(transformed_data, table_names):
            load_data_to_clickhouse(client, df, table_name)  
    else:
        print("Erro na conexão com ClickHouse.")

if __name__ == '__main__':
    run_etl()
