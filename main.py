from src.extract import extract_data
from src.transform import transform_data
from src.load import connect_to_clickhouse, load_data_to_clickhouse
from src.convert import csv_to_parquet 

def run_etl():
    tmp_dir = 'tmp/'

    # Passo 1: Converte arquivos CSV para Parquet e captura os nomes dos arquivos (tabelas)
    table_names = csv_to_parquet(tmp_dir, tmp_dir)
    
    # Passo 2: Extrai os dados dos arquivos Parquet
    dataframes = extract_data(tmp_dir)
    
    # Passo 3: Aplica o processo completo de transformação
    transformed_data = [transform_data(df) for df in dataframes]
    
    # Passo 4: Conecta ao ClickHouse
    client = connect_to_clickhouse()
    
    if client:
        # Passo 5: Carrega os dados no ClickHouse, associando cada DataFrame ao nome da tabela correspondente
        for df, table_name in zip(transformed_data, table_names):
            load_data_to_clickhouse(client, df, table_name)  # Nome da tabela é o nome do arquivo CSV
    else:
        print("Erro na conexão com ClickHouse.")

if __name__ == '__main__':
    run_etl()
