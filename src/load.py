import os
import pandas as pd
from dotenv import load_dotenv
import clickhouse_connect  

load_dotenv()

# Obter as variáveis de ambiente do arquivo .env
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST').replace('http://', '')  
if ':' in CLICKHOUSE_HOST:
    CLICKHOUSE_HOST = CLICKHOUSE_HOST.split(':')[0]  
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8123))  

CLICKHOUSE_USERNAME = os.getenv('CLICKHOUSE_USERNAME')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE')

print(f"Host: {CLICKHOUSE_HOST}")
print(f"Port: {CLICKHOUSE_PORT}")

# Função para conectar ao ClickHouse
def connect_to_clickhouse():
    try:
        # Conectar ao ClickHouse com o cliente da biblioteca clickhouse-connect
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,  
            username=CLICKHOUSE_USERNAME,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        print("Conexão com ClickHouse bem-sucedida")
        return client
    except Exception as e:
        print(f"Erro ao conectar ao ClickHouse: {e}")
        return None

# Função para mapear tipos de dados do Pandas para ClickHouse
def map_pandas_to_clickhouse_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'UInt32'  
    elif pd.api.types.is_float_dtype(dtype):
        return 'Float64'
    elif pd.api.types.is_object_dtype(dtype):
        return 'String'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DateTime'
    else:
        raise ValueError(f"Tipo de dado não mapeado: {dtype}")

# Função para gerar o SQL de criação da tabela com base no DataFrame
def generate_create_table_query(df, table_name):
    columns = []
    for col in df.columns:
        dtype = df[col].dtype
        clickhouse_type = map_pandas_to_clickhouse_type(dtype)
        columns.append(f"{col} {clickhouse_type}")
    
    columns_sql = ", ".join(columns)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    ) ENGINE = MergeTree()
    ORDER BY tuple();
    """
    return create_table_query

# Função para criar a tabela dinamicamente com base nas colunas do DataFrame
def create_table_dynamic(client, df, table_name):
    try:
        # Gerar o comando CREATE TABLE de acordo com as colunas e tipos do DataFrame
        create_table_query = generate_create_table_query(df, table_name)
        client.command(create_table_query)
        print(f"Tabela '{table_name}' criada (ou já existe).")
    except Exception as e:
        print(f"Erro ao criar tabela: {e}")

# Função para carregar os dados no ClickHouse
def load_data_to_clickhouse(client, df, table_name):
    try:
        # Criar a tabela no ClickHouse de forma dinâmica
        create_table_dynamic(client, df, table_name)
        
        # Inserir os dados
        client.insert_df(table_name, df)
        print(f"Dados carregados com sucesso na tabela '{table_name}'")
    except Exception as e:
        print(f"Erro ao carregar os dados no ClickHouse: {e}")