import os
import pandas as pd

def csv_to_parquet(csv_path: str, parquet_path: str):
    table_names = []  # Lista para armazenar os nomes das tabelas (nomes dos arquivos CSV)
    
    # Percorre todos os arquivos na pasta csv_path
    files = [f for f in os.listdir(csv_path) if f.endswith('.csv')]
    for file in files:
        csv_file_path = os.path.join(csv_path, file)
        parquet_file_path = os.path.join(parquet_path, file.replace('.csv', '.parquet'))
        
        try:
            # Lê o arquivo CSV com a codificação 'latin1'
            df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')  # ou 'latin1'
            
            # Converte e salva como Parquet
            df.to_parquet(parquet_file_path)
            print(f"Arquivo {file} convertido para Parquet: {parquet_file_path}")
            
            # Armazena o nome do arquivo CSV (sem a extensão .csv)
            table_name = file.replace('.csv', '')
            table_names.append(table_name)
        
        except UnicodeDecodeError as e:
            print(f"Erro de codificação ao ler o arquivo {file}: {e}")
        except Exception as e:
            print(f"Erro ao processar o arquivo {file}: {e}")
    
    return table_names  # Retorna a lista de nomes de tabelas
