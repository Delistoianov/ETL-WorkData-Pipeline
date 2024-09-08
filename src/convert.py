import os
import pandas as pd

def csv_to_parquet(csv_path: str, parquet_path: str):
    table_names = []  
    
    files = [f for f in os.listdir(csv_path) if f.endswith('.csv')]
    for file in files:
        csv_file_path = os.path.join(csv_path, file)
        parquet_file_path = os.path.join(parquet_path, file.replace('.csv', '.parquet'))
        
        try:
            df = pd.read_csv(csv_file_path, encoding='ISO-8859-1') 
            
            df.to_parquet(parquet_file_path)
            print(f"Arquivo {file} convertido para Parquet: {parquet_file_path}")
            
            table_name = file.replace('.csv', '')
            table_names.append(table_name)
        
        except UnicodeDecodeError as e:
            print(f"Erro de codificação ao ler o arquivo {file}: {e}")
        except Exception as e:
            print(f"Erro ao processar o arquivo {file}: {e}")
    
    return table_names  
