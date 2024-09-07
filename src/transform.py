import pandas as pd

# Função para normalizar os nomes das colunas
def normalize_data(df):
    df.columns = [col.strip().lower() for col in df.columns]  # Normaliza os nomes das colunas
    df.drop_duplicates(inplace=True)  # Remove duplicatas
    print(f"Colunas normalizadas: {df.columns}")
    return df

def handle_missing_values(df):
    try:
        for col in df.columns:
            if df[col].dtype == 'object':
                print(f"Preenchendo nulos em: {col}")
                df[col].fillna('missing', inplace=True)  
            else:
                print(f"Preenchendo nulos em: {col} com a média")
                df[col].fillna(df[col].mean(), inplace=True)  
        print(f"Valores nulos tratados: {df.isnull().sum().sum()} nulos restantes")
    except Exception as e:
        print(f"Erro na função handle_missing_values: {e}")
    return df


def convert_data_types(df):
    for col in df.columns:
        if df[col].dtype == 'object':
            if 'date' in col or 'data' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')  
        elif df[col].dtype == 'float64' or df[col].dtype == 'int64':
            df[col] = pd.to_numeric(df[col], errors='coerce')  
    print("Tipos de dados convertidos")
    return df

def remove_outliers(df):
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        mean = df[col].mean()
        std_dev = df[col].std()
        upper_limit = mean + 3 * std_dev
        lower_limit = mean - 3 * std_dev
        df = df[(df[col] >= lower_limit) & (df[col] <= upper_limit)]
    print("Outliers removidos")
    return df

def normalize_strings(df):
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].str.strip().str.lower()
    print("Strings normalizadas")
    return df

def transform_data(df):
    df = normalize_data(df)       
    df = handle_missing_values(df)      
    df = convert_data_types(df)         
    df = remove_outliers(df)            
    df = normalize_strings(df)      
    print("Dados transformados com sucesso")
    return df
