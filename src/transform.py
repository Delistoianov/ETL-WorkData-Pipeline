import pandas as pd

def normalize_data(df):
    # Normaliza os nomes das colunas removendo espaços e convertendo para minúsculas, e remove duplicatas
    df.columns = [col.strip().lower() for col in df.columns] 
    df.drop_duplicates(inplace=True)  
    print(f"Colunas normalizadas: {df.columns}")
    return df

def handle_missing_values(df):
    # Preenche valores nulos: strings com 'missing' e numéricos com a média da coluna
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
    # Converte colunas de data para datetime e colunas numéricas para tipo numérico
    for col in df.columns:
        if df[col].dtype == 'object':
            if 'date' in col or 'data' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')  
        elif df[col].dtype == 'float64' or df[col].dtype == 'int64':
            df[col] = pd.to_numeric(df[col], errors='coerce')  
    print("Tipos de dados convertidos")
    return df

def preprocess_datetime_columns(df):
    # Preenche valores nulos em colunas datetime com uma data padrão
    datetime_cols = df.select_dtypes(include=['datetime64']).columns
    for col in datetime_cols:
        df[col].fillna(pd.Timestamp('1970-01-01'), inplace=True)  
    print("Colunas datetime processadas")
    return df

def preprocess_int_columns(df):
    # Ajusta valores em colunas inteiras para estarem dentro do intervalo 0 a 4294967295
    int_cols = df.select_dtypes(include=['int64']).columns
    for col in int_cols:
        df[col] = df[col].apply(lambda x: x if 0 <= x <= 4294967295 else 0)
    print("Colunas inteiras processadas")
    return df

def remove_outliers(df):
    # Remove outliers em colunas numéricas baseando-se em 3 desvios padrão da média
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
    # Normaliza strings removendo espaços e convertendo para minúsculas
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].str.strip().str.lower()
    print("Strings normalizadas")
    return df

def transform_data(df):
    # Aplica uma série de transformações no DataFrame
    df = normalize_data(df)       
    df = handle_missing_values(df)      
    df = convert_data_types(df)         
    
    df = preprocess_datetime_columns(df)  
    df = preprocess_int_columns(df)  
    
    df = remove_outliers(df)            
    df = normalize_strings(df)      
    print("Dados transformados com sucesso")
    return df