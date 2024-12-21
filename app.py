from dotenv import load_dotenv
import os
import pandas as pd
import sqlalchemy

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

try:
    # Leitura do arquivo CSV
    df = pd.read_csv('raw/empregdata.csv')
    print(df)

    # Obter as variáveis de ambiente
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')

    if not all([server, database, username, password]):
        raise ValueError("Informações de conexão não encontradas nas variáveis de ambiente")

    # Criar a string de conexão
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

    # Criar engine de conexão
    engine = sqlalchemy.create_engine(connection_string)

    # Salvar o DataFrame no banco de dados
    df.to_sql('nome_da_tabela', con=engine, if_exists='replace', index=False)

    print("DataFrame salvo com sucesso no banco de dados!")

except FileNotFoundError:
    print("File not found")
except Exception as e:
    print(f"An error occurred: {e}")
