import requests
import pandas as pd


# Funcão para executar no airflow
def exec_extracao():

    # Requisições API

    cervejarias_api = "https://api.openbrewerydb.org/breweries"

    get_cervejarias = requests.get(cervejarias_api)

    cervejarias_json = get_cervejarias.json() 


    # Salvando os dados brutos em parquet na camada bronze

    cervejarias_df_bronze = pd.DataFrame(cervejarias_json)
    print(cervejarias_df_bronze)

    cervejarias_df_bronze.to_parquet("bronze.parquet", engine = 'pyarrow') 
    print("foi salvo")
