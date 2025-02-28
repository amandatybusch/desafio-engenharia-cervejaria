import pandas as pd


# Funcão para executar no airflow
def exec_tratamento():

    cervejarias_df = pd.read_parquet('bronze.parquet', engine='pyarrow')
    

    # Tratamento: tipagem e exclusão de colunas

    cervejarias_df['id'] = cervejarias_df['id'].astype('string')

    cervejarias_df['name'] = cervejarias_df['name'].astype('string')

    cervejarias_df['brewery_type'] = cervejarias_df['brewery_type'].astype('string')

    cervejarias_df['address_1'] = cervejarias_df['address_1'].astype('string')
    cervejarias_df = cervejarias_df.rename(columns={'address_1': 'address'})

    cervejarias_df['address_2'] = cervejarias_df['address_2'].astype('string')
    cervejarias_df = cervejarias_df.rename(columns={'address_2': 'address_complement'})

    cervejarias_df = cervejarias_df.drop(columns=['address_3'])

    cervejarias_df['city'] = cervejarias_df['city'].astype('string')

    cervejarias_df['postal_code'] = cervejarias_df['postal_code'].astype('string')

    cervejarias_df = cervejarias_df.drop(columns=['state_province'])

    cervejarias_df['country'] = cervejarias_df['country'].astype('string')

    cervejarias_df['longitude'] = pd.to_numeric(cervejarias_df['longitude'], errors='coerce')

    cervejarias_df['latitude'] = pd.to_numeric(cervejarias_df['latitude'], errors='coerce')

    cervejarias_df['phone'] = cervejarias_df['phone'].replace(r'\D', '', regex=True)
    cervejarias_df['phone'] = pd.to_numeric(cervejarias_df['phone'], errors='coerce')
    cervejarias_df['phone'] = cervejarias_df['phone'].astype('Int64')

    cervejarias_df['website_url'] = cervejarias_df['website_url'].astype('string')

    cervejarias_df['state'] = cervejarias_df['state'].astype('string')

    cervejarias_df = cervejarias_df.drop(columns=['street'])
    

    # Tratamento: filtro de linhas incompletas

    cervejarias_nulos_df = cervejarias_df[cervejarias_df[['id', 'brewery_type', 'state', 'country']].isnull().any(axis=1)]
    print(f"Há um total de {len(cervejarias_nulos_df)} registros com valores nulos, de um total de {len(cervejarias_df)} registros")

    cervejarias_df = cervejarias_df.dropna(subset=['id', 'brewery_type', 'state', 'country'])


    # Salvando camada prata com particionamento

    cervejarias_df.to_parquet('cervejarias_prata', 
                            engine='pyarrow', 
                            partition_cols=['country', 'state'])
    print("foi salvo")