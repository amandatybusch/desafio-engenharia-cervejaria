import pandas as pd


# Funcão para executar no airflow
def exec_carregamento():

    df_cervejarias = pd.read_parquet("cervejarias_prata", engine='pyarrow')


    # Visão com quantidades de cervejarias por tipo e localização

    contagem = df_cervejarias.groupby(['brewery_type', 'country', 'state']).size().reset_index(name='brewery_qtd')

    combinacoes_validas = df_cervejarias[['country', 'state']].drop_duplicates()

    contagem_valida = contagem[contagem[['country', 'state']].apply(tuple, axis=1).isin(combinacoes_validas.apply(tuple, axis=1))]

    cervejarias_ouro= contagem_valida.sort_values(by=['country', 'state', 'brewery_type'])


    # Salvando camada ouro 

    cervejarias_ouro.to_csv('cervejarias_ouro.csv', index=False)
    print("foi salvo")
