import pandas as pd


# Funcão para executar no airflow
def exec_validacao_prata():


    # Checa se os registros atendem a condições de padrão de CEP e latitude/longitide

    df_prata = pd.read_parquet("cervejarias_prata", engine='pyarrow')
    result = True

    for _, row in df_prata.iterrows():
        if row['country'] == 'United States' and len(row['postal_code']) < 5:
            result = False
            print(row)
            print("United States postal_code error")
            break
        elif row['country'] == 'Ireland' and len(row['postal_code']) < 7:
            result = False
            print(row)
            print("Ireland postal_code error")
            break
        elif (pd.notnull(row['latitude']) and not (-90 <= row['latitude'] <= 90)) or \
            (pd.notnull(row['longitude']) and not (-180 <= row['longitude'] <= 180)):
            result = False
            print(row)
            print("Latitude/Longitude error")
            break

    if not result:
        raise ValueError("Task failed ")
    
    return result
