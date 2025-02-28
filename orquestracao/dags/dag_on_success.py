# Essa dag é um exemplo de pipeline que só é executado quando todas as tasks tiverem sucesso
# Se alguma task falhar, a proxima task não executa
# Contem tasks que rodam com sucesso, possibilitando a execução da task_ouro


import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from codigos.extracao import exec_extracao
from codigos.tratamento import exec_tratamento
from codigos.carregamento import exec_carregamento

# Agendamento
with DAG(
    dag_id='dag_on_success',
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule='@daily',
    tags=['produces', 'dataset-scheduled'],
) as dag1:
    

    # Task de extração dos dados provenientes da API- camada bronze
    task_bronze = PythonOperator(
        task_id='task_bronze',
        python_callable=exec_extracao,       
        retries=3, 
        retry_delay=pendulum.duration(seconds=10), 
        dag=dag1
    )

    # Task de tratamento dos dados particionados- camada prata
    task_prata = PythonOperator(
        task_id='task_prata', 
        python_callable=exec_tratamento,       
        retries=3, 
        retry_delay=pendulum.duration(seconds=10),  
        dag=dag1
    )

    # Task de carregamento dos dados analíticos- camada ouro
    task_ouro = PythonOperator(
        task_id='task_ouro',
        python_callable=exec_carregamento,       
        retries=3,  
        retry_delay=pendulum.duration(seconds=10), 
        dag=dag1
    )

    # Dependências- é necessário o sucesso da task atual para e execução da proxima
    task_bronze >> task_prata >> task_ouro

# Monitoramento- 
# Nesse exemplo o processo de monitoramento foi desconsiderado

# Alertas-
# Para enviar alertas em casos de erro, os parâmetros email_on_failure e email do Python operator 
# poderiam ser utilizados, junto a configuração do airflow.cfg.
# Seria possivel enviar alertas por outros canais de comunicação (Slack, Google Chat, ETC),sendo necessario 
# criar a funcao no código do airflow, atribuir ao parametro on_failure_callback do operador essa funcao.
   
