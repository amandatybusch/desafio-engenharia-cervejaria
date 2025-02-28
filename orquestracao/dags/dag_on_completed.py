# Essa dag é um exemplo de pipeline que é executado quando as tasks principais tiverem sucesso
# Se alguma task secundária falhar, não interfere na task_ouro
# Contem uma task secundária com simulação de falha e uma task de apoio
# Contem um task de checagem de dados

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from codigos.extracao import exec_extracao
from codigos.tratamento import exec_tratamento
from codigos.carregamento import exec_carregamento
from codigos.validacao_prata import exec_validacao_prata

# Agendamento
with DAG(
    dag_id='dag_on_completed',
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule='@daily',
    tags=['produces', 'dataset-scheduled'],
) as dag_cervejaria:
    
    # Task de extração dos dados provenientes da API- camada bronze
    task_bronze = PythonOperator(
        task_id='task_bronze',
        python_callable=exec_extracao,       
        retries=3, 
        retry_delay=pendulum.duration(minutes=5), 
        dag=dag_cervejaria
    )

    # Task de tratamento dos dados particionados- camada prata
    task_prata = PythonOperator(
        task_id='task_prata', 
        python_callable=exec_tratamento,       
        retries=3, 
        retry_delay=pendulum.duration(minutes=5),  
        dag=dag_cervejaria
    )

    # Task de checagem dos dados da camada prata
    task_check = PythonOperator(
        task_id='task_check',
        python_callable=exec_validacao_prata,
        dag=dag_cervejaria
    )

    # Task de simulação de uma execução que falha- 
    # tabela secundaria
    task_prata_2 = BashOperator(
        task_id='task_prata_2',
        bash_command='sleep 10; exit 1',
        retries=1,
        retry_delay=pendulum.duration(seconds=10),
        dag=dag_cervejaria
    )   

    # Task de apoio a task_prata_2 
    helper_task_prata_2 = EmptyOperator(
        task_id="helper_task_prata",
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Task de carregamento dos dados analíticos- camada ouro
    task_ouro = PythonOperator(
        task_id='task_ouro',
        python_callable=exec_carregamento,       
        retries=2,  
        retry_delay=pendulum.duration(seconds=10), 
        dag=dag_cervejaria,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Dependências- a task_prata_2 é a unica que pode falhar sem comprometer a execucao da task_ouro
    task_bronze >> [task_prata, task_prata_2] 
    
    task_prata >> task_check
    task_prata_2 >> helper_task_prata_2

    [task_check, helper_task_prata_2] >> task_ouro
   

# Monitoramento- 
# O processo de monitoramento se dá pela task_check, onde é realizada uma verificação de qualidade 
# dos dados e caso não cumpram os requisitos, a task irá falhar e refletir nos logs do airflow


# Alertas-

# Para enviar alertas em casos de erro, os parâmetros email_on_failure e email do Python operator 
# poderiam ser utilizados, junto a configuração do airflow.cfg.
# exemplo com essa feature:
# task_check = PythonOperator(
#        task_id='task_check',
#        python_callable=exec_validacao_prata,
#        dag=dag1,
#        email='teste@exemplo.com',
#        email_on_failure=True
#    )

# Seria possivel enviar alertas por outros canais de comunicação (Slack, Google Chat, ETC),sendo necessario 
# criar a funcao no código do airflow, atribuir ao parametro on_failure_callback do operador essa funcao.
# def enviar_msg_no_teams():
#  codigo...
#  task_check = PythonOperator(
#        task_id='task_check',
#        python_callable=exec_validacao_prata,
#        on_failure_callback=enviar_msg_no_teams, 
#        dag=dag1
#    )