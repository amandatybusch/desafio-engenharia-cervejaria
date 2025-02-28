# desafio-cervejaria
# Instruções

Clone o repositorio em sua máquina, abra o vscode e instale o plugin do Dev Containers.

## Diretório "ambiente_dev" 
Esse diretorio foi criado em um primeiro momento para verificar se os codigos estavam rodando corretamente.

Abra a pasta "ambiente_dev" do repositório clonado, abra a barra de pesquisa do vscode, selecione rebuild and reopen in container.


Abra o terminal do vscode, execute o comando python extracao.py para a geração do arquivo bronze.parquet.

Execute o comando python tratamento.py para geração da pasta cervejarias_prata com os arquivos parquet partcionados.

Execute o comando python carregamento.py para geração do arquivo csv "cervejarias_ouro".


## Diretório "orquestracao"
Abra a pasta "orquestracao" do repositório clonado, abra um novo terminal e execute o comando docker-compose up, isso significa que o airflow está rodando.

Abra o navegador e acesse a url "localhost:8080", username airflow e password airflow.

As dags desenvolvidas serao as primeiras exbidas: dag_error, dag_on_success, dag_on_completed.
Em cada dag selecione trigger DAG e acesse a interface graph para visualizar as dags rodando.

Os codigos da pasta dags de orquestracao contem o objetivo de cada exemplo de dag.

Para verificar se os arquivos foram salvos abra um nvo terminak e rode o comando
docker ps, copie o nome do container e execute o comando docker exec -it orquestracao-airflow-worker-1 /bin/bash para acessar o container.

Use o comando ls para verificar se os arquivos das camadas bronze, prata e outo estão presentes.
Outra opção para visualizar os arquivos salvos: abra o docker desktop, aba containers, expanda orquestracao,acesse airflow-worker-1 > files > opt > airflow > arquivos.

Idealmente esses arquivos devem ficar armazenados em camadas num ambiente cloud.