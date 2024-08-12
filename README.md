# Instruções
Para iniciar o docker-compose basta executar o comando 
```
docker compose up --build -d
````
Com isso, todas as imagens e dependencias serão baixadas. Após a execução dos containes, aguarde alguns minutos e acesse
o [Airflow no localhost:8080](http://localhost:8080/login/) com **usuário: admin** e **senha: admin**

Após entrar no Airflow, etive a execução das 2 dags:

![DAGs](img/airflow.png)

## DAGs:
- ### **consume_clients_kafka**:
    A fim de exemplo, executa de um em um minuto. Possui duas tasks:
    - **kafka_consume_topic_clients**: responsável por ler as mensagens do tópico kafka e salvar o dado bruto no s3 na camada bronze. Retorna o path do arquivo salvo no s3 para a próxima task.

    - **read_file_from_minio**: responsável por ler o arquivo escrito na task anterior e salvar no ClickHouse aplicando SCD2.

- ### **kafka_create_fake_data**:
    A fim de exemplo, executa de um em um minuto. Possui a task:
    - **kafka_create_fake_data**: responsável por gerar dados fakes de clientes e publicar no topico kafka. Utiliza dos dados ja existentes no ClickHouse para gerar dados repetidos com datas de cadastro diferentes a fim de exemplificar o SCD2.

## Visualização dos dados
Para acessar o MiniIO acesse: http://localhost:9001/browser com **usuário: minioadmin e senha: minioadmin**.

Os logs da execução das DAGs são armazenados no bucket bronze_layer via configuração do Conector nas configurações do Airflow.

Os dados brutos são salvos em .json no bucket bronze_layer
![DAGs](img/bucket.png)

o particionamento é feito da seguinte forma:
![DAGs](img/partition.png)


# ClickHouse

Para viualizar os dados no ClickHouse, eu utilizei o DBeaver no localhost:8123

![DAGs](img/clickhouse.png)

# Melhorias:
- Melhorar a passagem de chaves e senhas de acesso
- Criar conexão com Kafka via airflow connection


