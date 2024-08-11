from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from kafka import KafkaProducer
import json
from faker import Faker
import random
    
"""
Esta DAG apenas insere dados fakes de clientes no topico kafka
para exemplificar o processo.
"""
def create_fake_data():

    producer = KafkaProducer(bootstrap_servers='kafka:9092',
                value_serializer=lambda x: json.dumps(x).encode('utf-8'))
  

    # Inicializa o Faker
    fake = Faker('pt_BR')

    # Gera uma lista de clientes fictícios
    clientes = []

    # Número de registros que você deseja gerar
    import random
    num_registros = random.randint(100, 500)

    for _ in range(num_registros):
        cliente = {
            "nome": fake.first_name(),
            "sobrenome": fake.last_name(),
            "idade": random.randint(18, 70),
            "email": fake.email(),
            "data_cadastro": fake.date_this_year().strftime("%Y-%m-%d")
        }
        clientes.append(cliente)


    for cliente in clientes:
        producer.send('clients_kafka',  value=cliente)


dag = DAG('kafka_create_fake_data', description='Pipeline de Cadastro de Clientes',
          schedule_interval='* * * * *',
          start_date=datetime(2024, 8, 9), catchup=False)

consume_task = PythonOperator(task_id='kafka_create_fake_data', python_callable=create_fake_data, dag=dag)

consume_task
