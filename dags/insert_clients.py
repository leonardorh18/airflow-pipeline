from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from kafka import KafkaProducer
import json
from faker import Faker
import random
from clickhouse_driver import Client
"""
Esta DAG apenas insere dados fakes de clientes no topico kafka
para exemplificar o processo.
"""
def create_fake_data():

    producer = KafkaProducer(bootstrap_servers='kafka:9092',
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    
    fake = Faker()
    clientes = []
    num_registros = 100  # Número total de registros
    

    # Conexão ao servidor ClickHouse
    client_ch = Client(
        host='clickhouse',       # Endereço do servidor ClickHouse
        port=9000,              # Porta padrão para conexões TCP
        user='default',         # Usuário (padrão é 'default')
        password='',            # Senha (por padrão, vazio)
        database='default',     # Nome do banco de dados (por padrão, 'default')
    )
    # Dicionário para armazenar dados de clientes já gerados
    
    client_ch.execute('''
    CREATE TABLE IF NOT EXISTS dim_clients (
        surrogate_key UUID DEFAULT generateUUIDv4(),
        nome String,
        sobrenome String,
        idade Int32,
        email String,
        data_cadastro Date,
        effective_date Date,
        end_date Date,
        is_current UInt8
    ) ENGINE = MergeTree()
    ORDER BY (nome, sobrenome, effective_date)
    ''')
    clientes_existentes = client_ch.execute("SELECT distinct nome, sobrenome, idade, email FROM dim_clients where is_current = 1")
    clientes_existentes_dict = {}
    if len(clientes_existentes) > 0:
        clientes_existentes_dict = {cliente[3]: {"nome": cliente[0], "sobrenome": cliente[1], "idade": cliente[2]} for cliente in clientes_existentes}
    for _ in range(num_registros):
        # Decidir se deve criar um novo cliente ou reutilizar um existente
        if clientes_existentes_dict and random.random() < 0.3:  # 30% de chance de reutilizar um cliente existente
            email = random.choice(list(clientes_existentes_dict.keys()))
            cliente_base = clientes_existentes_dict[email]
            cliente = {
                "nome": cliente_base["nome"],
                "sobrenome": cliente_base["sobrenome"],
                "idade": cliente_base["idade"],
                "email": email,
                "data_cadastro": fake.date_this_year().strftime("%Y-%m-%d")
            }
        else:
            email = fake.email()
            cliente = {
                "nome": fake.first_name(),
                "sobrenome": fake.last_name(),
                "idade": random.randint(18, 70),
                "email": email,
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
