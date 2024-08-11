from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
from clickhouse_driver import Client
from minio import Minio
from minio.error import S3Error
import json
from io import BytesIO
import os

from kafka import KafkaConsumer
TOPIC = 'clients_kafka'
BOOTSTRAP = 'kafka:9092'
ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")

from clickhouse_driver import Client
from minio import Minio
BUCKET_NAME = 'bronze-layer'

class TopicOffsetControll():
    def __init__(self, topic):
        self.topic = topic
        self.client = Client(
                host='clickhouse',       # Endereço do servidor ClickHouse
                port=9000,              # Porta padrão para conexões TCP
                user='default',         # Usuário (padrão é 'default')
                password='',            # Senha (por padrão, vazio)
                database='default',     # Nome do banco de dados (por padrão, 'default')
        )
        
    def get_offset(self):
        self.client.execute("""CREATE TABLE IF NOT EXISTS kafka_topics_offset_control (
                kafka_topic String,
                last_offset Int64,
                exec_date DEFAULT now() 
                ) ENGINE = MergeTree()
                ORDER BY exec_date """)
        offset = self.client.execute(f"SELECT max(last_offset) FROM kafka_topics_offset_control WHERE kafka_topic = '{self.topic}'")[0][0]
        return offset

    def insert_offset(self, offset):
        print(f"OFFSET {offset} ")
        self.client.execute(f"""
        INSERT INTO kafka_topics_offset_control (kafka_topic, last_offset) 
        VALUES ('{self.topic}',  {offset})
        """)
    
def insert_bronze(data):
    client = Minio("minio:9000",
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure = False
    )
    found = client.bucket_exists(BUCKET_NAME)
    if not found:
        client.make_bucket(BUCKET_NAME)
        print("Created bucket", BUCKET_NAME)
    json_data = json.dumps(data)
    json_bytes = BytesIO(json_data.encode('utf-8'))
    now = datetime.now()
    object_name = f'KAFKA/topic={TOPIC}/year={now.year}/month={now.month}/day={now.day}/{now.strftime("%Y-%m-%d %H:%M:%S")}.json'
    try:
        client.put_object(
            BUCKET_NAME,
            object_name,
            data=json_bytes,
            length=len(json_data),
            content_type="application/json"
        )
        print(f'JSON {object_name} enviado para o bucket {BUCKET_NAME} com sucesso!')
    except S3Error as e:
        print(f'Erro ao enviar arquivo: {e}')    
    
    return object_name



def consume_kafka():
    print(f"CHAVES: SECRET {SECRET_KEY}, ACCESS: {ACCESS_KEY}")
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[BOOTSTRAP],  # Servidor Kafka
            enable_auto_commit=True,  # Para commit automático das mensagens
            consumer_timeout_ms=10000,  #fica aguardando topicos por 10 segundos
            value_deserializer=lambda x: x.decode('utf-8')  # Deserializa as mensagens para string
        )
        # Especificar a partição e o offset inicial
        topic_controll = TopicOffsetControll(TOPIC)
        partition = 0  # Partição que deseja consumir
        offset_inicial = topic_controll.get_offset()  # Offset a partir do qual deseja começar a ler
        
        # Atribuir a partição ao consumidor
        tp = TopicPartition(TOPIC, partition)
        consumer.assign([tp])
        
        # Definir o offset inicial manualmente
        consumer.seek(tp, offset_inicial)
        
        print("Aguardando mensagens...")
        
        # Consome mensagens do tópico
        message_list = []
        for message in consumer:
            print(f"Recebido: {message.value}")
            message_list.append(json.loads(message.value))
        if len(message_list) > 0:
            file = insert_bronze(message_list)
            topic_controll.insert_offset(message.offset)
            print(f"------------------ARQUIVO SALVO {file}")
            return file
        else:
            return None
    
    except Exception as e:
        print(e)
        raise Exception(e)
        #create_log(status = 'failure', message = e)

import polars as pl
from datetime import date
from clickhouse_driver import Client

# Conexão ao servidor ClickHouse
client_ch = Client(
    host='clickhouse',       # Endereço do servidor ClickHouse
    port=9000,              # Porta padrão para conexões TCP
    user='default',         # Usuário (padrão é 'default')
    password='',            # Senha (por padrão, vazio)
    database='default',     # Nome do banco de dados (por padrão, 'default')
)

def read_table():
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
    result = client_ch.execute(f"SELECT * FROM dim_clients where is_current = 1")
    columns = ['surrogate_key','nome', 'sobrenome', 'idade', 'email', 'data_cadastro',  'effective_date', 'end_date', 'is_current']
    # Criar o DataFrame Polars a partir dos resultados
    df_table = pl.DataFrame(result, schema=columns) 
    return df_table

def insert_dim_clients(content):
    #Criando dataframe polars com os dados a serem inseridos
    df_insert = pl.DataFrame(content)
    df_insert = df_insert.with_columns(
        pl.col("data_cadastro").str.strptime(pl.Date, format="%Y-%m-%d").alias("data_cadastro")
    ).with_columns(pl.lit(date.today()).alias("effective_date"))\
    .with_columns(pl.lit(date(2149, 6, 6)).alias("end_date"))\
    .with_columns(pl.lit(1).alias("is_current"))
    
    df_insert = df_insert.groupby("email").apply(
        lambda group: group.with_columns([
            pl.when(pl.col("data_cadastro") == pl.col("data_cadastro").max())
            .then(1)
            .otherwise(0)
            .alias("is_current"),

            pl.when(pl.col("data_cadastro") != pl.col("data_cadastro").max())
            .then(pl.col("effective_date"))
            .otherwise(pl.col("end_date"))
            .alias("end_date")
        ])
    )
    #lendo a tabela para fazer o scd2
    df_table = read_table()
    if df_table.is_empty():
        data = df_insert.to_dicts()
        
        # Inserir os dados no ClickHouse
        client_ch.execute(
            'INSERT INTO dim_clients (nome, sobrenome, idade, email, data_cadastro, effective_date, end_date, is_current) VALUES',
            [(row['nome'], row['sobrenome'], row['idade'], row['email'], row['data_cadastro'], row['effective_date'], row['end_date'], row['is_current']) for row in data]
        )
        print("Dados inseridos com sucesso")
    else:  
        """
        Nesse caso, eu utilizo o email como chave primaria (poderia ser id, cpf, etc...), faço o join com a tabela para ver os emails repetidos
        que estão sendo inseridos e filtro pela data de cadastro para garantir que os dados repetidos que estamos inserindo
        sao o cadastro mais atualizado.
        
        """
        df_joined = df_insert.join(
            df_table, 
            on="email", 
            how="inner"
        ).filter(
            pl.col("data_cadastro").cast(pl.Date) >= pl.col("data_cadastro_right").cast(pl.Date)
        )
        
        # Linhas a serem inativadas com is_current = 0 e fechamento do end date
        df_update_inactive = df_joined.select([
            pl.col("surrogate_key"),
            pl.col("nome"),
            pl.col("sobrenome"),
            pl.col("idade"),
            pl.col("email"),
            pl.col("data_cadastro_right").alias("data_cadastro"),
            pl.col("effective_date_right").alias("effective_date"),
            pl.col("effective_date").alias("end_date"),
            pl.lit(0).alias("is_current")
        ])
        for row in df_update_inactive.to_dicts():
            client_ch.execute(
                """
                ALTER TABLE dim_clients
                UPDATE end_date = %(end_date)s,
                       is_current = %(is_current)s
                WHERE surrogate_key = %(surrogate_key)s
                """,
                {'end_date': row['end_date'], 'is_current': row['is_current'], 'surrogate_key': row['surrogate_key']}
            )
        
        """ 
        caso onde o cliente a ser inserido ja esta na base mas tem a data de cadastro menor do
        que a já cadastrada, nesse caso, o dado é inserido mas o dado ja persistido
        é mantido como o dado corrente porque tem a data de cadastro maior
        
        """
        df_update_inactive_new  = df_insert.join(
            df_table, 
            on="email", 
            how="inner"
        ).filter(
            pl.col("data_cadastro").cast(pl.Date) < pl.col("data_cadastro_right").cast(pl.Date)
        ).select([
            pl.col("nome"),
            pl.col("sobrenome"),
            pl.col("idade"),
            pl.col("email"),
            pl.col("data_cadastro"),
            pl.col("effective_date"),
            pl.col("effective_date").alias("end_date"),
            pl.lit(0).alias("is_current")
        ])
        df_insert = df_insert.join(
            df_update_inactive_new, 
            on="email", 
            how="left"
        ).select([
            pl.col("nome"),
            pl.col("sobrenome"),
            pl.col("idade"),
            pl.col("email"),
            pl.col("data_cadastro"),
            pl.when(pl.col("nome_right").is_not_null()).then(pl.col("effective_date_right")).otherwise(pl.col("effective_date")).alias("effective_date"),
            pl.when(pl.col("nome_right").is_not_null()).then(pl.col("end_date_right")).otherwise(pl.col("end_date")).alias("end_date"),
             pl.when(pl.col("nome_right").is_not_null()).then(pl.col("is_current_right")).otherwise(pl.col("is_current")).alias("is_current"),
        ])
        # Converta o DataFrame para um formato que pode ser enviado ao ClickHouse
        data = df_insert.to_dicts()
        print("dados a serem inseridos:")
        print(data)
        # Inserir os dados no ClickHouse
        client_ch.execute(
            'INSERT INTO dim_clients (nome, sobrenome, idade, email, data_cadastro, effective_date, end_date, is_current) VALUES',
            [(row['nome'], row['sobrenome'], row['idade'], row['email'], row['data_cadastro'], row['effective_date'], row['end_date'], row['is_current']) for row in data]
        )
        
def read_file_from_bronze(**kwargs):
    file_path = kwargs['ti'].xcom_pull(task_ids='kafka_consume_topic_clients')
    print(f"LENDO ARQUIVO ---------- {file_path}")
    if file_path == None:
        return None
    
    client = Minio("minio:9000",
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure = False
    )
    
    try:
        data = client.get_object(BUCKET_NAME, file_path)
        content = json.load(data)
        print(f"Conteúdo do arquivo {file_path}:")
        print(content)
        insert_dim_clients(content)
    except S3Error as e:
        print(f'Erro ao ler o arquivo: {e}')
    
    return content

dag = DAG('consume_clients_kafka', description='Pipeline de ingestão',
          schedule_interval='* * * * *',
          start_date=datetime(2024, 8, 9), catchup=False)


consume_task = PythonOperator(
    task_id='kafka_consume_topic_clients', 
    python_callable=consume_kafka,
    dag=dag)

read_file_task = PythonOperator(
    task_id='read_file_from_minio',
    python_callable=read_file_from_bronze,
    provide_context=True,
    dag=dag
)

consume_task >> read_file_task