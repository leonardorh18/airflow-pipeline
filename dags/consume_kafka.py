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
        last_message = None
        # Consome mensagens do tópico
        message_list = []
        for message in consumer:
            print(f"Recebido: {message.value}")
            message_list.append(message.value)
        if len(message) > 0:
            insert_bronze(message.value)
            file = topic_controll.insert_offset(message.offset)
            return file
        else:
            return None
    
    except Exception as e:
        print(e)
        raise Exception(e)
        #create_log(status = 'failure', message = e)

def read_file_from_bronze(file_path):
    client = Minio("minio:9000",
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure = False
    )
    
    try:
        data = client.get_object(BUCKET_NAME, file_path)
        content = data.read().decode('utf-8')
        print(f"Conteúdo do arquivo {file_path}:")
        print(content)
    except S3Error as e:
        print(f'Erro ao ler o arquivo: {e}')
    
    return content

dag = DAG('consume_clients_kafka', description='Pipeline de ingestão',
          schedule_interval='* * * * *',
          start_date=datetime(2024, 8, 9), catchup=False)


consume_task = PythonOperator(task_id='kafka_consume_topic_clients', python_callable=consume_kafka, dag=dag)
read_file_task = PythonOperator(
    task_id='read_file_from_minio',
    python_callable=read_file_from_bronze,
    op_args=[consume_task.output],  # Passa o caminho do arquivo retornado pela `consume_task`
    dag=dag
)

consume_task >> read_file_task