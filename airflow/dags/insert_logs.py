from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
from minio import Minio
from minio.error import S3Error
from airflow.models import Variable
ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")

BUCKET_NAME = 'bronze-layer'

def copy_logs_to_minio(**context):
    
    # Caminho dos logs no Airflow
    log_base_path = "/usr/local/airflow/logs/consume_clients_kafka/upload_logs_to_minio/2024-08-11T23:05:00+00:00/1.log"  # Ajuste conforme necessário
    dag_id = context['dag'].dag_id
    
    execution_date = context['ts_nodash']
    print(context)
    print(str(dag_id), execution_date,  context['ti'].log)
    
    log_dir = os.path.join(log_base_path, dag_id, execution_date)
    print(f"log_dir {log_dir}")
    # Inicializa o cliente do MinIO
    minio_client = Minio("minio:9000",
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure = False
    )
    
    # Verifica se o bucket existe, se não, cria
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
    
    # Caminha pelos arquivos de log e faz upload para o MinIO
    for root, dirs, files in os.walk(log_dir):
        for file in files:
            file_path = os.path.join(root, file)
            s3_key = f"airflow_logs/{dag_id}/{execution_date}/{file}"
            try:
                minio_client.fput_object(BUCKET_NAME, s3_key, file_path)
                print(f"Arquivo {file_path} copiado para {s3_key} no MinIO")
            except S3Error as e:
                print(f"Erro ao copiar {file_path} para {s3_key} no MinIO: {e}")

