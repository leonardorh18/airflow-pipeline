from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from kafka import KafkaConsumer
import boto3
import clickhouse_driver
import json

def consume_and_store():
    print("RODANDO ***********************")


dag = DAG('pipeline_clients', description='Pipeline de Cadastro de Clientes',
          schedule_interval='@daily',
          start_date=datetime(2024, 8, 9), catchup=False)

consume_task = PythonOperator(task_id='consume_and_store', python_callable=consume_and_store, dag=dag)

consume_task
