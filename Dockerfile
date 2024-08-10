FROM puckel/docker-airflow:1.10.9

# Instalar as dependências necessárias
USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    && apt-get clean

# Instalar bibliotecas Python necessárias
RUN pip install --upgrade pip
RUN pip install kafka-python boto3 clickhouse-driver 

# Alterar permissões do diretório
RUN chown -R airflow:airflow /usr/local/airflow

USER airflow

# Criar diretório para DAGS
COPY ./dags /usr/local/airflow/dags
