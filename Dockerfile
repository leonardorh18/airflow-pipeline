FROM apache/airflow:2.9.3-python3.10

# Instalar as dependências necessárias
USER root
ENV ACCESS_KEY='minioadmin'
ENV SECRET_KEY='minioadmin'
RUN apt-get update && apt-get install -y \
    python3-pip \
    && apt-get clean

USER airflow
COPY requirements.txt .

# Instalar bibliotecas Python necessárias
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

