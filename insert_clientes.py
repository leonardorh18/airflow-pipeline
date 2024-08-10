from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='kafka:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Dados fictícios de clientes
clientes = [
    {"nome": "João", "sobrenome": "Silva", "idade": 34, "email": "joao.silva@email.com", "data_cadastro": "2024-08-09"},
    {"nome": "Maria", "sobrenome": "Oliveira", "idade": 29, "email": "maria.oliveira@email.com", "data_cadastro": "2024-08-09"},
    # Adicione mais registros conforme necessário
]

for cliente in clientes:
    producer.send('teste', cliente)
