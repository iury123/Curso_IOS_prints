### Producer Vendas ##############################################################################################
#   Autor: Vinicius Henrique dos Santos
#   Empresa: FIAP
#   Objetivo: Esse script tem como objetivo simular um sistema de vendas que envia mensagem para um tópico Kafka
##################################################################################################################

import json
import random
import time
from faker import Faker
from datetime import datetime
from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092',  
}

producer = Producer(config)
topic = 'vendas'  # Esse topico precisa existir previamente

fake = Faker()

def f_envia_mensagens(err, msg):
    if err is not None:
        print(f'Envio da mensagem falhou: {err}')
    else:
        print(f'Mensagem entregue para o Kafka no tópico {msg.topic()} - particao [{msg.partition()}]')

def f_simulador_vendas():
    user = fake.simple_profile()
    venda = {
        'id_venda': fake.uuid4(),
        "id_produto": random.choice(['prod01', 'prod02', 'prod03', 'prod04', 'prod05', 'prod06', 'prod07', 'prod08', 'prod09', 'prod10']),
        "nm_produto": random.choice(['notebook', 'tv', 'celular', 'monitor', 'mouse', 'teclado', 'cadeira', 'xbox', 'fonte de energia', 'playstation']),
        'nm_categoria': random.choice(['eletronico', 'cosmetico', 'casa', 'eletrodomestico', 'auto', 'mobilia', 'pet', 'bebidas']),
        'nm_marca': random.choice(['apple', 'samsung', 'brastemp', 'lenovo', 'microsoft', 'sony', 'dell', 'eletrolux']),
        'vendedor': user['username'],
        'valor': round(random.uniform(0.1, 100000.0), 2),
        'quantidade': random.randint(1, 10),
        "tp_pagamento": random.choice(['pix', 'credito a vista', 'credito parcelado', 'boleto']),        
        'data': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    }
    return venda

try:
    while True:
        dados_venda = f_simulador_vendas()
        id_venda = dados_venda['id_venda']
        dados_venda_json = json.dumps(dados_venda).encode('utf-8')
        producer.produce(topic, key=id_venda.encode('utf-8'), value=dados_venda_json, callback=f_envia_mensagens)
        producer.poll(0)
        time.sleep(10)

except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    print("Simulação de streaming de dados de venda concluída.")

