### Verifica Topicos #############################################################################################
#   Autor: Vinicius Henrique dos Santos
#   Empresa: FIAP
#   Objetivo: Esse script tem como objetivo fazer a configuracao inicial dos topicos no Kafka, quando o topico 
#             ja existir, ele sera destruido e criado novamente. 
#             Caso nao deseje detruir e recriar, apenas listar os topicos, mude o parametro p_recria = 0
#             Caso o parametro p_limpa_topicos = 1 todos os topicos do kafka serao deletados permanentemente
##################################################################################################################

from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
from confluent_kafka import KafkaException

#Parametros
topic_name = 'dev_log_sensor'  # Substitua
partitions = 1
replication_factor = 1
p_recria = 1  # 0 ou 1


admin_config = {
    'bootstrap.servers': 'localhost:9092',  
}

admin_client = AdminClient(admin_config)

topic_metadata = admin_client.list_topics(timeout=10)

print("Topicos no cluster Kafka:")
for topic in topic_metadata.topics:
    print(topic)

def f_cria_topico(admin_client, topic_name, partitions, replication_factor):
    new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replication_factor)
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()  # O metodo result ira bloquear ate que o topico seja criado
            print(f"Topico {topic} criado com sucesso.")
        except Exception as e:
            print(f"Falha na criacao do topico {topic}: {e}")

def delete_topic(admin_client, topic_name):
    fs = admin_client.delete_topics([topic_name])
    for topic, f in fs.items():
        try:
            f.result()  # O metodo result ira bloquear ate que o topico seja excluido
            print(f"Topico {topic} excluido com sucesso.")
        except Exception as e:
            print(f"Falha na exclusao do topico {topic}: {e}")

def topic_exists(admin_client, topic_name):
    topic_metadata = admin_client.list_topics(timeout=5)
    return topic_name in topic_metadata.topics

if topic_exists(admin_client, topic_name):
    print(f"Topico '{topic_name}' ja existe.")
    if p_recria:
        print("Recriando o topico...")
        delete_topic(admin_client, topic_name)
        f_cria_topico(admin_client, topic_name, partitions, replication_factor)
    else:
        print("Mantendo o topico existente.")
else:
    print(f"Topico '{topic_name}' nao existe. Criando topico...")
    f_cria_topico(admin_client, topic_name, partitions, replication_factor)