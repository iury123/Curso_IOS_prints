### Limpa Topicos ################################################################################################
#   Autor: Vinicius Henrique dos Santos
#   Empresa: FIAP
#   Objetivo: Esse script tem como objetivo apagar todos os topicos kafka quando necessario. p_limpa_topicos=1 
#             sinaliza que todos os topicos serao apagados
##################################################################################################################

from confluent_kafka.admin import AdminClient

admin_config = {
    'bootstrap.servers': 'localhost:9092',  
}

admin_client = AdminClient(admin_config)


p_limpa_topicos = 1  

def apaga_todos_os_topicos(admin_client):
    topic_metadata = admin_client.list_topics(timeout=10)
    topicos = list(topic_metadata.topics.keys())  


    fs = admin_client.delete_topics(topicos, operation_timeout=30)
    for topic, f in fs.items():
        try:
            f.result()  
            print(f"Tópico '{topic}' excluído com sucesso.")
        except Exception as e:
            print(f"Falha na exclusão do tópico '{topic}': {e}")

if p_limpa_topicos == 1:
    print("Apagando todos os tópicos...")
    apaga_todos_os_topicos(admin_client)
else:
    print("Manutenção de tópicos existentes.")
