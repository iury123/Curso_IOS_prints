### Consumidor Python + MySQL ######################################################################################################
#   Autor: Vinicius Henrique dos Santos
#   Empresa: FIAP
#   Objetivo: Esse script tem como objetivo simular um consumidor do topico de vendas do Kafka, eh responsavel por colocar os dados 
#             dentro do banco de dados MySQL server instalado e configurado previamente
####################################################################################################################################

import json
from confluent_kafka import Consumer, KafkaError
import mysql.connector
from mysql.connector import errorcode

kafka_config = {
    'bootstrap.servers': 'localhost:9092', 
    'group.id': 'grupo_vendas',
    'auto.offset.reset': 'earliest'
### dica para os alunos ############################################################################################################
#   O parametro 'auto.offset.reset' nos permite definir o comportamento do consumidor quando nao existe um offset valido ou o offset
#   atual nao existe mais no servidor (o que sugere que os dados foram excluidos do topico). 
#
#   Opcoes para esse parametro
#
#       earliest: redefine o offset para o menor disponivel na particao
#       latest: redefine o offset para ler apenas as mensagens novas apos o consumidor ser iniciado
#       none: essa opcao serve para que os dados nao sejam perdidos, caso nenhum offset seja encontrado para o consumidor 
#             receberemos uma mensagem de excecao
####################################################################################################################################
}       

consumer = Consumer(kafka_config)
consumer.subscribe(['vendas'])

#Essas configuracoes devem ser exatamente iguais as que voce configurou pelo material "saiba mais"
db_config = {
    'user': 'fiap',
    'password': 'fiap',
    'host': 'localhost',
    'database': 'db_datapipelines',
}

def f_conecta_mysql(db_config):
    try:
        cnx = mysql.connector.connect(**db_config)
        return cnx
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Erro de usuario ou senha: Verifique as configuracoes que voce realizou no SAIBA MAIS")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Erro de banco de dados: verifique o nome do banco de dados que voce criou no SAIBA MAIS")
        else:
            print(err)
    return None

def f_insere_dados_mysql(cnx, venda_data):
    cursor = cnx.cursor()
    add_venda = ("INSERT INTO tb_vendas "
                 "(id_venda, valor, quantidade, data_venda ) "
                 "VALUES (%s, %s, %s, %s)")
    cursor.execute(add_venda, venda_data)
    cnx.commit()
    cursor.close()

cnx = f_conecta_mysql(db_config)
if cnx is None:
    raise Exception("Erro ao tentar se conectar ao MySQL(reveja credenciais).")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

### Nota do professor ##########################################################################################################
#   Dois tipos de dados super comuns ao se trabalhar com Kafka sao Jason e Avro, eh fundamental que voce saiba transformar as 
#   informacoes lidas pelo consumidor e transformar em formato tabular, ou o contrario sempre que necessario
################################################################################################################################
        venda = json.loads(msg.value().decode('utf-8'))
        venda_data = (
            venda['id_venda'],
            venda['valor'],
            venda['quantidade'],
            venda['data']
        )

        f_insere_dados_mysql(cnx, venda_data)

except KeyboardInterrupt:
    print('Simulacao finalizada, usuario interrompeu via teclado')

finally:
    if cnx is not None:
        cnx.close()
    consumer.close()
