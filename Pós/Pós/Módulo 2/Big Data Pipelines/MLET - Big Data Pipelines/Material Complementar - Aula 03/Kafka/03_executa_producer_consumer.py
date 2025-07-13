### Executa produtores e consumidores #######################################################################
#   Autor: Vinicius Henrique dos Santos 
#   Empresa: FIAP
#   Objetivo: Esse script tem como objetivo executar simultaneamente os produtores e os executores, pois via 
#             tela pelo VsCode nao eh possivel, entao estamos mandando as execucoes em threads separadas 
#             dentro desse script.
#############################################################################################################

import threading
import subprocess

def task1():
    script_path = '/home/kafka/kafka/scripts/01_sensor_producer.py'
    subprocess.run(['python3', script_path])

def task2():
    script_path = '/home/kafka/kafka/scripts/02_consumidor_MySQL.py'
    subprocess.run(['python3', script_path])

thread1 = threading.Thread(target=task1)
thread2 = threading.Thread(target=task2)

thread1.start()
thread2.start()

thread1.join()
thread2.join()

print("Scripts executados")
