#### Nota do Desenvolvedor ####
#       Nome: Vinicius Henrique dos Santos
#       Objetivo: Deonstracao de uma dag simples, sem funcionalidades para entender a mecanica de desenvolvimento dentro da ferramenta

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum


####  Especificacoes dos argumentos da DAG ####
#       owner: desenvolvedor do pipeline
#       depends_on_past: esse parametro nos permite amarrar a execucao de uma task ao sucesso dessa mesma task na execucao anterior
#                        por padrao esse argumento eh False, sao casos muito pontuais que demandam esse parametro ativado
#       start_date: indica a data em que essa dag sera executada, pode ser passada pelo padrão datetime(2024, 12, 31) ou usando a funcao
#                   now da biblioteca pendulum, usando dessa segunda forma a dag ja ira executar assim que entrar em producao 
#       email_on_failure e email_on_retry: parametro para envio de notificacao em caso de falhas, seguindo o padrao 
#                                          ['email1@example.com', 'email2@example.com']
#       retries: permite a parametrizacao do numero de tentativas de execucao em caso de falha nas tarefas
#       retry_delay: intervalo de tempo entre as tentativas de execução de uma tarefa

def_dag = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.now('UTC').subtract(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='seu_primeiro_dag',
    default_args=def_dag,
    schedule= '@daily'
) as dag:
     
#### Especificacoes das tarefas com  EmptyOperator ####
#       task_id: identificador unico da task dentro da dag

    tarefa_1 = EmptyOperator(task_id = 'tarefa_1')
    tarefa_2 = EmptyOperator(task_id = 'tarefa_2')
    tarefa_3 = EmptyOperator(task_id = 'tarefa_3')
    tarefa_4 = EmptyOperator(task_id = 'tarefa_4')
    tarefa_5 = EmptyOperator(task_id = 'tarefa_5')

    tarefa_1 >> [tarefa_2, tarefa_3, tarefa_4] 
    [tarefa_2, tarefa_3, tarefa_4]  >> tarefa_5