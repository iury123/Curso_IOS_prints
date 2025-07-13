import boto3
import time
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime, timedelta
import pendulum
import os

aws_access_key_id= ''
aws_secret_access_key= ''
v_regiao= ''

def f_upload_to_s3(aws_access_key_id, aws_secret_access_key, v_regiao):

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=v_regiao
    )
    v_nm_bucket = 'SEU-BUCKET'
    v_caminho_s3 = 'bronze/'

    arquivos = [
        ('/home/airflow/Documents/airflowFIAP/codigos/produtos.csv', v_caminho_s3 + 'produtos.csv'),
        ('/home/airflow/Documents/airflowFIAP/codigos/usuarios.csv', v_caminho_s3 + 'usuarios.csv'),
        ('/home/airflow/Documents/airflowFIAP/codigos/vendas.csv', v_caminho_s3 + 'vendas.csv')
    ]

    for file_path, s3_file_name in arquivos:
        try:
            s3.upload_file(file_path, v_nm_bucket, s3_file_name)
            print(f"Arquivo {file_path} carregado com sucesso para {v_nm_bucket}/{s3_file_name}")
        except Exception as e:
            print(f"Erro ao carregar o arquivo {file_path}:", e)

def f_provisiona_claster_emr (aws_access_key_id, aws_secret_access_key, v_regiao):
    emr_client = boto3.client('emr', 
                              aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key,
                              region_name=v_regiao) 

    cluster_response = emr_client.run_job_flow(
        Name='emr-job-fiap',
        ReleaseLabel='emr-7.0.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Core nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Task nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'TASK',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                }
            ],
            'Ec2KeyName': 'SEU-KEYPAIR',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-SUA-SUBNET',
        },
        Applications=[
            {'Name': 'Hadoop'},
            {'Name': 'Hive'},
            {'Name': 'Spark'},
            {'Name': 'Presto'},
            {'Name': 'JupyterEnterpriseGateway'}
        ],
        Configurations=[
            {
                'Classification': 'spark-hive-site',
                'Properties': {
                    'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                }
            }
        ],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='regras-emr',
        VisibleToAllUsers=True,
        LogUri='s3://SEU-BUCKET/logs',
        StepConcurrencyLevel=10,
    )

    print('Aguarde 10 minutos para o provisionamento do Cluster!')
    time.sleep(600)

    cluster_id = cluster_response['JobFlowId']
    print(f'Cluster criado com o ID: {cluster_id}')
            
def f_simula_cicd (aws_access_key_id, aws_secret_access_key, v_regiao):

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=v_regiao
    )
    v_nm_bucket = 'SEU-BUCKET'
    v_caminho_s3 = 'cod/'

    arquivos = [
        ('/home/airflow/Documents/airflowFIAP/codigos/03_00_processa_spark_vendas.py', v_caminho_s3 + 'processa_spark_vendas.py')
    ]

    for file_path, s3_file_name in arquivos:
        try:
            s3.upload_file(file_path, v_nm_bucket, s3_file_name)
            print(f"Arquivo {file_path} carregado com sucesso para {v_nm_bucket}/{s3_file_name}")
        except Exception as e:
            print(f"Erro ao carregar o arquivo {file_path}:", e)

def f_spark_submit (aws_access_key_id, aws_secret_access_key, v_regiao):

    emr_client = boto3.client('emr', 
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=v_regiao)

    clusters = emr_client.list_clusters(
        ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
    )

    if clusters['Clusters']:
        cluster_id = clusters['Clusters'][0]['Id']
    else:
        print("Não há clusters ativos.")
        cluster_id = None

    spark_step = {
        'Name': 'FIAP - DataPipelines - processa vendas',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                's3://SEU-BUCKET/cod/processa_spark_vendas.py',
            ]
        }
    }

    action = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[spark_step])
    print(f"Added step with ID: {action['StepIds'][0]}")

    step_id = action['StepIds'][0]

    def f_verifica_job_status(client, cluster_id, step_id):
        while True:
            step_status = client.describe_step(ClusterId=cluster_id, StepId=step_id)
            status = step_status['Step']['Status']['State']
            print(f"Status do Step {step_id} no Cluster {cluster_id}: {status}")

            if status == "COMPLETED":
                print(f"Step {step_id} no Cluster {cluster_id} concluído com sucesso.")
                break
            elif status in ["CANCELLED", "FAILED", "INTERRUPTED"]:
                raise Exception(f"Erro crítico: Step {step_id} no Cluster {cluster_id} terminou com o status: {status}.")
            else:
                print(f"Aguardando o step {step_id} no Cluster {cluster_id} ser concluído...")
                time.sleep(30)  

    f_verifica_job_status(emr_client, cluster_id, step_id)

def f_encerra_cluster_EMR(aws_access_key_id, aws_secret_access_key, v_regiao):

    emr_client = boto3.client('emr', 
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=v_regiao)
    
    clusters = emr_client.list_clusters(
        ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
    )

    for cluster in clusters['Clusters']:
        print(cluster['Id'])
        cluster_id=cluster['Id']
        emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        print(f'Cluster {cluster_id} solicitado para terminar.')   

### INICIA DEFINICAO DA DAG ###

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'pipeline_vendas',
    default_args=default_args,
    description='Processa os dados dos produtores e entrega em um bucket s3',
    schedule_interval=timedelta(days=1),
)

### INICIA DEFINICAO DAS TASKS ###

task_01 = PythonOperator(
    task_id='upload_s3',
    python_callable=f_upload_to_s3,
    op_kwargs={
        'aws_access_key_id': aws_access_key_id,
        'aws_secret_access_key': aws_secret_access_key,
        'v_regiao': v_regiao
    },
    dag=dag,
)

task_02 = PythonOperator(
    task_id='provisiona_cluster_EMR',
    python_callable=f_provisiona_claster_emr,
    op_kwargs={
        'aws_access_key_id': aws_access_key_id,
        'aws_secret_access_key': aws_secret_access_key,
        'v_regiao': v_regiao
    },
    dag=dag,
)

task_03 = PythonOperator(
    task_id='cicd',
    python_callable=f_simula_cicd,
    op_kwargs={
        'aws_access_key_id': aws_access_key_id,
        'aws_secret_access_key': aws_secret_access_key,
        'v_regiao': v_regiao
    },
    dag=dag,
)

task_04 = PythonOperator(
    task_id='spark_submit_EMR',
    python_callable=f_spark_submit,
    op_kwargs={
        'aws_access_key_id': aws_access_key_id,
        'aws_secret_access_key': aws_secret_access_key,
        'v_regiao': v_regiao
    },
    dag=dag,
)

task_05 = PythonOperator(
    task_id='encerra_cluster_EMR',
    python_callable=f_encerra_cluster_EMR,
    op_kwargs={
        'aws_access_key_id': aws_access_key_id,
        'aws_secret_access_key': aws_secret_access_key,
        'v_regiao': v_regiao
    },
    dag=dag,
)

[task_01, task_02,task_03] >> task_04 >> task_05
