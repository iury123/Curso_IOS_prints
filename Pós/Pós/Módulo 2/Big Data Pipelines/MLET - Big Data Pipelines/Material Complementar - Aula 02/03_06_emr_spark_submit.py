import boto3
import time

aws_access_key_id = ''
aws_secret_access_key = ''
emr_client = boto3.client('emr', 
                          region_name='sa-east-1', 
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key,)

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
            's3://SEU_BUCKET/cod/processa_spark_vendas.py',
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