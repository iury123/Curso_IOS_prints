import boto3
import time

aws_access_key_id = ''
aws_secret_access_key = ''

emr_client = boto3.client('emr', 
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key,
                          region_name='SUA-REGIAO') 

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
        'Ec2KeyName': 'key-pair-fiap-v2',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-011a04b0f3a5cd20b',
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
    LogUri='s3://SEU_BUCKET/logs',
    StepConcurrencyLevel=10,
)

print('Aguarde 10 minutos para o provisionamento do Cluster!')
time.sleep(600)

cluster_id = cluster_response['JobFlowId']
print(f'Cluster criado com o ID: {cluster_id}')

