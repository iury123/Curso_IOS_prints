import boto3

aws_access_key_id = ''
aws_secret_access_key = ''
emr_client = boto3.client('emr', 
                          region_name='SUA-REGIAO', 
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key,)


clusters = emr_client.list_clusters(
    ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
)

for cluster in clusters['Clusters']:
    print(cluster['Id'])
    cluster_id=cluster['Id']
    emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    print(f'Cluster {cluster_id} solicitado para terminar.')
