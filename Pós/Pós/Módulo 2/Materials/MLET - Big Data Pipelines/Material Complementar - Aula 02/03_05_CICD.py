import boto3

aws_access_key_id = ''
aws_secret_access_key = ''

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='SUA-REGIAO'
)

v_nm_bucket = 'SEU_BUCKET'
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