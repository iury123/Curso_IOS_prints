import boto3
import pandas as pd

aws_access_key_id = ''
aws_secret_access_key = ''

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='sa-east-1'
)

v_nm_bucket = 'SEU_BUCKET'
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


