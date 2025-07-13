#### Nota do Desenvolvedor ####
#       Nome: Vinicius Henrique dos Santos
#       Objetivo: Transformar o script criado para realizar o download das informacoes de cotacoes em uma dag que seja executada 1x por semana

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import pendulum
import os

#### Codigo Python ####
#       Lembre-se: para transformar seu script Python em uma DAG, precisamos que ele seja alterado para funcionar como uma funcao

dt_hoje_str = datetime.now().strftime('%Y%m%d') 

caminho_input= '/home/airflow/Documents/airflowFIAP/codigos/tickers.csv'
caminho_output= os.path.join('home/airflow/Documents/airflowFIAP/codigos/acoes', dt_hoje_str) 

def download_acoes (**kwargs):
    upload_arquivo = kwargs['caminho_input']
    df_acoes = pd.read_csv(upload_arquivo)

    end_date = datetime.now()
    start_date = end_date - timedelta(days=15)

    for ticker in df_acoes['Ticker']:
        df_dados_bolsa = yf.download(ticker, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))

        df_dados_bolsa.index = pd.to_datetime(df_dados_bolsa.index)
        ultimo_dia_pregao = df_dados_bolsa.index.max()
        sete_dias_atras = ultimo_dia_pregao - timedelta(days=7) #7 dias pois nao temos pregoes aos finais de semana
        mascara_data = (df_dados_bolsa.index > sete_dias_atras) & (df_dados_bolsa.index <= ultimo_dia_pregao)
        
        df_ultimos_5_pregoes = df_dados_bolsa.loc[mascara_data].copy()

        #Criando duas novas variaveis para entregar prontas aos consumidores:

        df_ultimos_5_pregoes.loc[:, 'Amplitude_do_Preco'] = df_ultimos_5_pregoes['High'] - df_ultimos_5_pregoes['Low']
        df_ultimos_5_pregoes.loc[:, 'Total_RS_Negociado'] = df_ultimos_5_pregoes['Close'] * df_ultimos_5_pregoes['Volume']

        nome_arquivo = f"{ticker.replace('.SA', '')}_ultimos_5_pregoes.csv"
        df_ultimos_5_pregoes.to_csv(os.path.join(caminho_output, nome_arquivo))
        
        print(f"Dados para {ticker} salvos em {nome_arquivo}")

def_dag = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.now('UTC').subtract(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_acoes_ibov',
    default_args=def_dag,
    schedule= '@weekly'
) as dag:
    
    task_01 = BashOperator(
        task_id = '01_cria_diretorio',
        bash_command = f'mkdir -p {caminho_output}',
        dag=dag
    )

    task_02 = PythonOperator(
    task_id='download_dados_ibov',
    python_callable=download_acoes,
    op_kwargs={'caminho_input': caminho_input,'caminho_output': caminho_output},
   )
    
    task_01 >> task_02 