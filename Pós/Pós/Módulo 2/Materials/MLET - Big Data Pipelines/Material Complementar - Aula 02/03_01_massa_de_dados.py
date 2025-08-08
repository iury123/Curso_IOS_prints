import pandas as pd
import numpy as np

# Configuração inicial
np.random.seed(0)  # Para resultados reproduzíveis

# Gerando dados de usuários
usuarios = pd.DataFrame({
    'ID': range(1, 101),  # 100 usuários
    'NOME': [f'Usuário_{i}' for i in range(1, 101)],
    'IDADE': np.random.randint(18, 70, size=100),
    'EMAIL': [f'usuario_{i}@fiap-eng-ml-datapipelines.com' for i in range(1, 101)]
})

# Gerando dados de produtos
produtos = pd.DataFrame({
    'ID_PRODUTO': range(1, 51),  # 50 produtos
    'NOME_PRODUTO': [f'Produto_{i}' for i in range(1, 51)],
    'PRECO': np.random.uniform(10, 500, size=50).round(2)
})

# Gerando dados de vendas com 50.000 linhas
vendas = pd.DataFrame({
    'ID_VENDA': range(1, 15000001),  # 500.000 vendas
    'ID_USUARIO': np.random.choice(usuarios['ID'], size=15000000),
    'ID_PRODUTO': np.random.choice(produtos['ID_PRODUTO'], size=15000000),
    'QUANTIDADE': np.random.randint(1, 10, size=15000000)
})

# Exibindo as primeiras linhas dos DataFrames
#print("Usuários:\n", usuarios.head())
#print("\nProdutos:\n", produtos.head())
#print("\nVendas:\n", vendas.head())

# Exportando os DataFrames para arquivos CSV

# Caminhos para os arquivos CSV
caminho_csv_usuarios = '/home/airflow/Documents/airflowFIAP/codigos/usuarios.csv'
caminho_csv_produtos = '/home/airflow/Documents/airflowFIAP/codigos/produtos.csv'
caminho_csv_vendas = '/home/airflow/Documents/airflowFIAP/codigos/vendas.csv'

# Exportando cada DataFrame para um arquivo CSV
usuarios.to_csv(caminho_csv_usuarios, index=False)
produtos.to_csv(caminho_csv_produtos, index=False)
vendas.to_csv(caminho_csv_vendas, index=False)




