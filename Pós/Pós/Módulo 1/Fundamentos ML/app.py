import investpy

api_key = 'LIUOV7WGO2Y7H90E'

# Obter dados de ação
df = investpy.obter_dados_acao('AAPL', api_key)

# Calcular retorno diário
df = investpy.calcular_retorno_diario(df)

# Plotar dados da ação
investpy.plotar_dados_acao(df)
