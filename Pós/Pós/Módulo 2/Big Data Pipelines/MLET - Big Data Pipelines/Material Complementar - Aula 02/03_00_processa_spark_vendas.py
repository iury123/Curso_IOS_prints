
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, current_timestamp

output_path = 's3://SEU_BUCKET/gold/vendas/'

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("ProcessaVendas")\
        .getOrCreate()

df_vendas = spark.read.csv('s3://SEU_BUCKET/vendas.csv', header=True, inferSchema=True)
df_produtos = spark.read.csv('s3://SEU_BUCKET/produtos.csv', header=True, inferSchema=True)
df_usuarios = spark.read.csv('s3://SEU_BUCKET/usuarios.csv', header=True, inferSchema=True)

df_vendas_usuarios = df_vendas.join(df_usuarios, df_vendas["ID_USUARIO"] == df_usuarios["ID"], "left")
df_final = df_vendas_usuarios.join(df_produtos, df_vendas_usuarios["ID_PRODUTO"] == df_produtos["ID_PRODUTO"], "left")

df_final = df_final.select(
    df_vendas["ID_VENDA"],
    df_vendas["ID_USUARIO"],
    df_vendas["ID_PRODUTO"],
    df_vendas["QUANTIDADE"],
    df_usuarios["NOME"].alias("NOME_USUARIO"),
    df_usuarios["IDADE"],
    df_usuarios["EMAIL"],
    df_produtos["NOME_PRODUTO"],
    df_produtos["PRECO"]
)

df_final = df_final.withColumn("VALOR_VENDA", col("QUANTIDADE") * col("PRECO"))
df_final = df_final.withColumn("COMISSAO", when(col("ID_PRODUTO") <= 25, col("VALOR_VENDA") * 0.10).otherwise(col("VALOR_VENDA") * 0.13))
df_final = df_final.withColumn("DH_PROCESSAMENTO", current_timestamp())

df_final.write.csv(output_path, mode="overwrite", header=True)
df_final.write.parquet(output_path, mode="overwrite")

