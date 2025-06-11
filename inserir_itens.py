import time

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Attr, Key
from google.colab import userdata

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local").appName("dados").getOrCreate()

dados = spark.read.csv("dados_importar.csv", header=True, inferSchema=True, sep=";")

nome_usuario = userdata.get("nome_usuario")
id_usuario = userdata.get("id_usuario")
novo_id = userdata.get("novo_id")
nome_tabela = userdata.get("nome_tabela")

dados = (
    dados.withColumnRenamed("Nome da Tarefa", "Nome")
    .withColumnRenamed("Status", "Status Id")
    .withColumnRenamed("Status descrição", "Status")
    .withColumnRenamed("Tipo da Tarefa", "Tipo")
    .withColumnRenamed("Data de Criação", "Data")
)
dados = dados.withColumn("Data", col("Data").cast("date"))
dados = dados.withColumn("Data", col("Data").cast("string"))
dados = dados.filter(dados["Usuário"] == nome_usuario)

dados = (
    dados.withColumn("Status", regexp_replace("Status", "Concluído", "done"))
    .withColumn("Status", regexp_replace("Status", "A Fazer", "todo"))
    .withColumn(
        "ID do Usuário",
        regexp_replace("ID do Usuário", id_usuario, novo_id),
    )
)

dados = dados.filter(~dados.Status.isin(["Cancelado"]))

dados = dados.withColumn("PK", concat(lit("USER#"), col("ID do Usuário")))

dados = dados.withColumn("item_id", expr("uuid()"))
dados = dados.withColumn(
    "SK",
    concat(
        lit("ITEM#"),
        col("item_id"),
        lit("LIST#"),
        date_format(col("Data"), "yyyy-MM-dd"),
    ),
)

dados_inserir = dados.select("Nome", "Data", "Status", "Tipo", "PK", "SK").toPandas()
dados = dados_inserir.to_dict(orient="records")

dynamodb = boto3.resource(
    "dynamodb",
    region_name=userdata.get("region_name"),
    aws_access_key_id=userdata.get("access_key"),
    aws_secret_access_key=userdata.get("secret_access_key"),
)
tabela = dynamodb.Table(nome_tabela)

delay = 2
limite = 100

try:
    with tabela.batch_writer() as batch:
        for i, item in enumerate(dados, 1):
            batch.put_item(Item=item)
            if i % limite == 0:
                time.sleep(delay)
    print(f"Sucesso! {len(dados)} itens processados.")
except Exception as e:
    print(f"Erro inesperado: {str(e)}")
