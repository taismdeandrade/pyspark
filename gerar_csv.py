from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from dateutil.relativedelta import relativedelta

spark = (
    SparkSession.builder
    .master('local')
    .appName('dados')
    .getOrCreate()
)

# Conecta ao DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    region_name='sa-east-1',
    aws_access_key_id=userdata.get('access_key'),
    aws_secret_access_key=userdata.get('secret_access_key')
)
tabela = dynamodb.Table('teste')

# Consulta dados
response = tabela.query(
    KeyConditionExpression=Key('PK').eq('USER#133c8a5a-e0f1-7009-a518-15bdbe0c845a') & Key('SK').begins_with('ITEM#'),
    FilterExpression=Attr('Status').eq('todo')
)
items = response['Items']

# Define esquema do DataFrame
schema = StructType([
    StructField('PK', StringType(), False),
    StructField('SK', StringType(), False),
    StructField('Nome', StringType(), False),
    StructField('Data', StringType(), False),
    StructField('Status', StringType(), False),
    StructField('Tipo', StringType(), False)
])

# Cria DataFrame
df_itens = spark.createDataFrame(items, schema=schema)
df_itens = df_itens.withColumn("Data", to_date(col("Data"), "yyyy-MM-dd"))

# Adiciona coluna AnoMes no formato "2025-06"
df_itens = df_itens.withColumn("AnoMes", date_format(col("Data"), "yyyy-MM"))

# Cria lista dos últimos 6 meses
hoje = datetime.today()
ultimos_meses = [(hoje - relativedelta(months=i)).strftime("%Y-%m") for i in reversed(range(6))]

# Filtra registros dos últimos 6 meses
df_filtrado = df_itens.filter(col("AnoMes").isin(ultimos_meses))

# Conta quantos registros por tipo e mês
df_agrupado = df_filtrado.groupBy("Tipo", "AnoMes").count()

df_abandonados = df_agrupado.filter(
    ((col("Tipo") == "Tarefa a Ser Feita") & (datediff(current_date(), col("AnoMes")) > 15)) |
    ((col("Tipo") == "Item de Compra") & (datediff(current_date(), col("AnoMes")) > 30))
)

print("Contagem por tipo nos últimos 6 meses:")
df_abandonados.show(truncate=False)

df_pivotado = (
    df_agrupado.groupBy("Tipo")
    .pivot("AnoMes", ultimos_meses)
    .agg(first("count"))
    .fillna(0)
    .orderBy("Tipo")
)

print("Contagem por tipo nos últimos 6 meses:")
df_pivotado.show(truncate=False)
df_relatorio = df_pivotado.toPandas()
df_relatorio.to_csv('relatorio.csv', index=False)