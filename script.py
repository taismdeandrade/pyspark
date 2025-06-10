from google.colab import userdata
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import pandas as pd
from boto3.dynamodb.conditions import Key, Attr


spark = (
    SparkSession.builder
    .master('local')
    .appName('dados')
    .getOrCreate()
)


dados = spark.read.csv('amostragem.csv', header=True, inferSchema=True, sep=';')

dados = dados.withColumnRenamed('Nome da Tarefa', 'Nome').withColumnRenamed('Status', 'Status Id').withColumnRenamed('Status descrição', 'Status').withColumnRenamed('Tipo da Tarefa', 'Tipo').withColumnRenamed('Data de Criação', 'Data')
dados = dados.withColumn('Data', col('Data').cast('date'))
dados = dados.withColumn("Data", col("Data").cast("string"))
dados = dados.filter(dados['Usuário'] == 'Jeferson Klau')

dados = dados.withColumn('Status', regexp_replace('Status', 'Concluído', 'done')).withColumn('Status', regexp_replace('Status', 'A Fazer', 'todo')).withColumn('ID do Usuário', regexp_replace('ID do Usuário', 'b4853fc1f03a3a4cec530a98a94d89ad', '133c8a5a-e0f1-7009-a518-15bdbe0c845a'))

dados = dados.filter(~dados.Status.isin(['Cancelado']))

dados = dados.withColumn("PK", concat(lit("USER#"), col("ID do Usuário")))

dados = dados.withColumn("item_id", expr("uuid()"))
dados = dados.withColumn("SK", concat(lit("ITEM#"), col("item_id"), lit("LIST#"), date_format(col("Data"), "yyyy-MM-dd")))
#dados.show()
dados_inserir = dados.select('Nome','Data','Status', 'Tipo', 'PK', 'SK').limit(1000).toPandas().to_dict(orient="records")

# Credenciais e região AWS

aws_secret_access_key = 'SEU_SECRET_KEY'
region_name = 'sa-east-1'

# Conecta ao DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    region_name=region_name,
    aws_access_key_id=userdata.get('access_key'),
    aws_secret_access_key=userdata.get('secret_access_key')
)
tabela = dynamodb.Table('teste')

#Inserindo os registros
try:
   with tabela.batch_writer() as batch:
        for item in dados_inserir:
            batch.put_item(Item=item)
        print("Dados inseridos com sucesso no DynamoDB.")
except Exception as e:
   print(f"Erro ao inserir dados no DynamoDB: {e}")
