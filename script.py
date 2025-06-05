from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3

spark = (
    SparkSession.builder
    .master('local')
    .appName('dados')
    .getOrCreate()
)


dados = spark.read.csv('amostragem.csv', header=True, inferSchema=True, sep=';')

dados = dados.withColumnRenamed('Nome da Tarefa', 'Nome').withColumnRenamed('Status', 'Status Id').withColumnRenamed('Status descrição', 'Status').withColumnRenamed('Tipo da Tarefa', 'Tipo').withColumnRenamed('Data de Criação', 'Data')

dados = dados.withColumn("Data", col("Data").cast("string"))
dados = dados.filter(dados['Usuário'] == 'Jeferson Klau')

dados = dados.withColumn('Status', regexp_replace('Status', 'Concluído', 'done')).withColumn('Status', regexp_replace('Status', 'A Fazer', 'todo'))

dados = dados.filter(~dados.Status.isin(['Cancelado']))

dados = dados.withColumn("PK", concat(lit("USER#"), col("ID do Usuário")))

dados = dados.withColumn("item_id", expr("uuid()")) 
dados = dados.withColumn(
    "SK",
    concat(
        lit("ITEM#"),
        col("item_id"),
        lit("LIST#"),
        date_format(col("Data"), "yyyy-MM-dd")
    )
)

dados_inserir = dados.select('Nome','Data','Status', 'Usuário', 'PK', 'SK').limit(1000).toPandas().to_dict(orient="records")

# Conectando ao DynamoDB
dynamodb = boto3.resource("dynamodb", region_name="sa-east-1") 
tabela = dynamodb.Table("teste")

# Inserindo os registros
try:
    with tabela.batch_writer() as batch:
        for item in dados_inserir:
            batch.put_item(Item=item)
    print("Dados inseridos com sucesso no DynamoDB.")
except Exception as e:
    print(f"Erro ao inserir dados no DynamoDB: {e}")
