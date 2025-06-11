from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Attr, Key
from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local").appName("dados").getOrCreate()

nome_tabela = userdata.get("nome_tabela")
id_usuario = userdata.get("id_usuario")

dynamodb = boto3.resource(
    "dynamodb",
    region_name=userdata.get("region_name"),
    aws_access_key_id=userdata.get("access_key"),
    aws_secret_access_key=userdata.get("secret_access_key"),
)

tabela = dynamodb.Table(nome_tabela)

response = tabela.query(
    KeyConditionExpression=Key("PK").eq("USER#{id_usuario}")
    & Key("SK").begins_with("ITEM#"),
    FilterExpression=Attr("Status").eq("todo"),
)
items = response["Items"]

schema = StructType(
    [
        StructField("PK", StringType(), False),
        StructField("SK", StringType(), False),
        StructField("Nome", StringType(), False),
        StructField("Data", StringType(), False),
        StructField("Status", StringType(), False),
        StructField("Tipo", StringType(), False),
    ]
)

df = spark.createDataFrame(items, schema=schema)
df = df.withColumn("Data", to_date(col("Data"), "yyyy-MM-dd"))

df = df.withColumn("mes", date_format(col("Data"), "yyyy-MM"))

hoje = datetime.today()
ultimos_6_meses = [
    (hoje - relativedelta(months=i)).strftime("%Y-%m") for i in reversed(range(6))
]

df_filtrado = df.filter(col("mes").isin(ultimos_6_meses))

df_agrupado = df_filtrado.groupBy("Tipo", "mes").count()

df_abandonados = df_agrupado.filter(
    (
        (col("Tipo") == "Tarefa a Ser Feita")
        & (datediff(current_date(), col("mes")) > 15)
    )
    | ((col("Tipo") == "Item de Compra") & (datediff(current_date(), col("mes")) > 30))
)

df_pivotado = (
    df_agrupado.groupBy("Tipo")
    .pivot("mes", ultimos_6_meses)
    .agg(first("count"))
    .fillna(0)
    .orderBy("Tipo")
)

df_relatorio = df_pivotado.toPandas()
df_relatorio.to_csv("relatorio.csv", index=False)
