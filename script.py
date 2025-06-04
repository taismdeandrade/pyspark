from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .master('local')
    .appName('dados')
    .getOrCreate()
)

dados = spark.read.csv('amostragem.csv', header=True, inferSchema=True, sep=';')

#df.show()
#df.printSchema()

#mostra apenas as colunas selecionadas
dados.select('Nome da Tarefa', 'Tipo da Tarefa', 'Status Descrição', 'Usuário').show()

#filtra os dados e seleciona apenas os do usuario jeferson klau
usuario = dados.filter(dados['Usuário'] == 'Jeferson Klau')
usuario.show()

#Filtra por usuario e descrição concluido
concluido = dados.filter((dados['Usuário'] == 'Jeferson Klau') & (dados['Status Descrição'] == 'Concluído'))
concluido.show()

#utiliza o data frame usuario e filtra pela descrição concluido
concluido = usuario.filter(usuario['Status Descrição'] == 'Concluído')
concluido.show()

dados = dados.withColumnRenamed('Nome da Tarefa', 'Nome').withColumnRenamed('Status', 'Status Id').withColumnRenamed('Status descrição', 'Status')

#transforma os valores da coluna Status de concluído e a fazer para done e todo
dados = dados.withColumn('status', regexp_replace('Status', 'Concluído', 'done'))
dados = dados.withColumn('status', regexp_replace('Status', 'A Fazer', 'todo'))
dados.show()