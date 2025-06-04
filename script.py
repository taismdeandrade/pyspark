from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .master('local')
    .appName('dados')
    .getOrCreate()
)

#Cria um dataframe chamado "dados" a partir do arquivo csv
dados = spark.read.csv('amostragem.csv', header=True, inferSchema=True, sep=';')

#Mostra os tipos de dados das colunas
dados.printSchema()

#mostra apenas as colunas selecionadas
dados.select('Nome da Tarefa', 'Tipo da Tarefa', 'Status Descrição', 'Usuário').show()

#Cria um novo dataframe chamado usuario filtrando do dataframe dados, seleciona apenas os relacionados ao usuario jeferson klau
usuario = dados.filter(dados['Usuário'] == 'Jeferson Klau')
usuario.show()

#Cria um novo dataframe chamado concluido, e filtra por usuario e descrição concluido
concluido = dados.filter((dados['Usuário'] == 'Jeferson Klau') & (dados['Status Descrição'] == 'Concluído'))
concluido.show()

#utiliza o data frame usuario e filtra pela descrição concluido
concluido = usuario.filter(usuario['Status Descrição'] == 'Concluído')
concluido.show()

#Renomeia as colunas "Nome da Tarefa", "Status", e "Status descrição" para "Nome", "Status Id", e "Status" respectivamente
dados = dados.withColumnRenamed('Nome da Tarefa', 'Nome').withColumnRenamed('Status', 'Status Id').withColumnRenamed('Status descrição', 'Status')

#Muda o tipo da coluna "Data de Criação" de timestamp para o tipo date
dados = dados.withColumn('Data de Criação', col('Data de Criação').cast('date'))

#transforma os valores da coluna "Status" de "concluído" e "A fazer" para "done" e "todo"
dados = dados.withColumn('status', regexp_replace('Status', 'Concluído', 'done'))
dados = dados.withColumn('status', regexp_replace('Status', 'A Fazer', 'todo'))
dados.show()