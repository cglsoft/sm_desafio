# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, SQLContext


"""
  Estrutura de Dados para o Gerenciador 
  
"""

class MockDadosIO:
    _spark = None
    _sqlContext = None

    # Linux localization
    _diretorio_linux = '/home/lisboa/semantix/etl_pyspark/dados/mock/nasa'

    _diretorio_windows = 'C:\\dump\\sm\\'
    _diretorio = ''

    def __init__(self, job, so):
        if MockDadosIO._spark is None:
            MockDadosIO._spark = SparkSession.builder.appName(job).getOrCreate()
            MockDadosIO._sqlContext = SQLContext(self._spark)

        # Setup diretorio de leitura
        if so == 0:
            MockDadosIO._diretorio = MockDadosIO._diretorio_linux
        else:
            MockDadosIO._diretorio = MockDadosIO._diretorio_windows

    def spark_session(self):
        return MockDadosIO._spark

    # Nasa LOGS
    def nasa_logs(self):
        return MockDadosIO._spark.read.option("encoding", "utf-8").csv(
            MockDadosIO._diretorio,  header=True, sep=',')

    def nasa_logs2(self):
        return MockDadosIO._sqlContext.read.text(self._diretorio_linux)


    # Gravacao dos mocs
    def gravar(self, df, nome):
        df.repartition(1).write.csv(MockDadosIO._diretorio + nome, header=True, mode='overwrite', sep='|')


class ProdDadosIO:
    _spark = None

    def __init__(self, job):
        if ProdDadosIO._spark is None:
            ProdDadosIO._spark = SparkSession.builder.appName(job) \
                .config("hive.exec.dynamic.partition", "true") \
                .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                .enableHiveSupport() \
                .getOrCreate()

    def spark_session(self):
        return ProdDadosIO._spark

    # Nasa logs
    def nasa_logs(self):
        return ProdDadosIO._spark.read.table('raw_sm.nasa_logs')

    # Gravacao HIVE
    def gravar(self, df, nome):
        df.write.option("compression", "zlib").option("encoding", "UTF-8").mode(
            "overwrite").format("orc").option(
            "header", "false").insertInto(nome, overwrite=True)