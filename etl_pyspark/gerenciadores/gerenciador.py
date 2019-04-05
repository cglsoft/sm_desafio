# -*- coding: utf-8 -*-
import regras.sql as etl_sql
import regras._udf_comum as udf_comum
import pyspark.sql.functions as f
from pyspark.sql.functions import col

"""
    Classe Gerenciadora de regras logicas 
    
    Construtor:
        Argumentos:
            param1 (self): Referencia para o proprio objeto.
            param2 (DadosIO): objeto da classe de dados que possui uma conexao com o Spark.
    
        Logica: recupera os Dataframes originais necessarios (cliente, pedido, etc...)
"""


class Gerenciador:
    # Conexao (session) do Spark e acesso a dados
    _dados_io = None
    _spark_session = None

    # Dataframes originais
    df_nasa_log = None
    df_nasa_view = None

    def __init__(self, _dados_io):
        self._dados_io = _dados_io
        self._spark_session = _dados_io.spark_session()

        # Dataframes
        self.df_nasa_log = _dados_io.nasa_logs2()

    # Processamento RDD
    def do_sm(self):
        # Processo de ingestão de dados
        self.df_nasa_view = etl_sql.nasa_ingestao(self.df_nasa_log).cache()

        self.df_nasa_view.show(n=10, truncate=False)

    # Data VIEW - 1 - Número​ ​ de​ ​ hosts​ ​ únicos.
    def view_d1(self):
        df = self.df_nasa_view.groupBy('host') \
            .agg(f.count('host').alias('total')) \
            .filter(col('total') == 1) \
            .count()

        print('Data VIEW - 1 - Número​ ​ de​ ​ hosts​ ​ únicos = {0}'.format(df))

    # Data VIEW - 2. O​ ​total​ ​de​ erros​ ​404.
    def view_d2(self):
        print('Data VIEW - 2. O​ ​total​ ​de​ erros​ ​404.')
        self.df_nasa_view.groupBy('status') \
            .agg(f.count('status').alias('total')) \
            .filter(col('status') == '404') \
            .show(truncate=False)

    # Data View - 3. Os 5 URLs que mais causaram erro 404.
    def view_d3(self):
        print('Data View - 3. Os 5 URLs que mais causaram erro 404.')
        self.df_nasa_view.groupBy('url', 'status') \
            .agg(f.count('url').alias('total')) \
            .where(col('status') == '404') \
            .sort(col('total').desc()) \
            .show(n=5, truncate=False)

    # Data View - 4. Quantidade​ ​de​ ​erros​ ​404​ ​por​​ dia.
    def view_d4(self):
        print('Data View - 4. Quantidade de erros 404 por dia.')
        self.df_nasa_view.withColumn('dia', f.col('timestamp').substr(1, 2)) \
            .groupBy('status', col('dia')) \
            .agg(f.count('status').alias('total')) \
            .where(col('status') == '404') \
            .distinct() \
            .sort(col('dia').asc(), col('total').desc()) \
            .show(truncate=False)

    # Data View - 5. O​ ​ total​ ​de​ bytes​ ​retornados.
    def view_d5(self):
        print('Data View - 5. O​ ​total de​ ​bytes​ retornados.')
        self.df_nasa_view.agg(f.sum('bytes').alias('total_bytes')) \
            .sort(col('total_bytes').desc())\
            .show(n=5, truncate=False)

    # Chamada padrão do Gerenciador para leitura conforme os parametros de execucao do servico
    def etl_ingestao(self):
        self.do_sm()

        # Data View -- Simulador
        self.view_d1()

        self.view_d2()

        self.view_d3()

        self.view_d4()

        self.view_d5()

        # Gravacao da tabela no Datalake / MOCK
        self._dados_io.gravar(self.df_nasa_view, 'app_gf.nasa_logs')
