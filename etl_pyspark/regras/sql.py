# -*- coding: utf-8 -*-

from pyspark.sql.functions import regexp_extract

'''
    Arquivo para criar funcoes que usam SparkSQL para realizar transformacoes que retornam Dataframes.

'''


# Ingestao NASA Apache logs files
def nasa_ingestao(df):
    return (df.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                      regexp_extract('value',
                                     r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]',
                                     1).alias('timestamp'),
                      regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias(
                          'url'),
                      regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast(
                          'integer').alias(
                          'status'),
                      regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias(
                          'bytes')))
