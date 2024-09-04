"""This is read library created on 09/04/2024 by Sreeni
 This module file will be used to read data from different files and databases"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
def read_file(path,type,spark):
    if type == 'csv':
        df = spark.read.csv(path, header=True,inferSchema=True)
        return df

