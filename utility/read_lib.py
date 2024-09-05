"""This is read library created on 09/04/2024 by Sreeni
 This module file will be used to read data from different files and databases"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json

spark = SparkSession.builder.getOrCreate()
def read_file(path,type,schema_path,spark,multiline=True):
    if type == 'csv':
        if schema_path != 'NOT APPL':
            with open(schema_path,'r') as schema_file:
                schema = StructType.fromJson(json.load(schema_file))
            df = spark.read.schema(schema).csv(path, header=True )
            return df
        else:
            df = spark.read.csv(path, header=True, inferSchema=True)
            return df

    elif type == 'json':
        if multiline:
            df = spark.read.option("multiline", True).json(path)
            return df
        else:
            df = spark.read.option("multiline", True).json(path)
            return df


