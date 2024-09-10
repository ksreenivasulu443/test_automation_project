"""This is read library created on 09/04/2024 by Sreeni
 This module file will be used to read data from different files and databases"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json
from utility.genereal_lib import flatten

spark = SparkSession.builder.getOrCreate()


def read_file(path, type, schema_path, spark, multiline):
    if type == 'csv':
        if schema_path != 'NOT APPL':
            with open(schema_path, 'r') as schema_file:
                schema = StructType.fromJson(json.load(schema_file))
            df = spark.read.schema(schema).csv(path, header=True)
            return df
        else:
            df = spark.read.csv(path, header=True, inferSchema=True)
            return df

    elif type == 'json':
        df = spark.read.option("multiline", multiline).json(path)
        df = flatten(df)
        return df

    elif type == 'parquet':
        df = spark.read.parquet(path)
        return df
    elif type == 'avro':
        df = spark.read.format('avro').load("path")
        return df
    elif type == 'text':
        df = spark.read.format("text").load(path)
        return df
    elif type == 'orc':
        pass
    elif type == 'dat':
        pass
