"""This is read library created on 09/04/2024 by Sreeni
 This module file will be used to read data from different files and databases"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json
from utility.genereal_lib import flatten

# snow_jar = r"C:\Users\A4952\PycharmProjects\June_automation_batch1\jars\snowflake-jdbc-3.14.3.jar"
# spark = SparkSession.builder.master("local[1]") \
#     .appName("test") \
#     .config("spark.jars", snow_jar) \
#     .config("spark.driver.extraClassPath", snow_jar) \
#     .config("spark.executor.extraClassPath", snow_jar) \
#     .getOrCreate()


def read_file(spark,path, type, schema_path, multiline):
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
        df = spark.read.format('avro').load(path)
        return df
    elif type == 'text':
        df = spark.read.format("text").load(path)
        return df
    elif type == 'orc':
        pass
    elif type == 'dat':
        pass

def read_snowflake(spark, table, database,query_path):

    with open(r"C:\Users\A4952\PycharmProjects\test_automation_project\config\database_connection.json") as f:
        config = json.load(f)[database]

    if query_path != 'NOT APPL':
        with open(query_path, "r") as file:
            sql_query = file.read()
        print(sql_query)
        df = spark.read \
            .format("jdbc") \
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
            .option("url", config['jdbc_url']) \
            .option("query", sql_query) \
            .load()
    else:
        df = spark.read \
            .format("jdbc") \
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
            .option("url", config['jdbc_url']) \
            .option("dbtable", table) \
            .load()

    return df

# df = read_snowflake(spark)
#
# df.show()

