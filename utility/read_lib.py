"""This is read library created on 09/04/2024 by Sreeni
 This module file will be used to read data from different files and databases"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json
from utility.genereal_lib import flatten, read_config, fetch_transformation_query_path, read_schema,fetch_file_path


def read_file(spark, path, type, schema_path, multiline):
    if type !='adls':
        path = fetch_file_path(path)
    else:
        path =path
    if type == 'csv':
        if schema_path != 'NOT APPL':
            schema = read_schema(schema_path)
            # with open(schema_path, 'r') as schema_file:
            #     schema = StructType.fromJson(json.load(schema_file))
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

    elif type == 'adls':
        config = read_config('adls')
        adls_account_name = config['adls_account_name'] # Your ADLS account name
        adls_container_name = config["adls_container_name"]  # Your container name
        key = config['key']
        # Set Spark configuration for ADLS Gen2 using SharedKey authentication
        spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "SharedKey")
        spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)

        adls_file_system_url = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/"

        # Path to ADLS directory where files are added monthly
        adls_folder_path = f"{adls_file_system_url}{path}"
        df = spark.read.parquet(adls_folder_path)
        return df

    elif type == 'dat':
        pass


def read_snowflake(spark, table, database, query_path):
    config = read_config(database)

    if query_path != 'NOT APPL':
        sql_query = fetch_transformation_query_path(query_path)
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


def read_db(spark, table, database, query_path):
    # config data( This line is to read data from database_connect.json file for specific db
    # here datavalue value coming form source_db_name / target_db_name
    config = read_config(database)

    if query_path != 'NOT APPL':
        #  fetch_transformation_query_path ( This line is to  read sql query from sql file )
        # here query path value comes from source_transformation_query_path/target_transformation_query_path
        sql_query = fetch_transformation_query_path(query_path)
        print(sql_query)
        df = spark.read.format("jdbc"). \
            option("url", config['url']). \
            option("user", config['user']). \
            option("password", config['password']). \
            option("query", sql_query). \
            option("driver", config['driver']).load()
    else:
        df = spark.read.format("jdbc"). \
            option("url", config['url']). \
            option("user", config['user']). \
            option("password", config['password']). \
            option("dbtable", table). \
            option("driver", config['driver']).load()

    return df
