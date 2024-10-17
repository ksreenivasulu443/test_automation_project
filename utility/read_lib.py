"""This is read library created on 09/04/2024 by Sreeni
 This module file will be used to read data from different files and databases"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json
from utility.genereal_lib import flatten, read_config, fetch_transformation_query_path, read_schema, fetch_file_path, \
    expected_data_setup


def read_file(spark, path, file_type, schema_path, multiline,row):
    # Resolve the file path if not ADLS
    if file_type != 'adls':
        path = fetch_file_path(path)

    # Initialize the dataframe variable
    df = None

    # Handle different file types
    if file_type == 'csv':
        if schema_path != 'NOT APPL':
            schema = read_schema(schema_path)
            df = spark.read.schema(schema).csv(path, header=True)
        else:
            df = spark.read.csv(path, header=True, inferSchema=True)

    elif file_type == 'json':
        df = spark.read.option("multiline", multiline).json(path)
        df = flatten(df)

    elif file_type == 'parquet':
        df = spark.read.parquet(path)

    elif file_type == 'avro':
        df = spark.read.format('avro').load(path)

    elif file_type == 'text':
        df = spark.read.format("text").load(path)

    elif file_type == 'adls':
        config = read_config('adls')
        adls_account_name = config['adls_account_name']
        adls_container_name = config["adls_container_name"]
        key = config['key']

        # Set Spark configuration for ADLS Gen2
        spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "SharedKey")
        spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)

        adls_file_system_url = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/"
        adls_folder_path = f"{adls_file_system_url}{path}"

        #df = spark.read.parquet(adls_folder_path)
        df = spark.read.csv(adls_folder_path, header=True)

    exclude_cols = row['exclude_columns'].split(',')
    return df.drop(*exclude_cols)




def read_snowflake(spark, table, database, query_path,row):
    config = read_config(database)

    if query_path != 'NOT APPL' and query_path.endswith('.sql'):
        sql_query = fetch_transformation_query_path(query_path)
        print(sql_query)
        df = spark.read \
            .format("jdbc") \
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
            .option("url", config['jdbc_url']) \
            .option("query", sql_query) \
            .load()
    else:
        expected_data_setup(query_path)
        df = spark.read \
            .format("jdbc") \
            .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
            .option("url", config['jdbc_url']) \
            .option("dbtable", table) \
            .load()

    exclude_cols = row['exclude_columns'].split(',')
    return df.drop(*exclude_cols)


def read_db(spark, table, database, query_path,row):
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

    exclude_cols = row['exclude_columns'].split(',')
    return df.drop(*exclude_cols)
