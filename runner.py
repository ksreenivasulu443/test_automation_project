import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
import os

from utility.read_lib import read_file, read_snowflake, read_db

# Jars setup
project_path = os.getcwd()
print("project_path", project_path)
snow_jar = project_path+"\jar/snowflake-jdbc-3.14.3.jar"
#snow_jar = r"C:\Users\A4952\PycharmProjects\June_automation_batch1\jars\snowflake-jdbc-3.14.3.jar"
postgres_jar = project_path+"\jar\postgresql-42.2.5.jar"
# postgres_jar = r"C:\Users\A4952\PycharmProjects\test_automation_project\jar\postgresql-42.2.5.jar"

jars = snow_jar + ',' + postgres_jar

# Spark Session
spark = SparkSession.builder.master("local[1]") \
    .appName("test") \
    .config("spark.jars", jars) \
    .config("spark.driver.extraClassPath", jars) \
    .config("spark.executor.extraClassPath", jars) \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.0") \
    .getOrCreate()

# Test cases read
test_cases = pd.read_excel(r"C:\Users\A4952\PycharmProjects\test_automation_project\config\Master_Test_Template.xlsx")

run_test_case = test_cases.loc[(test_cases.execution_ind == 'Y')]
print("Ind==Y records")
print(run_test_case.head(10))

run_test_case = spark.createDataFrame(run_test_case)

validation = (run_test_case.groupBy('source', 'source_type', 'source_json_multiline',
                                    'source_db_name', 'source_schema_path', 'source_transformation_query_path',
                                    'target', 'target_type', 'target_json_multiline', 'target_db_name',
                                    'target_schema_path',
                                    'target_transformation_query_path',
                                    'key_col_list', 'null_col_list', 'exclude_columns',
                                    'unique_col_list', 'dq_column', 'expected_values', 'min_val', 'max_val').
              agg(collect_set('validation_Type').alias('validation_Type')))

validation.show(truncate=False)

testcases = validation.collect()

print("testcases in list form", testcases)

# Test case execution
for row in testcases:
    print("#"*40)
    print("source_file_path/table and type", row['source'], row['source_type'])
    print("target_file_path/table", row['target'], row['target_type'])
    print("source db name", row['source_db_name'])
    print("source trans query path", row['source_transformation_query_path'])

    if row['source_type'] == 'snowflake':
        source = read_snowflake(spark=spark,
                                table=row['source'],
                                database=row['source_db_name'],
                                query_path=row['source_transformation_query_path'])
    elif row['source_type'] == 'table':
        source = read_db(spark=spark,
                                table=row['source'],
                                database=row['source_db_name'],
                                query_path=row['source_transformation_query_path'])
    else:
        source = read_file(spark=spark,
                           path=row['source'],
                           type=row['source_type'],
                           schema_path=row['source_schema_path'],
                           multiline=row['source_json_multiline'])

    if row['target_type'] == 'snowflake':
        target = read_snowflake(spark=spark,
                                table=row['target'],
                                database=row['target_db_name'],
                                query_path=row['target_transformation_query_path'])

    elif row['target_type'] == 'table':
        target = read_db(spark=spark,
                                table=row['target'],
                                database=row['target_db_name'],
                                query_path=row['target_transformation_query_path'])
    else:
        target = read_file(spark=spark,
                           path=row['target'],
                           type=row['target_type'],
                           schema_path=row['target_schema_path'],
                           multiline=row['target_json_multiline'])

    source.show(truncate=False)
    target.show(truncate=False)


