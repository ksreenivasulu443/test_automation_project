import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
import os

from pyspark.sql.types import StructType, StructField, StringType
from utility.read_lib import read_file, read_snowflake, read_db
from utility.validation_lib import count_check, duplicate_check, uniqueness_check, null_value_check, \
    records_present_only_in_target, records_present_only_in_source, data_compare, schema_check, name_check, check_range
from pyspark.sql.functions import udf, col, regexp_extract, upper, isnan, when, trim, count, lit, sha2, concat

# Jars setup
project_path = os.getcwd()
snow_jar = project_path + "\jar\snowflake-jdbc-3.14.3.jar"
postgres_jar = project_path + "\jar\postgresql-42.2.5.jar"

jar_path = snow_jar + ',' + postgres_jar

# Spark Session
spark = SparkSession.builder.master("local[1]") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()
    # .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.0") \


# pd.set_option('display.max_rows', 50)
#
# # Display up to 20 columns
# pd.set_option('display.max_columns', 20)


Out = {
    "validation_Type": [],
    "Source_name": [],
    "target_name": [],
    "Number_of_source_Records": [],
    "Number_of_target_Records": [],
    "Number_of_failed_Records": [],
    "column": [],
    "Status": [],
    "source_type": [],
    "target_type": []
}

test_case_file_path = project_path + '\config\Master_Test_Template.xlsx'
# Test cases read
test_cases = pd.read_excel(test_case_file_path)

run_test_case = test_cases.loc[(test_cases.execution_ind == 'Y')]
print("Ind==Y records")
print(run_test_case.head(10))

run_test_case = spark.createDataFrame(run_test_case)

validation_df = (run_test_case.groupBy('source', 'source_type', 'source_json_multiline',
                                    'source_db_name', 'source_schema_path', 'source_transformation_query_path',
                                    'target', 'target_type', 'target_json_multiline', 'target_db_name',
                                    'target_schema_path',
                                    'target_transformation_query_path',
                                    'key_col_list', 'null_col_list', 'exclude_columns',
                                    'unique_col_list', 'dq_column', 'expected_values', 'min_val', 'max_val').
              agg(collect_set('validation_Type').alias('validation_Type')))

validation_df.show(truncate=False)

testcases = validation_df.collect()

print("testcases in list form", testcases)

# Test case execution
for row in testcases:
    print("#" * 40)
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

    source.show(truncate=False)  # expected(100,18)
    source.printSchema()
    target.show(truncate=False)  # actual(99, 18 )

    print("validations", row['validation_Type'])
    for validation in row['validation_Type']:
        if validation == 'count_check':
            count_check(source=source, target=target,row=row,Out=Out,validation=validation)
        elif validation == 'duplicate':
            duplicate_check(target=target, key_cols=row['key_col_list'],row=row,Out=Out,validation=validation)
        elif validation == 'uniqueness_check':
            uniqueness_check(target=target, unique_col=row['unique_col_list'],row=row,Out=Out,validation=validation)
        elif validation == 'null_value_check':
            null_value_check(target=target, null_cols=row['null_col_list'],row=row,Out=Out,validation=validation)
        elif validation == 'records_present_only_target':
            records_present_only_in_target(source=source, target=target, keyList=row['key_col_list'],row=row,Out=Out,validation=validation)
        elif validation == 'records_present_only_in_source':
            records_present_only_in_source(source=source, target=target, keyList=row['key_col_list'],row=row,Out=Out,validation=validation)
        elif validation == 'data_compare':
            data_compare(source=source, target=target, keycolumn=row['key_col_list'],row=row,Out=Out,validation=validation)
        elif validation == 'schema_check':
            schema_check(source=source, target=target, spark=spark,validation=validation)
        elif validation == 'name_check':
            name_check(target=target, column=row['dq_column'])
        elif validation == 'check_range':
            check_range(target=target,column=row['dq_column'],min_val=row['min_val'], max_val=row['max_value'])

print(Out)

summary = pd.DataFrame(Out)

print(summary.head())

summary.to_csv("summary.csv")

schema = StructType([
    StructField("validation_Type", StringType(), True),
    StructField("Source_name", StringType(), True),
    StructField("target_name", StringType(), True),
    StructField("Number_of_source_Records", StringType(), True),
    StructField("Number_of_target_Records", StringType(), True),
    StructField("Number_of_failed_Records", StringType(), True),
    StructField("column", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("target_type", StringType(), True)
])

# Convert Pandas DataFrame to Spark DataFrame
summary = spark.createDataFrame(summary, schema=schema)

hash_cols = ['source', 'source_type', 'source_json_multiline',
                                    'source_db_name', 'source_schema_path', 'source_transformation_query_path',
                                    'target', 'target_type', 'target_json_multiline', 'target_db_name',
                                    'target_schema_path',
                                    'target_transformation_query_path',
                                    'key_col_list', 'null_col_list', 'exclude_columns',
                                    'unique_col_list']

report_cols = ['source', 'source_type', 'source_json_multiline',
                                    'source_db_name', 'source_schema_path', 'source_transformation_query_path',
                                    'target', 'target_type', 'target_json_multiline', 'target_db_name',
                                    'target_schema_path',
                                    'target_transformation_query_path',
                                    'key_col_list', 'null_col_list', 'exclude_columns',
                                    'unique_col_list']


summary = (summary.withColumn("hash_key", sha2(concat(*[col(c) for c in hash_cols]), 256)))

validation_df= (validation_df.withColumn("hash_key", sha2(concat(*[col(c) for c in hash_cols]), 256)))

summary.show()