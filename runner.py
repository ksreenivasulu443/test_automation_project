import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
import os
import getpass
import datetime

from pyspark.sql.types import StructType, StructField, StringType
from utility.read_lib import read_file, read_snowflake, read_db
from utility.validation_lib import count_check, duplicate_check, uniqueness_check, null_value_check, \
    records_present_only_in_target, records_present_only_in_source, data_compare, schema_check, name_check, check_range
from pyspark.sql.functions import udf, col, regexp_extract, upper, isnan, when, trim, count, lit, sha2, concat
import sys
# Jars setup
project_path = os.getcwd()

cwd = os.getcwd()
batch_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
# result_local_file = cwd+'/Execution_detailed_summary_'+batch_id+'.txt'
# print("result_local_file",result_local_file)
#
# if os.path.exists(result_local_file):
#     os.remove(result_local_file)

# file = open(result_local_file, 'a')
# original = sys.stdout
# sys.stdout = file

snow_jar = '/Users/admin/PycharmProjects/test_automation_project/jar/snowflake-jdbc-3.14.3.jar'
postgres_jar = '/Users/admin/PycharmProjects/test_automation_project/jar/postgresql-42.2.5.jar'
azure_storage = '/Users/admin/PycharmProjects/test_automation_project/jar/azure-storage-8.6.6.jar'
hadoop_azure = '/Users/admin/PycharmProjects/test_automation_project/jar/hadoop-azure-3.3.1.jar'
jar_path = snow_jar + ',' + postgres_jar + ','+azure_storage + ',' + hadoop_azure


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
    "source": [],
    "target": [],
    "number_of_source_Records": [],
    "number_of_target_Records": [],
    "number_of_failed_Records": [],
    "column": [],
    "status": [],
    "source_type": [],
    "target_type": []
}

test_case_file_path = project_path + '/config/Master_Test_Template.xlsx'
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
                                query_path=row['source_transformation_query_path'],row=row)
    elif row['source_type'] == 'table':
        source = read_db(spark=spark,
                         table=row['source'],
                         database=row['source_db_name'],
                         query_path=row['source_transformation_query_path'],row=row)
    else:
        source = read_file(spark=spark,
                           path=row['source'],
                           file_type=row['source_type'],
                           schema_path=row['source_schema_path'],
                           multiline=row['source_json_multiline'],row=row)

    if row['target_type'] == 'snowflake':
        target = read_snowflake(spark=spark,
                                table=row['target'],
                                database=row['target_db_name'],
                                query_path=row['target_transformation_query_path'],row=row)

    elif row['target_type'] == 'table':
        target = read_db(spark=spark,
                         table=row['target'],
                         database=row['target_db_name'],
                         query_path=row['target_transformation_query_path'],row=row)
    else:
        target = read_file(spark=spark,
                           path=row['target'],
                           file_type=row['target_type'],
                           schema_path=row['target_schema_path'],
                           multiline=row['target_json_multiline'],row=row)

    source.show(n=5,truncate=False)  # expected(100,18)
    target.show(n=5,truncate=False)  # actual(99, 18 )

    print("validations", row['validation_Type'])
    for validation in row['validation_Type']:
        if validation == 'count_check':
            count_check(source=source, target=target, row=row, Out=Out, validation=validation)
        elif validation == 'duplicate':
            duplicate_check(target=target, key_cols=row['key_col_list'], row=row, Out=Out, validation=validation)
        elif validation == 'uniqueness_check':
            uniqueness_check(target=target, unique_col=row['unique_col_list'], row=row, Out=Out, validation=validation)
        elif validation == 'null_value_check':
            null_value_check(target=target, null_cols=row['null_col_list'], row=row, Out=Out, validation=validation)
        elif validation == 'records_present_only_target':
            records_present_only_in_target(source=source, target=target, keyList=row['key_col_list'], row=row, Out=Out,
                                           validation=validation)
        elif validation == 'records_present_only_in_source':
            records_present_only_in_source(source=source, target=target, keyList=row['key_col_list'], row=row, Out=Out,
                                           validation=validation)
        elif validation == 'data_compare':
            data_compare(source=source, target=target, keycolumn=row['key_col_list'], row=row, Out=Out,
                         validation=validation)
        elif validation == 'schema_check':
            schema_check(source=source, target=target, spark=spark,row=row, Out=Out, validation=validation)
        elif validation == 'name_check':
            name_check(target=target, column=row['dq_column'])
        elif validation == 'check_range':
            check_range(target=target, column=row['dq_column'], min_val=row['min_val'], max_val=row['max_value'])

#print(Out)

summary = pd.DataFrame(Out)

#print(summary.head())

summary.to_csv("summary.csv")

schema = StructType([
    StructField("validation_Type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("target", StringType(), True),
    StructField("number_of_source_Records", StringType(), True),
    StructField("number_of_target_Records", StringType(), True),
    StructField("number_of_failed_Records", StringType(), True),
    StructField("column", StringType(), True),
    StructField("status", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("target_type", StringType(), True)
])

# Convert Pandas DataFrame to Spark DataFrame
summary = spark.createDataFrame(summary, schema=schema)

#summary.show()

hash_cols = ['source', 'source_type', 'target', 'target_type','validation_Type']

#run_test_case.show()
#summary.show()
summary = (summary.withColumn("hash_key", sha2(concat(*[col(c) for c in hash_cols]), 256))
           .drop('validation_Type', 'source', 'target', 'source_type', 'target_type'))

run_test_case = (run_test_case.withColumn("hash_key", sha2(concat(*[col(c) for c in hash_cols]), 256)))

#summary.show()
final_result = run_test_case.join(summary, 'hash_key', how='left')

system_user = getpass.getuser()


final_result= final_result.withColumn('batch_date', lit(batch_id))\
    .withColumn('create_user',lit(system_user))\
    .withColumn('update_user',lit(system_user))

final_result.show()

url = "jdbc:snowflake://atjmorn-ht38363.snowflakecomputing.com/?user=KSREENIVASULU443&password=Dharmavaram1@&warehouse=COMPUTE_WH&db=SAMPLEDB&schema=CONTACT_INFO"


final_result.write.mode("overwrite") \
    .format("jdbc") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .option("url", url) \
    .option("dbtable", "SAMPLEDB.CONTACT_INFO.AUTOMATION_SUMMARY") \
    .save()
