import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set

from utility.read_lib import read_file, read_snowflake

snow_jar = r"C:\Users\A4952\PycharmProjects\June_automation_batch1\jars\snowflake-jdbc-3.14.3.jar"


spark = SparkSession.builder.master("local[1]") \
    .appName("test") \
    .config("spark.jars", snow_jar) \
    .config("spark.driver.extraClassPath", snow_jar) \
    .config("spark.executor.extraClassPath", snow_jar) \
    .getOrCreate()

test_cases = pd.read_excel(r"C:\Users\A4952\PycharmProjects\test_automation_project\config\Master_Test_Template.xlsx")
print("all test cases")
print(test_cases.head(10))

run_test_case = test_cases.loc[(test_cases.execution_ind == 'Y')]
print("Ind==Y records")
print(run_test_case.head(10))

run_test_case = spark.createDataFrame(run_test_case)

validation = (run_test_case.groupBy('source', 'source_type','source_json_multiline',
                                    'source_db_name', 'source_schema_path', 'source_transformation_query_path',
                                    'target', 'target_type','target_json_multiline', 'target_db_name', 'target_schema_path',
                                    'target_transformation_query_path',
                                    'key_col_list', 'null_col_list', 'exclude_columns',
                                    'unique_col_list', 'dq_column', 'expected_values', 'min_val', 'max_val').
              agg(collect_set('validation_Type').alias('validation_Type')))

validation.show(truncate=False)

testcases = validation.collect()

print("testcases in list form", testcases)

for row in testcases:
    print("source_file_path/table and type", row['source'], row['source_type'])
    print("target_file_path/table", row['target'], row['target_type'])
    print("source db name", row['source_db_name'])
    print("source trans query path",row['source_transformation_query_path'])

    if row['source_type'] == 'snowflake':
        source = read_snowflake(spark=spark,
                                table = row['source'] ,
                                db_name = row['source_db_name'],
                                query_path=row['source_transformation_query_path'])


    if row['target_type'] == 'snowflake' :
        target = read_snowflake(spark=spark,
                                table=row['target'],
                                db_name=row['target_db_name'],
                                query_path=row['target_transformation_query_path'])


    source.show(truncate=False)
    target.show(truncate=False)
