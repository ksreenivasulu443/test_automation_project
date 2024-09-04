import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set

from utility.read_lib import read_file

spark = SparkSession.builder.getOrCreate()

test_cases = pd.read_excel(r"C:\Users\A4952\PycharmProjects\test_automation_project\config\Master_Test_Template.xlsx")
print("all test cases")
print(test_cases.head(10))

run_test_case = test_cases.loc[(test_cases.execution_ind == 'Y')]
print("Ind==Y records")
print(run_test_case.head(10))

run_test_case = spark.createDataFrame(run_test_case)

validation = (run_test_case.groupBy('source', 'source_type',
                                    'source_db_name', 'source_schema_path', 'source_transformation_query_path',
                                    'target', 'target_type', 'target_db_name', 'target_schema_path',
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
    if row['source_type'] == 'csv':
        source = read_file(path = row['source'], type=row['source_type'], spark=spark)

    if row['target_type'] == 'csv':
        target = read_file(path = row['target'], type=row['target_type'], spark=spark)

    source.show(truncate=False)
    target.show(truncate=False)
