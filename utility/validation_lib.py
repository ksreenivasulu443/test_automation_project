from pyspark.sql.functions import udf, col, regexp_extract, upper, isnan, when, trim, count, lit, sha2, concat
from pyspark.sql.types import BooleanType
from datetime import datetime
from utility.report_lib import write_output


def count_check(source, target, row, Out):
    print("*" * 40)
    print("count check validation started")
    src_cnt = source.count()
    tgt_cnt = target.count()
    diff = abs(src_cnt - tgt_cnt)

    if diff > 0:
        print(
            f"source count is {src_cnt}, target count is {tgt_cnt}, Count is not matching between source and target, diff is",
            diff)
        write_output(validation_Type=row['validation_Type'],
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records=src_cnt,
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=diff,
                     column=row['key_col_list'],
                     Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)
    else:
        print(f"source count is {src_cnt}, target count is {tgt_cnt} count is matching between source and target")
        write_output(validation_Type=row['validation_Type'],
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records=src_cnt,
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=diff,
                     column=row['key_col_list'],
                     Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)

    print("*" * 40)
    print("count check validation ended")


def duplicate_check(target, key_cols, row, Out):
    print("*" * 40)
    print("duplicate check validation started")
    key_cols_list = key_cols.split(',')
    tgt_cnt = target.count()
    duplicate_df = target.groupBy(key_cols_list).count().filter('count>1')
    failed_ccount = duplicate_df.count()
    duplicate_df.show()
    fail_count = duplicate_df.count()
    if fail_count > 0:
        print("Duplicates present")
        write_output(validation_Type=row['validation_Type'],
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records='NOT APPL',
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=failed_ccount,
                     column=row['key_col_list'],
                     Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)
    else:
        print("Duplicates not present")
        write_output(validation_Type=row['validation_Type'],
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records="NOT APPL",
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=fail_count,
                     column=row['key_col_list'],
                     Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)
    print("duplicate check validation ended")
    print("*" * 40)


def uniqueness_check(target, unique_col, row, Out):
    print("*" * 40)
    print("Uniqueness check validation started")
    unique_col_list = unique_col.split(',')
    tgt_cnt = target.count()
    for column in unique_col_list:
        duplicate_df = target.groupBy(column).count().filter('count>1')
        duplicate_df.show()
        fail_count = duplicate_df.count()
        if fail_count > 0:
            print("Duplicates present in ", column)
            write_output(validation_Type=row['validation_Type'],
                         source=row['source'],
                         target=row['target'],
                         Number_of_source_Records='NOT APPL',
                         Number_of_target_Records=tgt_cnt,
                         Number_of_failed_Records=fail_count,
                         column=column,
                         Status='FAIL',
                         source_type=row['source_type'],
                         target_type=row['target_type'],
                         Out=Out)
        else:
            print("Duplicates not present in ", column)
            write_output(validation_Type=row['validation_Type'],
                         source=row['source'],
                         target=row['target'],
                         Number_of_source_Records="NOT APPL",
                         Number_of_target_Records=tgt_cnt,
                         Number_of_failed_Records=fail_count,
                         column=column,
                         Status='PASS',
                         source_type=row['source_type'],
                         target_type=row['target_type'],
                         Out=Out)

    print("Uniqueness check validation ended")
    print("*" * 40)


def null_value_check(target, null_cols, row, Out):
    null_columns_list = null_cols.split(",")
    tgt_cnt = target.count()
    for column in null_columns_list:
        Null_df = target.select(
            count(
                when(
                    (upper(col(column)) == 'NONE') |
                    (upper(col(column)) == 'NULL') |
                    (upper(col(column)) == 'NA') |
                    (trim(col(column)) == '') |
                    col(column).isNull() |
                    isnan(col(column)),
                    column
                )
            ).alias("Null_value_count")
        )

        Null_df.show()
        fail_count = Null_df.collect()[0][0]
        if fail_count > 0:
            print("Null present ", column)
            write_output(validation_Type=row['validation_Type'],
                         source=row['source'],
                         target=row['target'],
                         Number_of_source_Records='NOT APPL',
                         Number_of_target_Records=tgt_cnt,
                         Number_of_failed_Records=fail_count,
                         column=column,
                         Status='FAIL',
                         source_type=row['source_type'],
                         target_type=row['target_type'],
                         Out=Out)
        else:
            print("Null not present in ", column)
            write_output(validation_Type=row['validation_Type'],
                         source=row['source'],
                         target=row['target'],
                         Number_of_source_Records="NOT APPL",
                         Number_of_target_Records=tgt_cnt,
                         Number_of_failed_Records=fail_count,
                         column=column,
                         Status='PASS',
                         source_type=row['source_type'],
                         target_type=row['target_type'],
                         Out=Out)


def records_present_only_in_target(source, target, keyList, row, Out):
    keyList = keyList.split(",")
    src_cnt = source.count()
    tgt_cnt = target.count()
    tms = target.select(keyList).exceptAll(source.select(keyList))
    fail_count = tms.count()
    print("records_present_only_in_target and not source :", fail_count)
    tms.show()
    if fail_count > 0:
        write_output(validation_Type=row['validation_Type'],
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records=src_cnt,
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=fail_count,
                     column=row['key_col_list'],
                     Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)

    else:
        write_output(validation_Type=row['validation_Type'],
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records=src_cnt,
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=fail_count,
                     column=row['key_col_list'],
                     Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)


def records_present_only_in_source(source, target, keyList, row, Out):
    keyList = keyList.split(",")
    smt = source.select(keyList).exceptAll(target.select(keyList))
    failed = smt.count()
    print("records_present_only_in_source not in target :", failed)
    smt.show()


def data_compare(source, target, keycolumn, row, Out):
    keycolumn = keycolumn.split(",")
    keycolumn = [i.lower() for i in keycolumn]
    src_cnt = source.count()
    tgt_cnt = target.count()
    columnList = source.columns
    smt = source.exceptAll(target).withColumn("datafrom", lit("source"))
    tms = target.exceptAll(source).withColumn("datafrom", lit("target"))
    failed = smt.union(tms)
    failed.show()

    failed_count = failed.count()
    if failed_count > 0:
        write_output(validation_Type=row['validation_Type'],
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records=src_cnt,
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=failed_count,
                     column=row['key_col_list'],
                     Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)
    else:
        write_output(validation_Type=row['validation_Type'],
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records=src_cnt,
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=failed_count,
                     column=row['key_col_list'],
                     Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)

    if failed.count() > 0:

        failed2 = failed.select(keycolumn).distinct().withColumn("hash_key",
                                                                 sha2(concat(*[col(c) for c in keycolumn]), 256))
        source = source.withColumn("hash_key", sha2(concat(*[col(c) for c in keycolumn]), 256)). \
            join(failed2, ["hash_key"], how='left_semi').drop('hash_key')
        target = target.withColumn("hash_key", sha2(concat(*[col(c) for c in keycolumn]), 256)). \
            join(failed2, ["hash_key"], how='left_semi').drop('hash_key')

        print("columnList", columnList)
        print("keycolumns", keycolumn)
        for column in columnList:
            print(column.lower())
            if column.lower() not in keycolumn:
                keycolumn.append(column)
                temp_source = source.select(keycolumn).withColumnRenamed(column, "source_" + column)

                temp_target = target.select(keycolumn).withColumnRenamed(column, "target_" + column)
                keycolumn.remove(column)
                temp_join = temp_source.join(temp_target, keycolumn, how='full_outer')
                temp_join.withColumn("comparison", when(col('source_' + column) == col("target_" + column),
                                                        "True").otherwise("False")).filter(
                    f"comparison == False and source_{column} is not null and target_{column} is not null").show()


def schema_check(source, target, spark):
    source.createOrReplaceTempView("source")
    target.createOrReplaceTempView("target")
    source_schema = spark.sql("describe source")
    source_schema.show()
    source_schema.createOrReplaceTempView("source_schema")
    target_schema = spark.sql("describe target")
    target_schema.createOrReplaceTempView("target_schema")

    failed = spark.sql('''select * from (select lower(a.col_name) source_col_name,lower(b.col_name) target_col_name, a.data_type as source_data_type, b.data_type as target_data_type, 
    case when a.data_type=b.data_type then "pass" else "fail" end status
    from source_schema a full join target_schema b on lower(a.col_name)=lower(b.col_name)) where status='fail' ''')
    source_count = source_schema.count()
    target_count = target_schema.count()
    failed_count = failed.count()
    failed.show()
    if failed_count > 0:
        pass
    else:
        pass


def name_check(target, column):
    pattern = "^[a-zA-Z]"

    # Add a new column 'is_valid' indicating if the name contains only alphabetic characters
    df = target.withColumn("is_valid", regexp_extract(col(column), pattern, 0) != "")
    df.show()
    target_count = target.count()
    failed = df.filter('is_valid = False ')
    failed.show()
    failed_count = failed.count()
    if failed_count > 0:
        pass
    else:
        pass


def check_range(target, column, min_val, max_val):
    invalid_count = target.filter((col(column) < min_val) | (col(column) > max_val)).count()
    return invalid_count == 0


def date_check(target, dq_col):
    def is_valid_date_format(date_str: str) -> bool:
        try:
            # Try to parse the string in the format 'dd-mm-yyyy'
            datetime.strptime(date_str, "%d-%m-%Y")
            return True
        except ValueError:
            return False

    date_format_udf = udf(is_valid_date_format, BooleanType())

    df_with_validation = target.withColumn("is_valid_format", date_format_udf(col("date"))).filter(
        'is_valid_format = False')

    failed = df_with_validation.count()
    if failed > 0:
        print("fail")
    else:
        print('pass')
