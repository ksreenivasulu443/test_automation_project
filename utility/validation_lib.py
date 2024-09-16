from pyspark.sql.functions import col, upper, isnan, when, trim, count,lit,sha2


def count_check(source, target):
    print("*" * 40)
    print("count check validation started")
    src_cnt = source.count()
    tgt_cnt = target.count()
    diff = abs(src_cnt - tgt_cnt)

    if diff > 0:
        print(
            f"source count is {src_cnt}, target count is {tgt_cnt}, Count is not matching between source and target, diff is",
            diff)
    else:
        print(f"source count is {src_cnt}, target count is {tgt_cnt} count is matching between source and target")
    print("*" * 40)
    print("count check validation ended")


def duplicate_check(target, key_cols):
    print("*" * 40)
    print("duplicate check validation started")
    key_cols_list = key_cols.split(',')
    duplicate_df = target.groupBy(key_cols_list).count().filter('count>1')
    duplicate_df.show()
    fail_count = duplicate_df.count()
    if fail_count > 0:
        print("Duplicates present")
    else:
        print("Duplicates not present")
    print("duplicate check validation ended")
    print("*" * 40)


def uniqueness_check(target, unique_col):
    print("*" * 40)
    print("Uniqueness check validation started")
    unique_col_list = unique_col.split(',')
    for column in unique_col_list:
        duplicate_df = target.groupBy(column).count().filter('count>1')
        duplicate_df.show()
        fail_count = duplicate_df.count()
        if fail_count > 0:
            print("Duplicates present in ", column)
        else:
            print("Duplicates not present in ", column)

    print("Uniqueness check validation ended")
    print("*" * 40)


def null_value_check(target, null_cols):
    null_columns_list = null_cols.split(",")
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
        else:
            print("Null not present in ", column)

def records_present_only_in_target(source, target, keyList):
    keyList = keyList.split(",")
    tms = target.select(keyList).exceptAll(source.select(keyList))
    failed = tms.count()
    print("records_present_only_in_target and not source :" , failed)
    tms.show()


def records_present_only_in_source(source, target, keyList):
    keyList = keyList.split(",")
    smt = source.select(keyList).exceptAll(target.select(keyList))
    failed = smt.count()
    print("records_present_only_in_source not in target :" ,failed)
    smt.show()

def data_compare(source, target, keycolumn):
    keycolumn = keycolumn.split(",")
    keycolumn = [i.lower() for i in keycolumn]


    columnList = source.columns
    smt = source.exceptAll(target).withColumn("datafrom", lit("source"))
    tms = target.exceptAll(source).withColumn("datafrom", lit("target"))
    failed = smt.union(tms)
    failed.show()


    failed_count = failed.count()
    target_count = target.count()
    source_count = source.count()
    if failed_count > 0:
        pass
    else:
        pass



    if failed.count() > 0:

        failed2 = failed.select(keycolumn).distinct().withColumn("hash_key",sha2(concat(*[col(c) for c in keycolumn]), 256))
        source = source.withColumn("hash_key", sha2(concat(*[col(c) for c in keycolumn]), 256)).\
            join(failed2,["hash_key"],how='left_semi').drop('hash_key')
        target = target.withColumn("hash_key", sha2(concat(*[col(c) for c in keycolumn]), 256)). \
        join(failed2,["hash_key"],how='left_semi').drop('hash_key')


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



