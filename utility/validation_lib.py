from pyspark.sql.functions import col, upper, isnan, when,trim,count
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

def uniqueness_check( target, unique_col):
    print("*"*40)
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
        Null_df = target.select(count(when(col(column).contains('None') |
                                           upper(col(column)).contains('NULL') |
                                           upper(col(column)).contains('NA') |
                                           (trim(col(column)) == '') |
                                           col(column).isNull() |
                                           isnan(column), column
                                           )).alias("Null_value_count"))

        Null_df.show()
        fail_count = Null_df.collect()[0][0]
        if fail_count > 0:
            print("Null present ", column)
        else:
            print("Null not present in ", column)

