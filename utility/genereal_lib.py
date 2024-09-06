from pyspark.sql.functions import col, explode_outer

from pyspark.sql.types import ArrayType, StructType
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.master('local[1]').getOrCreate()
# df = spark.read.json(r'C:\Users\A4952\PycharmProjects\test_automation_project\files\Complex2.json', multiLine=True)
# df.show()

# df.printSchema()
def flatten(df):
    # compute Complex Fields (Lists and Structs) in Schema
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    print("complex fields", complex_fields)
    print("length of complex fileds",len(complex_fields))
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if type(complex_fields[col_name]) == StructType:
            expanded = [col(col_name + '.' + k).alias( k) for k in
                        [n.name for n in complex_fields[col_name]]]
            print("expanded columns", expanded)
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))
            print("after explode o/p of df")
            df.show()



        # recompute remaining Complex Fields in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
        print("complex fields after explode outer")
        print("complex fields", complex_fields)
        print("length of complex fileds", len(complex_fields))

    return df

# flatten_output = flatten(df)
#
# flatten_output.show()
