print("this is dev code")

import json
import os
import sys
import pandas as pd
from pyspark.sql.functions import explode_outer, concat, col, \
    trim,to_date, lpad, lit, count,max, min, explode, current_timestamp
from pyspark.sql import SparkSession
import getpass
import datetime
from utility.genereal_lib import fetch_file_path, read_config,read_schema

os.environ.setdefault("project_path", os.getcwd())
project_path = os.environ.get("project_path")

cwd = os.getcwd()
# user = os.environ.get('USER')
# print(user)
# result_local_file = cwd+'\Execution_detailed_summary.txt'
# print("result_local_file",result_local_file)
#
# if os.path.exists(result_local_file):
#     os.remove(result_local_file)
#
# file = open(result_local_file, 'a')
# original = sys.stdout
# sys.stdout = file

# jar_path = pkg_resources.resource_filename('jars', 'postgresql-42.2.5.jar')
#postgre_jar = project_path + "/jars/postgresql-42.2.5.jar"
snow_jar = project_path + "/jar/snowflake-jdbc-3.14.3.jar"
#oracle_jar = project_path + "/jars/ojdbc11.jar"

jar_path = snow_jar
spark = SparkSession.builder.master("local[1]") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()


# Fetch system user's login name
system_user = getpass.getuser()

batch_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")



# Load from File to DB raw table

file_path = fetch_file_path('Contact_info_21092024.csv')
schema=read_schema('contact_info_schema.json')
file = spark.read.csv(file_path, header=True, schema=schema)
file = file.filter(file.Identifier.isNotNull())

file.show()



file= file.withColumn('batch_date', lit(batch_id))\
    .withColumn('create_date', current_timestamp())\
    .withColumn('update_date', current_timestamp())\
    .withColumn('create_user',lit(system_user))\
    .withColumn('update_user',lit(system_user))

file.show()

url = "jdbc:snowflake://atjmorn-ht38363.snowflakecomputing.com/?user=KSREENIVASULU443&password=Dharmavaram1@&warehouse=COMPUTE_WH&db=SAMPLEDB&schema=CONTACT_INFO"


file.write.mode("overwrite") \
    .format("jdbc") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .option("url", url) \
    .option("dbtable", "SAMPLEDB.CONTACT_INFO.CONTACT_INFO_RAW") \
    .save()


file.createOrReplaceTempView("file")

contact_info_bronze = spark.sql(
    """ select
    cast(Identifier as decimal(10)) identifier,
    upper(Surname) surname,
    upper(given_name) given_name,
    middle_initial,
    suffix,
    Primary_street_number,
    primary_street_name,
    upper(city) city,
    state,
    zipcode,
    primary_street_number_prev,
    primary_street_name_prev,
    city_prev,
    state_prev,
    zipcode_prev,
    email,
    translate(Phone,'+-','') phone,
    rpad(birthmonth,8,'0') birthmonth
    from file
    """
 )

contact_info_bronze= contact_info_bronze.withColumn('batch_date', lit(batch_id))\
    .withColumn('create_date', current_timestamp())\
    .withColumn('update_date', current_timestamp())\
    .withColumn('create_user',lit(system_user))\
    .withColumn('update_user',lit(system_user))


contact_info_bronze.write.mode("overwrite") \
    .format("jdbc") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .option("url", url) \
    .option("dbtable", "SAMPLEDB.CONTACT_INFO.CONTACT_INFO_BRONZE") \
    .save()


contact_info_bronze.createOrReplaceTempView("contact_info_bronze")

contact_info_silver= spark.sql(
        """
        select
        Identifier,
        Surname,
        given_name,
        middle_initial,
        Primary_street_number,
        primary_street_name,
        city,
        state,
        zipcode,
        Email,
        Phone,
        birthmonth,
        'Y' as Current_ind
        from contact_info_bronze
        union
        select
        Identifier,
        Surname,
        given_name,
        middle_initial,
        Primary_street_number_prev,
        primary_street_name_prev,
        city_prev,
        state_prev,
        zipcode_prev,
        Email,
        Phone,
        birthmonth,
        'N' as Current_ind
        from contact_info_bronze
        """)

contact_info_silver=contact_info_silver.withColumn('batch_date', lit(batch_id))\
    .withColumn('create_date', current_timestamp())\
    .withColumn('update_date', current_timestamp())\
    .withColumn('create_user',lit(system_user))\
    .withColumn('update_user',lit(system_user))




# #load data to snowflake
#





contact_info_silver.write.mode("overwrite") \
    .format("jdbc") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .option("url", url) \
    .option("dbtable", "SAMPLEDB.CONTACT_INFO.CONTACT_INFO_SILVER") \
    .save()

# config_data = read_config('snowflake_db')
# df = spark.read \
#     .format("jdbc") \
#     .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
#     .option("url", config_data['jdbc_url']) \
#     .option("dbtable", 'ETL_AUTO.CONTACT_INFO.CONTACT_INFO_RAW') \
#     .load()
#
# df.show()