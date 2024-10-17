import dbutils

from pyspark.sql import SparkSession
import logging
import os
from pyspark.sql.functions import *

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
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

# ADLS account information
adls_account_name = "juneauto"  # Your ADLS account name
adls_container_name = "project1"  # Your container name
key = "CBmVs+pWVKG5ClkNH8I2ooZN5ZTHBECr+Bmf2DQCV+Hj8py1Hfa51CcZ703YkIzKFHsKOsnIkdGZ+AStdaF/kQ=="  # Your Account Key

# Set Spark configuration for ADLS Gen2 using SharedKey authentication
spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "SharedKey")
spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)

# Snowflake JDBC connection string
url = "jdbc:snowflake://atjmorn-ht38363.snowflakecomputing.com/?user=KSREENIVASULU443&password=Dharmavaram1@&warehouse=COMPUTE_WH&db=DEMO_DB&schema=PUBLIC"

# Set the filesystem path
adls_file_system_url = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/"

# Path to ADLS directory where files are added monthly
#adls_qa_backup= f"{adls_file_system_url}master/customer/qa_backup/"
adls_folder_path = f"{adls_file_system_url}master/customer/full_load/"


#existing_df = spark.read.parquet(adls_qa_backup)
df = spark.read.csv(adls_folder_path, header=True,inferSchema=True)



final_df = (df.withColumn("start_date", concat(current_date(), lit("T00:00:00.0000Z")).cast("string")).
                  withColumn("end_date", lit("9999-12-31T23:59:59.999Z").cast("string")).
                  withColumn("history_flag", lit(False)))

final_df.show()




final_df.write.mode("append") \
                    .format("jdbc") \
                    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
                    .option("url", url) \
                    .option("dbtable", "CUSTOMER_HIST_EXPECTED") \
                    .save()
