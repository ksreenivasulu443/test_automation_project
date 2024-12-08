import os
from pyspark.sql import SparkSession

# Set the JAVA_HOME environment variable
# os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home"
# os.environ["HADOOP_HOME"] = "/Users/admin/Documents/spark-3.5.3-bin-hadoop3"
# os.environ["SPARK_HOME"] = "/Users/admin/Documents/spark-3.5.3-bin-hadoop3"
# os.environ["PYSPARK_PYTHON"] = "python"



# Create a Spark session
spark = SparkSession.builder \
    .appName("SimpleDataFrameExample") \
    .getOrCreate()

df= spark.read.parquet("/Users/admin/PycharmProjects/test_automation_project/files/userdata1.parquet")

df = df.filter('id<990')

df.write.mode('ignore').parquet("/Users/admin/PycharmProjects/test_automation_project/files/transformed")
