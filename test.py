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

# Sample data
data = [
    ("Alice", 30),
    ("Bob", 25),
    ("Cathy", 28),
    ("David", 35)
]

# Define the schema
columns = ["Name", "Age"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()

