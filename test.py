from pyspark.sql import SparkSession

# Initialize Spark session
# spark = SparkSession.builder \
#     .appName("Read Avro Example") \
#     .config("spark.jars.packages", r"C:\Users\A4952\PycharmProjects\test_automation_project\jar\spark-avro_2.13-3.4.0.jar") \
#     .getOrCreate()
snow_jar= r"C:\Users\A4952\PycharmProjects\test_automation_project\jar\spark-avro_2.13-3.4.0.jar"
spark = SparkSession.builder.master("local[1]") \
    .appName("test") \
    .config("spark.jars", snow_jar) \
    .config("spark.driver.extraClassPath", snow_jar) \
    .config("spark.executor.extraClassPath", snow_jar) \
    .getOrCreate()

# Specify the path to your Avro file
avro_file_path = r"C:\Users\A4952\PycharmProjects\test_automation_project\files\userdata1.avro"

# Read the Avro file
df = spark.read.format("avro").load(avro_file_path)

# Show the contents of the DataFrame
df.show()

# Stop the Spark session
spark.stop()
