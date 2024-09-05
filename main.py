
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json

spark = SparkSession.builder.getOrCreate()


# Read the JSON configuration file
# with open(r"C:\Users\A4952\PycharmProjects\test_automation_project\schema_files\contact_info_schema.json", 'r') as schema_file:
#         schema = StructType.fromJson(json.load(schema_file))

# df = spark.read.schema(schema).csv(r"C:\Users\A4952\PycharmProjects\test_automation_project\files\Contact_info.csv", header=True, inferSchema=True)


df = spark.read.csv(r"C:\Users\A4952\PycharmProjects\test_automation_project\files\Contact_info.csv", header=True, inferSchema=True)

df.printSchema()

print(df.schema.json())