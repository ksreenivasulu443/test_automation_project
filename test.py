from pyspark.sql import SparkSession



snow_jar = r"C:\Users\A4952\PycharmProjects\June_automation_batch1\jars\snowflake-jdbc-3.14.3.jar"
spark = SparkSession.builder.master("local[1]") \
                .appName("test") \
                .config("spark.jars", snow_jar) \
                .config("spark.driver.extraClassPath", snow_jar) \
                .config("spark.executor.extraClassPath", snow_jar) \
                .getOrCreate()

url = 'jdbc:snowflake://epizybn-qo01792.snowflakecomputing.com/?user=KSREENIVASULU443&password=Dharmavaram1@&warehouse=COMPUTE_WH&db=SAMPLEDB&schema=CONTACT_INFO'

df = spark.read \
                .format("jdbc") \
                .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
                .option("url", url) \
                .option("query", 'select * from CONTACT_INFO_RAW') \
                .load()

df.show()


