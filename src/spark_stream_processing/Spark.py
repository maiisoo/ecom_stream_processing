import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
import findspark

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.jars", os.getcwd() + "/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar" + "," + os.getcwd() + "/jars/kafka-clients-3.5.0.jar") \
    .appName("kafka_test") \
    .getOrCreate()

print(spark.version)

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .option("includeHeaders", "true") \
    .load()
df_to_string = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
print("done")


