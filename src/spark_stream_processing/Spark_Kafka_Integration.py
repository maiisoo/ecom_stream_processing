from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField
from src.conf import configs



class sparkKafkaIntegration:
    def __init__(self, bootstrap_server):
        self.bootstrap_server = bootstrap_server

    def sparkBuilder(self, master_url=configs.spark_master_url, app_name="kafka_integration"):
        spark = SparkSession.builder \
            .master(master_url) \
            .appName(app_name) \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def readFromKafka(self, spark, topics):
        return spark \
            .readStream \
            .format("kafka_producer") \
            .option("kafka_producer.bootstrap.servers", self.bootstrap_server) \
            .option("subscribe", topics) \
            .option("includeHeaders", "true") \
            .load()

    def writeToKafka(self, df, topic, checkpoint):
        return df.select(functions.to_json(functions.struct("*")).alias("value")) \
            .writeStream \
            .format("kafka") \
            .outputMode("append") \
            .option("kafka.bootstrap.servers", self.bootstrap_server) \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint) \
            .start()


def readFromTopic(df, topic):
    df = df.filter(col("topic") == topic)
    return df.withColumn("value", df["value"].cast(StringType()))


def createSchema(columns):
    fields = []
    for column in columns:
        field = StructField(column, StringType(), False)
        fields.append(field)
    schema = StructType(fields)
    return schema


def castType(df, columns, datatype):
    for column in columns:
        df = df.withColumn(column, df[column].cast(datatype))
    return df


def toDataFrameWithSchema(df, schema):
    df = df.withColumn("value", from_json(col("value"), schema).alias("value")) \
        .selectExpr("value.*")

    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())
    return df


def writeToConsole(df):
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

# spark_kafka = SparkKafkaIntergation("192.168.100.102:9092")

# df = df.withColumn("value", df["value"].cast(StringType()))

# schema = createSchema(columns=["user_id", "product_id", "source", "datetime"])

# query = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()
#
# query.awaitTermination()
