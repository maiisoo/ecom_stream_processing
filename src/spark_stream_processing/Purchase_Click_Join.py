from pyspark import SparkConf
from Spark_Kafka_Integration import *
from pyspark.sql.types import TimestampType


# Schema
purchaseSchema = createSchema(columns=["user_id", "product_id", "datetime", "payment", "status", "error"])
clickSchema = createSchema(columns=["user_id", "product_id", "source", "datetime"])
# Create Spark_Kafka obj
spark_kafka = sparkKafkaIntegration(configs.kafka_server_ip)
spark = spark_kafka.sparkBuilder(app_name="streaming")

DF = spark_kafka.readFromKafka(spark, topics="click,purchase")
# Create dataframe for click records
clickDF = readFromTopic(DF, "click")
clickDF = toDataFrameWithSchema(clickDF, clickSchema)
clickDF = castType(clickDF, ['datetime'], TimestampType())

# Create dataframe for purchase records
purchaseDF = readFromTopic(DF, "purchase")
purchaseDF = toDataFrameWithSchema(purchaseDF, purchaseSchema)
purchaseDF = castType(purchaseDF, ['datetime'], TimestampType())

clickDF = clickDF.withWatermark("datetime", "10 minutes")
purchaseDF = purchaseDF.withWatermark("datetime", "10 minutes")

joinPurchaseClickDF = purchaseDF.alias("p").join(clickDF.alias("c"),
                                                 functions.expr(
                                                     "p.user_id = c.user_id AND " +
                                                     "p.product_id = c.product_id AND " +
                                                     "c.datetime < p.datetime AND " +
                                                     "p.datetime <= c.datetime + INTERVAL 10 minutes"
                                                 )
                                                 ).drop("c.user_id", "c.product_id")

writeToConsole(joinPurchaseClickDF)


spark_kafka.writeToKafka(joinPurchaseClickDF, "join", "./Checkpoint/Joins")

spark_kafka.writeToPostgres(joinPurchaseClickDF, epoch_id=)


