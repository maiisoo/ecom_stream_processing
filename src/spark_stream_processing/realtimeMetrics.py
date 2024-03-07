from src.spark_stream_processing.Spark_Kafka_Integration import *

spark_kafka = sparkKafkaIntegration(configs.kafka_server_ip)

spark = spark_kafka.sparkBuilder(app_name="metrics")
joinDF = spark_kafka.readFromKafka(spark, topics=["joined-purchase"])
joinDF = readFromTopic(joinDF, topic="joined-purchase")

joinDF.withColumnRename('c.datetime', 'clickTime')
joinDF.withColumnRename('p.datetime', 'purchaseTime')


