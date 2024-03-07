from Spark_Kafka_Integration import *
from pyspark.sql.types import TimestampType
from src.conf import configs

# Load users, products from PostgresDB
spark = SparkSession.builder \
            .master('localhost') \
            .appName('readPostgres') \
            .getOrCreate()

# Configure PostgreSQL connection properties
postgres_properties = {
    "user": "user",
    "password": "mypassword",
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://localhost:5432/your_database"
}

# Define the table you want to read
table_name = "your_table"

# Read data from PostgreSQL into a PySpark DataFrame
df = spark.read \
    .format("jdbc") \
    .option("url", postgres_properties["url"]) \
    .option("dbtable", f"public.{table_name}") \
    .option("user", postgres_properties["user"]) \
    .option("password", postgres_properties["password"]) \
    .option("driver", postgres_properties["driver"]) \
    .load()

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()


# Schema
purchaseSchema = createSchema(columns=["user_id", "product_id", "datetime", "payment", "status", "error"])
clickSchema = createSchema(columns=["user_id", "product_id", "source", "datetime"])
# Create Spark_Kafka obj
spark_kafka_click = sparkKafkaIntegration(configs.kafka_server_ip)
spark_kafka_purchase = sparkKafkaIntegration(configs.kafka_server_ip)
clickSpark = spark_kafka_click.sparkBuilder(app_name="click")
purchaseSpark = spark_kafka_purchase.sparkBuilder(app_name="purchase")

# Create dataframe for click records
clickDF = spark_kafka_click.readFromKafka(clickSpark, topics="test3")
clickDF = readFromTopic(clickDF, "test3")
clickDF = toDataFrameWithSchema(clickDF, clickSchema)
clickDF = castType(clickDF, ['datetime'], TimestampType())
# writeToConsole(clickDF)

# Create dataframe for purchase records
purchaseDF = spark_kafka_purchase.readFromKafka(purchaseSpark, topics="test4")
purchaseDF = readFromTopic(purchaseDF, "test4")
purchaseDF = toDataFrameWithSchema(purchaseDF, purchaseSchema)
purchaseDF = castType(purchaseDF, ['datetime'], TimestampType())

# clickDF = clickDF.withWatermark("datetime", "10 minutes")
# purchaseDF = purchaseDF.withWatermark("datetime", "10 minutes")
#
# joinPurchaseClickDF = purchaseDF.alias("p").join(clickDF.alias("c"),
#                                                  functions.expr(
#                                                      "p.user_id = c.user_id AND " +
#                                                      "p.product_id = c.product_id AND " +
#                                                      "c.datetime < p.datetime"
#                                                  )
#                                                  ).drop("c.user_id", "c.product_id")
#
# kafka_topic = "joined-purchase"
# kafka_checkpoint_location = "./Checkpoints/joined-purchase"
#
# spark_kafka_purchase.writeToKafka(purchaseDF, kafka_topic, kafka_checkpoint_location)
# writeToConsole(joinPurchaseClickDF)



