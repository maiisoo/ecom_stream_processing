import json

from kafka import KafkaConsumer

from src.kafka import kafka_conf

# import kafka_conf
#
# topic_name = "test0"
#
# c = KafkaConsumer(
#     topic_name,
#     bootstrap_servers='192.168.100.102:9092',
#     auto_offset_reset='earliest'
#
# )
# for message in c:
#     print("Da nhan", message.value)

consumer = KafkaConsumer(
    "test",
    bootstrap_servers=kafka_conf.kafka_server_ip,
    auto_offset_reset='latest',
    enable_auto_commit=True)
print("starting the consumer")
for msg in consumer:
    print("Click = {}".format(json.loads(msg.value)))
