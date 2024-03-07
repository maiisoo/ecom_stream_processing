import json

from kafka import KafkaConsumer
from src.conf import configs

consumer = KafkaConsumer(
    "test4",
    bootstrap_servers=configs.kafka_server_ip,
    auto_offset_reset='latest',
    enable_auto_commit=True)

print("starting the consumer")
for msg in consumer:
    # with hdfs.open(hdfs_path, 'at') as f:
    #    print("Click = {}".format(json.loads(msg.value)))
    #    f.write(f"{msg.value.decode('utf-8')}\n")
    print("Click = {}".format(json.loads(msg.value)))
