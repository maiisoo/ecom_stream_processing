import json
import random
import time
from kafka import KafkaProducer
from src.conf import configs
from src.kafka_producer.Click import Click
from src.kafka_producer.Purchase import Purchase

def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=[configs.kafka_server_ip],
                         value_serializer=json_serializer)

click_info = []

while 1 == 1:
    click = Click()
    click_mess = click.toMessage()
    producer.send("click", click_mess)
    click_info.append([click.user_id, click.product_id])
    print("Click: ", click_mess, "sent.")
    time.sleep(1)
    if random.randint(0, 100) % 5 == 0 and len(click_info) > 0:
        user_product_clicked = random.choice(click_info)
        purchase = Purchase(user_product_clicked[0], user_product_clicked[1])
        purchase_mess = purchase.toMessage()
        producer.send("purchase", purchase_mess)
        print("Purchase: ", purchase_mess, "sent.")

