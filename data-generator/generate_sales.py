import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

products = ["Laptop", "Phone", "Tablet", "Monitor"]

while True:

    event = {
        "order_id": random.randint(1000, 9999),
        "product": random.choice(products),
        "price": round(random.uniform(100, 2000), 2),
        "quantity": random.randint(1, 5),
        "timestamp": int(time.time())
    }

    producer.send("sales_events", event)

    print(event)

    time.sleep(1)