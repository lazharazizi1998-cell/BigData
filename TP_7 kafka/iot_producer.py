from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Sending IoT sensor data...")

while True:
    data = {
        "machine_id": 1,
        "temperature": round(random.uniform(20, 95), 2),
        "humidity": round(random.uniform(30, 80), 2),
        "vibration": round(random.uniform(0.1, 2.0), 2),
    }

    producer.send("iot-sensors", data)
    print("Sent:", data)

    time.sleep(1)
