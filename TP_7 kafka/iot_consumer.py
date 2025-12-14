from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'iot-sensors',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening for IoT data...")

for msg in consumer:
    data = msg.value
    print("Received:", data)

    if data["temperature"] > 80:
        print("⚠️ ALERT: High temperature!")
