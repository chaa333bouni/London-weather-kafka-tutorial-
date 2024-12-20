from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'weather',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def process_messages():
    for message in consumer:
        data = message.value
        print(f"Received data: {data}")
        # Add logic to store in a database or perform analytics

if __name__ == '__main__':
    process_messages()
