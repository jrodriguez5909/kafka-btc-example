from kafka import KafkaConsumer
import json

# Create a consumer instance
consumer = KafkaConsumer(
    'bitcoin', 'ethereum',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Start consuming
for message in consumer:
    print(message)