import requests
from kafka import KafkaProducer
from time import sleep
import json

# Create a producer instance
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define function to get BTC price
def get_btc_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    response = requests.get(url)
    data = response.json()
    return data['bitcoin']['usd']

while True:
    # Get Bitcoin data
    price = get_btc_price()

    # Send Bitcoin data to 'bitcoin' Kafka topic
    producer.send('bitcoin', value=price)

    # Wait for 5 seconds
    sleep(5)