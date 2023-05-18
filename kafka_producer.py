import requests
from kafka import KafkaProducer
from time import sleep
import json

# Create a producer instance
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_crypto_price(crypto):
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={crypto}&vs_currencies=usd"
    response = requests.get(url)
    data = response.json()
    if crypto in data and 'usd' in data[crypto]:
        return data[crypto]['usd']
    else:
        print(f"Error retrieving price for {crypto}")
        return None

while True:
    # Get Bitcoin and Ethereum data
    bitcoin_price = get_crypto_price('bitcoin')
    ethereum_price = get_crypto_price('ethereum')

    # Send Bitcoin and Ethereum data to their respective Kafka topics
    producer.send('bitcoin', value=bitcoin_price)
    producer.send('ethereum', value=ethereum_price)

    # Wait for 10 seconds
    sleep(10)