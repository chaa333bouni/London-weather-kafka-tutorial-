import json
import random
from kafka import KafkaProducer
import requests

API_KEY = '7330f34bcb109aa73c381d67e9189613'
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather?q=London,uk&APPID=7330f34bcb109aa73c381d67e9189613'

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather_data(city):
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'  # For Celsius
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        return {
            'city': data['name'],
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'weather': data['weather'][0]['description']
        }
    else:
        print(f"Failed to fetch weather data for {city}: {response.status_code}")
        return None

def produce_messages():
    cities = ['New York', 'London', 'Paris', 'Tokyo', 'Delhi']  # Add more cities as needed
    while True:
        city = random.choice(cities)
        weather_data = fetch_weather_data(city)
        if weather_data:
            print(f"Sending: {weather_data}")
            producer.send('weather', value=weather_data)

if __name__ == '__main__':
    produce_messages()
