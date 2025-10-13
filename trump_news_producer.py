from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime


class TrumpNewsProducer:
    def __init__(self, api_key):
        self.api_key = api_key
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        self.base_url = "https://newsapi.org/v2/everything"

    def fetch_trump_news(self):
        params = {
            "q": "Trump",
            "language": "en",
            "pageSize": 20,
            "sortBy": "publishedAt",
            "apiKey": self.api_key,
        }

        response = requests.get(self.base_url, params=params)
        return response.json()

    def send_to_kafka(self, articles):
        for article in articles:
            enriched = {
                "title": article.get("title", ""),
                "source": article.get("source", {}).get("name", ""),
                "published_at": article.get("publishedAt", ""),
                "url": article.get("url", ""),
                "timestamp": datetime.now().isoformat(),
            }

            self.producer.send("trump_news", enriched)
            print(f"Sent: {enriched['title'][:50]}...")

        self.producer.flush()


if __name__ == "__main__":
    API_KEY = "fd630380bd784983abf9cdc30177475b"

    producer = TrumpNewsProducer(API_KEY)
    news_data = producer.fetch_trump_news()

    if news_data.get("articles"):
        producer.send_to_kafka(news_data["articles"])
        print(f"Sent {len(news_data['articles'])} articles")
