from kafka import KafkaProducer
import requests
import json
import time
import os
from datetime import datetime

class TrumpNewsProducer:
    def __init__(self):
        # Read the API key and Broker address from the environment variables (set in docker-compose)
        self.api_key = os.getenv('NEWS_API_KEY')
        broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
        
        print(f"🔄 Connecting to Kafka at: {broker}")
        
        # Retry logic: Wait for Kafka to start before crashing
        while True:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[broker],
                    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                )
                print("✅ Connected to Kafka!")
                break
            except Exception as e:
                print(f"⏳ Waiting for Kafka... ({e})")
                time.sleep(5)

        self.base_url = "https://newsapi.org/v2/everything"

    def fetch_trump_news(self):
        if not self.api_key:
            print("❌ Error: NEWS_API_KEY not found in environment.")
            return {}
            
        params = {
            "q": "Trump",
            "language": "en",
            "pageSize": 50,
            "sortBy": "publishedAt",
            "apiKey": self.api_key,
        }
        try:
            response = requests.get(self.base_url, params=params)
            return response.json()
        except Exception as e:
            print(f"Error fetching news: {e}")
            return {}

    def send_to_kafka(self, articles):
        for article in articles:
            enriched = {
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "source": article.get("source", {}).get("name", ""),
                "published_at": article.get("publishedAt", ""),
                "url": article.get("url", ""),
                "timestamp": datetime.now().isoformat(),
            }
            self.producer.send("trump_news", enriched)
            print(f"Sent: {enriched['title'][:50]}...")
        self.producer.flush()

    def run(self):
        while True:
            print("🔄 Fetching latest Trump news...")
            data = self.fetch_trump_news()
            if data.get("articles"):
                self.send_to_kafka(data["articles"])
                print(f"✅ Sent {len(data['articles'])} articles.")
            else:
                print("⚠️ No articles found or error occurred.")
            
            # Wait 15 minutes before next fetch
            print("💤 Sleeping for 15 minutes...")
            time.sleep(900)

if __name__ == "__main__":
    producer = TrumpNewsProducer()
    producer.run()