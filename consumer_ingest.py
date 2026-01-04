import os
import json
import time
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from datetime import datetime

class KafkaElasticsearchConsumer:
    def __init__(self):
        self.broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
        self.es_host = os.getenv('ELASTIC_HOST', 'elasticsearch:9200')
        self.es = Elasticsearch([self.es_host])

    def wait_for_services(self):
        # 1. Wait for Elasticsearch
        print(f"⏳ Connecting to Elasticsearch at {self.es_host}...")
        while True:
            try:
                if self.es.ping():
                    print("✅ Connected to Elasticsearch!")
                    break
            except Exception:
                pass
            print("💤 Waiting for Elasticsearch to wake up...")
            time.sleep(5)

        # 2. Wait for Kafka
        print(f"⏳ Connecting to Kafka at {self.broker}...")
        while True:
            try:
                self.consumer = KafkaConsumer(
                    "trump_news",
                    bootstrap_servers=[self.broker],
                    auto_offset_reset="earliest",
                    group_id="ingest-group",
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                )
                print("✅ Connected to Kafka!")
                break
            except Exception as e:
                print(f"💤 Waiting for Kafka... ({e})")
                time.sleep(5)

    def consume_and_index(self):
        self.wait_for_services()
        print("📥 Ingest Consumer Started...")

        for message in self.consumer:
            try:
                doc = message.value
                doc["kafka_timestamp"] = datetime.now().isoformat()
                
                # Use 'body' for compatibility with older ES versions in your requirements
                self.es.index(index="trump-news-index", body=doc)
                
                print(f"💾 Indexed Raw: {doc.get('title', 'No Title')[:40]}...")

            except Exception as e:
                print(f"⚠️ Error processing message: {e}")

if __name__ == "__main__":
    consumer = KafkaElasticsearchConsumer()
    consumer.consume_and_index()