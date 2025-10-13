from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
from datetime import datetime


class KafkaElasticsearchConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            "trump_news",
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.es = Elasticsearch(["http://localhost:9200"])

    def consume_and_index(self):
        print("Starting to consume messages...")

        for message in self.consumer:
            try:
                doc = message.value
                doc["kafka_timestamp"] = datetime.now().isoformat()

                self.es.index(index="trump-news-index", body=doc)

                print(f"Indexed: {doc['title'][:50]}...")

            except Exception as e:
                print(f"Error: {e}")


if __name__ == "__main__":
    consumer = KafkaElasticsearchConsumer()
    consumer.consume_and_index()
