import os
import time
import re
from elasticsearch import Elasticsearch
from collections import Counter

class WordCloudProcessor:
    def __init__(self):
        es_host = os.getenv('ELASTIC_HOST', 'elasticsearch:9200')
        self.es = Elasticsearch([es_host])

    def wait_for_elastic(self):
        print("⏳ Word Cloud waiting for Elasticsearch...")
        while True:
            try:
                if self.es.ping():
                    print("✅ Word Cloud connected to Elasticsearch!")
                    break
            except Exception:
                pass
            time.sleep(5)

    def extract_words_from_titles(self):
        # Gracefully handle if the index doesn't exist yet (no news yet)
        if not self.es.indices.exists(index="trump-news-index"):
            return {}

        response = self.es.search(
            index="trump-news-index", body={"size": 1000, "_source": ["title"]}
        )

        all_words = []
        stop_words = {
            "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", 
            "by", "a", "an", "is", "after", "trump", "donald", "says", "new", "us", "u.s."
        }

        for hit in response["hits"]["hits"]:
            title = hit["_source"].get("title", "").lower()
            words = re.findall(r"\b[a-zA-Z]{3,}\b", title)
            words = [word for word in words if word not in stop_words]
            all_words.extend(words)

        return Counter(all_words)

    def run(self):
        self.wait_for_elastic()
        print("☁️ Word Cloud Processor Started...")
        
        while True:
            try:
                word_counts = self.extract_words_from_titles()
                if word_counts:
                    for word, count in word_counts.most_common(50):
                        doc = {"word": word, "count": count}
                        self.es.index(index="trump-words-index", body=doc)
                    print(f"✅ Updated Cloud: {len(word_counts)} unique words found.")
                else:
                    print("⏳ Waiting for data to arrive in 'trump-news-index'...")
            except Exception as e:
                print(f"⚠️ Error: {e}")
            
            time.sleep(60) # Update every 1 minute

if __name__ == "__main__":
    processor = WordCloudProcessor()
    processor.run()