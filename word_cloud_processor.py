from elasticsearch import Elasticsearch
import json
from collections import Counter
import re


class WordCloudProcessor:
    def __init__(self):
        self.es = Elasticsearch(["http://localhost:9200"])

    def extract_words_from_titles(self):
        # Get all documents
        response = self.es.search(
            index="trump-news-index", body={"size": 1000, "_source": ["title"]}
        )

        all_words = []
        stop_words = {
            "the",
            "and",
            "or",
            "but",
            "in",
            "on",
            "at",
            "to",
            "for",
            "of",
            "with",
            "by",
            "a",
            "an",
            "is",
            "after",
            "are",
            "was",
            "were",
            "be",
            "been",
            "have",
            "has",
            "had",
            "do",
            "does",
            "did",
            "will",
            "would",
            "could",
            "should",
            "may",
            "might",
            "must",
            "can",
            "cant",
            "wont",
            "dont",
            "doesnt",
            "didnt",
            "isnt",
            "arent",
            "wasnt",
            "werent",
            "hasnt",
            "havent",
            "hadnt",
            "this",
            "that",
            "these",
            "those",
            "i",
            "you",
            "he",
            "she",
            "it",
            "we",
            "they",
            "me",
            "him",
            "her",
            "us",
            "them",
            "my",
            "your",
            "his",
            "its",
            "our",
            "their",
        }

        for hit in response["hits"]["hits"]:
            title = hit["_source"]["title"].lower()
            # Remove punctuation and split into words
            words = re.findall(r"\b[a-zA-Z]{3,}\b", title)
            # Filter out stop words
            words = [word for word in words if word not in stop_words]
            all_words.extend(words)

        # Count word frequencies
        word_counts = Counter(all_words)

        print("Top 20 words in headlines:")
        for word, count in word_counts.most_common(20):
            print(f"{word}: {count}")

        return word_counts

    def create_word_documents(self):
        word_counts = self.extract_words_from_titles()

        # Create new documents for each word
        for word, count in word_counts.items():
            doc = {"word": word, "count": count, "word_length": len(word)}

            self.es.index(index="trump-words-index", body=doc)

        print(f"Created {len(word_counts)} word documents in trump-words-index")


if __name__ == "__main__":
    processor = WordCloudProcessor()
    processor.create_word_documents()
