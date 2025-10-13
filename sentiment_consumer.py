from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import logging
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TrumpNewsSentimentConsumer:
    def __init__(
        self,
        kafka_servers=["localhost:9092"],
        es_hosts=[{"host": "localhost", "port": 9200, "scheme": "http"}],
    ):
        """
        Initialize the Trump News Sentiment Analysis Consumer
        """
        try:
            # Initialize Kafka Consumer
            self.consumer = KafkaConsumer(
                "trump_news",  # Same topic as your existing producer
                bootstrap_servers=kafka_servers,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="trump-sentiment-group",  # Different group ID
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            logger.info("Kafka consumer initialized successfully")

            # Initialize Elasticsearch client
            self.es = Elasticsearch(es_hosts)

            # Test Elasticsearch connection
            if self.es.ping():
                logger.info("Elasticsearch connection established")
                self.setup_elasticsearch_indices()
            else:
                raise Exception("Cannot connect to Elasticsearch")

            # Initialize VADER sentiment analyzer
            self.analyzer = SentimentIntensityAnalyzer()
            logger.info("VADER sentiment analyzer initialized")

        except Exception as e:
            logger.error(f"Error initializing consumer: {e}")
            raise

    def setup_elasticsearch_indices(self):
        """
        Create Elasticsearch indices for sentiment analysis
        """
        # Main sentiment index
        sentiment_index = "trump-news-sentiment"
        sentiment_mapping = {
            "mappings": {
                "properties": {
                    "title": {
                        "type": "text",
                        "analyzer": "english",
                        "fields": {"keyword": {"type": "keyword"}},
                    },
                    "description": {"type": "text", "analyzer": "english"},
                    "source": {"type": "keyword"},
                    "source_category": {"type": "keyword"},
                    "published_at": {"type": "date"},
                    "url": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "processed_at": {"type": "date"},
                    "sentiment": {
                        "properties": {
                            "compound": {"type": "float"},
                            "pos": {"type": "float"},
                            "neu": {"type": "float"},
                            "neg": {"type": "float"},
                            "sentiment_label": {"type": "keyword"},
                            "sentiment_strength": {"type": "keyword"},
                            "confidence": {"type": "float"},
                            "analysis_timestamp": {"type": "date"},
                        }
                    },
                }
            }
        }

        # Sentiment summary index for aggregated stats
        summary_index = "trump-sentiment-summary"
        summary_mapping = {
            "mappings": {
                "properties": {
                    "date": {"type": "date"},
                    "source_category": {"type": "keyword"},
                    "sentiment_label": {"type": "keyword"},
                    "avg_compound": {"type": "float"},
                    "avg_confidence": {"type": "float"},
                    "article_count": {"type": "integer"},
                    "created_at": {"type": "date"},
                }
            }
        }

        # Create indices
        for index_name, mapping in [
            (sentiment_index, sentiment_mapping),
            (summary_index, summary_mapping),
        ]:
            try:
                if not self.es.indices.exists(index=index_name):
                    self.es.indices.create(index=index_name, body=mapping)
                    logger.info(f"Created Elasticsearch index: {index_name}")
                else:
                    logger.info(f"Elasticsearch index {index_name} already exists")
            except Exception as e:
                logger.error(f"Error setting up Elasticsearch index {index_name}: {e}")

    def categorize_source(self, source_name):
        """
        Categorize news sources for analysis
        """
        if not source_name:
            return "unknown"

        source_lower = source_name.lower()

        # News source categories
        categories = {
            "mainstream": [
                "cnn",
                "bbc",
                "reuters",
                "associated press",
                "ap news",
                "npr",
                "pbs",
                "abc news",
                "cbs news",
                "nbc news",
                "usa today",
                "time",
                "newsweek",
                "axios",
                "politico",
                "the hill",
            ],
            "conservative": [
                "fox news",
                "breitbart",
                "daily wire",
                "townhall",
                "newsmax",
                "one america news",
                "gateway pundit",
                "red state",
                "daily caller",
                "washington examiner",
                "new york post",
            ],
            "liberal": [
                "msnbc",
                "huffpost",
                "mother jones",
                "daily kos",
                "salon",
                "slate",
                "vox",
                "the nation",
                "new republic",
                "raw story",
            ],
            "business": [
                "cnbc",
                "marketwatch",
                "fortune",
                "forbes",
                "financial times",
                "business insider",
                "bloomberg",
                "wall street journal",
                "yahoo finance",
            ],
            "international": [
                "bbc news",
                "guardian",
                "independent",
                "telegraph",
                "sky news",
                "al jazeera",
                "rt",
                "dw",
                "france24",
                "euronews",
            ],
        }

        for category, sources in categories.items():
            if any(source in source_lower for source in sources):
                return category

        return "other"

    def clean_text(self, text):
        """
        Light text cleaning for sentiment analysis
        """
        if not text:
            return ""

        # Remove URLs but keep punctuation for VADER
        text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)
        text = " ".join(text.split())
        return text

    def get_sentiment_label(self, compound_score):
        """Convert VADER compound score to sentiment label"""
        if compound_score >= 0.05:
            return "positive"
        elif compound_score <= -0.05:
            return "negative"
        else:
            return "neutral"

    def get_sentiment_strength(self, compound_score):
        """Determine sentiment strength"""
        abs_compound = abs(compound_score)

        if abs_compound >= 0.7:
            return "very_strong"
        elif abs_compound >= 0.5:
            return "strong"
        elif abs_compound >= 0.3:
            return "moderate"
        elif abs_compound >= 0.1:
            return "weak"
        else:
            return "neutral"

    def calculate_confidence(self, scores):
        """Calculate confidence based on score distribution"""
        pos, neu, neg = scores["pos"], scores["neu"], scores["neg"]
        max_score = max(pos, neu, neg)
        compound_confidence = abs(scores["compound"])
        confidence = (max_score * 0.6) + (compound_confidence * 0.4)
        return round(min(confidence, 1.0), 3)

    def analyze_sentiment(self, text):
        """Perform VADER sentiment analysis"""
        try:
            if not text or text.strip() == "":
                return self.get_neutral_sentiment()

            clean_text = self.clean_text(text)
            if not clean_text:
                return self.get_neutral_sentiment()

            # VADER analysis
            scores = self.analyzer.polarity_scores(clean_text)

            # Enhanced metrics
            sentiment_label = self.get_sentiment_label(scores["compound"])
            sentiment_strength = self.get_sentiment_strength(scores["compound"])
            confidence = self.calculate_confidence(scores)

            return {
                "compound": round(scores["compound"], 3),
                "pos": round(scores["pos"], 3),
                "neu": round(scores["neu"], 3),
                "neg": round(scores["neg"], 3),
                "sentiment_label": sentiment_label,
                "sentiment_strength": sentiment_strength,
                "confidence": confidence,
                "analysis_timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"Error in sentiment analysis: {e}")
            return self.get_neutral_sentiment()

    def get_neutral_sentiment(self):
        """Return neutral sentiment for error cases"""
        return {
            "compound": 0.0,
            "pos": 0.0,
            "neu": 1.0,
            "neg": 0.0,
            "sentiment_label": "neutral",
            "sentiment_strength": "neutral",
            "confidence": 0.0,
            "analysis_timestamp": datetime.now().isoformat(),
        }

    def process_article(self, article):
        """Process article with sentiment analysis"""
        try:
            # Analyze sentiment on title and description
            title_sentiment = self.analyze_sentiment(article.get("title", ""))
            desc_sentiment = self.analyze_sentiment(article.get("description", ""))

            # Combine sentiments (weighted: title 70%, description 30%)
            combined_compound = (title_sentiment["compound"] * 0.7) + (
                desc_sentiment["compound"] * 0.3
            )
            combined_sentiment = {
                "compound": round(combined_compound, 3),
                "pos": round(
                    (title_sentiment["pos"] * 0.7) + (desc_sentiment["pos"] * 0.3), 3
                ),
                "neu": round(
                    (title_sentiment["neu"] * 0.7) + (desc_sentiment["neu"] * 0.3), 3
                ),
                "neg": round(
                    (title_sentiment["neg"] * 0.7) + (desc_sentiment["neg"] * 0.3), 3
                ),
                "sentiment_label": self.get_sentiment_label(combined_compound),
                "sentiment_strength": self.get_sentiment_strength(combined_compound),
                "confidence": round(
                    (title_sentiment["confidence"] * 0.7)
                    + (desc_sentiment["confidence"] * 0.3),
                    3,
                ),
                "analysis_timestamp": datetime.now().isoformat(),
            }

            # Categorize source
            source_category = self.categorize_source(article.get("source", ""))

            # Create enriched article
            enriched_article = {
                **article,
                "source_category": source_category,
                "sentiment": combined_sentiment,
                "title_sentiment": title_sentiment,
                "description_sentiment": desc_sentiment,
                "processed_at": datetime.now().isoformat(),
            }

            return enriched_article

        except Exception as e:
            logger.error(f"Error processing article: {e}")
            return None

    def send_to_elasticsearch(self, article):
        """Send processed article to Elasticsearch"""
        try:
            response = self.es.index(index="trump-news-sentiment", body=article)
            return response["result"] == "created"
        except Exception as e:
            logger.error(f"Error sending to Elasticsearch: {e}")
            return False

    def run(self):
        """Main consumer loop"""
        logger.info("Starting Trump News Sentiment Analysis Consumer...")
        logger.info("Consuming from topic: trump_news")
        logger.info("Indexing to: trump-news-sentiment")

        try:
            message_count = 0

            for message in self.consumer:
                try:
                    article = message.value

                    # Process article with sentiment analysis
                    processed_article = self.process_article(article)

                    if processed_article:
                        # Send to Elasticsearch
                        success = self.send_to_elasticsearch(processed_article)

                        if success:
                            message_count += 1
                            sentiment = processed_article["sentiment"]

                            # Log with emoji for better visibility
                            emoji = (
                                "😊"
                                if sentiment["sentiment_label"] == "positive"
                                else "😞"
                                if sentiment["sentiment_label"] == "negative"
                                else "😐"
                            )

                            log_msg = (
                                f"[{message_count}] {emoji} {sentiment['sentiment_label'].upper()}: "
                                f"{article.get('title', 'No title')[:60]}... | "
                                f"Source: {article.get('source', 'Unknown')} ({processed_article.get('source_category', 'unknown')}) | "
                                f"Score: {sentiment['compound']} | Confidence: {sentiment['confidence']}"
                            )

                            logger.info(log_msg)
                        else:
                            logger.warning("Failed to send article to Elasticsearch")
                    else:
                        logger.warning("Failed to process article")

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue

        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        try:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


def main():
    """Main function"""
    try:
        consumer = TrumpNewsSentimentConsumer()
        consumer.run()
    except Exception as e:
        logger.error(f"Failed to start consumer: {e}")


if __name__ == "__main__":
    main()
