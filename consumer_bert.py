import json
import os
import time
import numpy as np
import tensorflow as tf
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from transformers import BertTokenizer, TFBertForSequenceClassification
from datetime import datetime

# --- Config ---
BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
ES_HOST = os.getenv('ELASTIC_HOST', 'localhost:9200')
MODEL_PATH = './bert_sentiment_model'

def wait_for_services():
    es = Elasticsearch([ES_HOST])
    print(f"⏳ BERT waiting for Elasticsearch...")
    while True:
        try:
            if es.ping():
                print("✅ BERT connected to Elasticsearch!")
                return es
        except Exception:
            pass
        time.sleep(5)

# --- Load BERT (CPU Mode) ---
print("⏳ Loading BERT...")
try:
    os.environ['CUDA_VISIBLE_DEVICES'] = '-1' 
    tokenizer = BertTokenizer.from_pretrained(MODEL_PATH)
    model = TFBertForSequenceClassification.from_pretrained(MODEL_PATH)
    print("✅ BERT Loaded.")
except Exception as e:
    print(f"❌ Error loading BERT: {e}")
    exit(1)

# --- Wait for DB ---
es = wait_for_services()

# --- Connect to Kafka ---
print(f"⏳ BERT waiting for Kafka...")
while True:
    try:
        consumer = KafkaConsumer(
            'trump_news', 
            bootstrap_servers=[BROKER],
            group_id="bert-group",
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("✅ BERT connected to Kafka!")
        break
    except Exception:
        time.sleep(5)

print("🎧 BERT Consumer Started...")

for msg in consumer:
    article = msg.value
    headline = article.get('title', '')
    
    if headline:
        try:
            # Predict
            inputs = tokenizer([headline], max_length=128, truncation=True, 
                               padding='max_length', return_tensors='tf')
            logits = model(inputs).logits
            probs = tf.nn.softmax(logits, axis=1).numpy()[0]
            
            label_idx = np.argmax(probs)
            labels = {0: "negative", 1: "neutral", 2: "positive"}
            sentiment = labels[label_idx]
            confidence = float(probs[label_idx])
            
            # Dashboard Score
            score = confidence if label_idx == 2 else -confidence if label_idx == 0 else 0.0

            # Enrich
            article['sentiment'] = {
                'sentiment_label': sentiment,
                'compound': score,
                'confidence': confidence
            }
            article['model'] = 'bert'  # Tag for Kibana
            article['processed_at'] = datetime.now().isoformat()

            es.index(index="trump-news-sentiment", body=article)
            print(f"🤖 BERT: {headline[:30]}... -> {sentiment} ({score:.2f})")
        except Exception as e:
            print(f"⚠️ Error: {e}")