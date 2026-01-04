import json
import os
import pickle
import time
import numpy as np
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing.sequence import pad_sequences
from datetime import datetime

# --- Config ---
BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
ES_HOST = os.getenv('ELASTIC_HOST', 'localhost:9200')
MAX_LEN = 100

def wait_for_services():
    # 1. Connect to Elasticsearch
    es = Elasticsearch([ES_HOST])
    print(f"⏳ LSTM waiting for Elasticsearch at {ES_HOST}...")
    while True:
        try:
            if es.ping():
                print("✅ LSTM connected to Elasticsearch!")
                return es
        except Exception:
            pass
        time.sleep(5)

# --- Load LSTM (CPU Mode) ---
print("⏳ Loading LSTM...")
try:
    os.environ['CUDA_VISIBLE_DEVICES'] = '-1' # Force CPU
    model = load_model('sentiment_lstm_model.h5')
    with open('tokenizer.pickle', 'rb') as handle:
        tokenizer = pickle.load(handle)
    print("✅ LSTM Loaded.")
except Exception as e:
    print(f"❌ Error loading LSTM: {e}")
    exit(1)

# --- Wait for DB ---
es = wait_for_services()

# --- Connect to Kafka ---
print(f"⏳ LSTM waiting for Kafka at {BROKER}...")
while True:
    try:
        consumer = KafkaConsumer(
            'trump_news', 
            bootstrap_servers=[BROKER],
            group_id="lstm-group",
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("✅ LSTM connected to Kafka!")
        break
    except Exception:
        time.sleep(5)

print("🎧 LSTM Consumer Started...")

for msg in consumer:
    article = msg.value
    headline = article.get('title', '')
    
    if headline:
        try:
            # Predict
            seq = tokenizer.texts_to_sequences([headline])
            padded = pad_sequences(seq, maxlen=MAX_LEN)
            pred = model.predict(padded, verbose=0)
            
            label_idx = np.argmax(pred)
            labels = {0: "negative", 1: "neutral", 2: "positive"}
            sentiment = labels[label_idx]
            confidence = float(pred[0][label_idx])
            
            # Dashboard Score (-1 to 1)
            score = confidence if label_idx == 2 else -confidence if label_idx == 0 else 0.0

            # Enrich
            article['sentiment'] = {
                'sentiment_label': sentiment,
                'compound': score,
                'confidence': confidence
            }
            article['model'] = 'lstm'  # Tag for Kibana
            article['processed_at'] = datetime.now().isoformat()

            es.index(index="trump-news-sentiment", body=article)
            print(f"🧠 LSTM: {headline[:30]}... -> {sentiment} ({score:.2f})")
        except Exception as e:
            print(f"⚠️ Error: {e}")