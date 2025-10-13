# 📰 Real-Time Trump News Sentiment Analysis  
**A Comprehensive End-to-End Data Pipeline for Political Media Insights**

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)
![Kafka](https://img.shields.io/badge/Streaming-Apache%20Kafka-black?logo=apache-kafka)
![Elasticsearch](https://img.shields.io/badge/Search-Elasticsearch-orange?logo=elasticsearch)
![Kibana](https://img.shields.io/badge/Dashboard-Kibana-yellow?logo=kibana)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED?logo=docker)
![License](https://img.shields.io/badge/License-MIT-green)

> Real-time sentiment analysis pipeline that ingests live political news data, processes it through distributed streaming, performs sentiment classification, and visualizes results in real-time dashboards.

---

## 🧭 Overview

This project implements a **real-time data engineering and NLP pipeline** that continuously analyzes **Trump-related news sentiment** using modern technologies.  
It demonstrates a full end-to-end system — from **data ingestion** to **visualization** — fully containerized with **Docker** for scalability and portability.

---

## ⚙️ System Architecture

```mermaid
graph TD
A[News API] --> B[Kafka + Zookeeper]
B --> C[Sentiment Consumers (Python + VADER)]
C --> D[Elasticsearch]
D --> E[Kibana Dashboard]
subgraph Dockerized Environment
A
B
C
D
E
end
```

**Pipeline Stages**
1. **Data Ingestion:** Fetch Trump-related articles from the [News API](https://newsapi.org).  
2. **Streaming:** Push data into Apache Kafka for real-time processing.  
3. **Processing:**  
   - *Consumer 1:* Indexes news articles into Elasticsearch.  
   - *Consumer 2:* Performs sentiment analysis using **VADER NLP**.  
   - *Consumer 3:* Generates word frequency indices for word clouds.  
4. **Storage:** Store enriched data (sentiment, metadata) in **Elasticsearch**.  
5. **Visualization:** Interactive dashboards in **Kibana**.  
6. **Deployment:** Fully containerized using **Docker** for easy setup and scalability.

---

## 🧠 Core Features

| Feature | Description |
|:---------|:-------------|
| 📰 **Real-Time News Ingestion** | Fetches breaking Trump-related articles via News API. |
| ⚡ **Kafka Streaming** | Streams data in real-time for scalable processing. |
| 💬 **VADER Sentiment Analysis** | Analyzes article tone as positive, negative, or neutral. |
| 🔍 **Elasticsearch Indexing** | Stores enriched data for fast search and analytics. |
| 📊 **Kibana Dashboards** | Visualizes trends, sentiment distribution, and word clouds. |
| 🐳 **Dockerized Deployment** | Ensures reproducibility and easy multi-container setup. |

---

## 🧰 Tech Stack

| Layer | Technology |
|:------|:------------|
| **Data Source** | [News API](https://newsapi.org) |
| **Data Streaming** | Apache Kafka + Zookeeper |
| **Data Processing** | Python (Producers & Consumers) |
| **Sentiment Analysis** | VADER (NLTK) |
| **Storage & Search** | Elasticsearch |
| **Visualization** | Kibana |
| **Containerization** | Docker & Docker Compose |

---

## 🧩 Pipeline Components

### **Producer.py**
- Fetches latest Trump-related articles via News API.
- Publishes structured JSON to Kafka topic `trump_news`.

### **Consumer 1 – News Indexer**
- Reads Kafka messages.
- Indexes raw news articles into Elasticsearch.

### **Consumer 2 – Sentiment Processor**
- Applies VADER sentiment analysis.
- Enriches each article with:
  - Polarity score (positive/neutral/negative)
  - Confidence level
  - Source metadata

### **Consumer 3 – Word Indexer**
- Extracts all words from headlines.
- Removes stopwords.
- Builds a frequency index for **word cloud generation** in Kibana.

---

## 🖥️ Visualization & Demo

You can include a **demo image or GIF** of your dashboard here 👇  
(just replace the link with your uploaded image on GitHub)

### 🖼️ Dashboard Preview
![Kibana Dashboard Demo](assets/demo-dashboard.png)
*Example: Real-time sentiment and keyword analysis in Kibana.*

> 💡 Tip: Place your image inside an `assets/` or `images/` folder in your repo, then use relative path like `![Demo](assets/demo.png)`.

If you have a **screen recording**, you can embed a link here:  
🎥 [Watch Demo Video](#)

---

## 💻 Installation & Setup

### 1️⃣ Clone Repository
```bash
git clone https://github.com/yourusername/trump-news-sentiment-pipeline.git
cd trump-news-sentiment-pipeline
```

### 2️⃣ Configure Environment
Create a `.env` file:
```
NEWS_API_KEY=your_newsapi_key
ELASTIC_HOST=http://localhost:9200
KAFKA_BROKER=localhost:9092
```

### 3️⃣ Start the Dockerized Pipeline
```bash
docker-compose up --build
```

### 4️⃣ Access the Interfaces
| Service | URL |
|:--------|:----|
| **Kibana Dashboard** | http://localhost:5601 |
| **Elasticsearch** | http://localhost:9200 |
| **Kafka UI (optional)** | http://localhost:8080 |

---

## 📊 Example Output

**Sample Sentiment Scores (from VADER):**

| Headline | Sentiment | Score |
|:----------|:-----------|:-------|
| “Trump rallies supporters in key state” | Positive | 0.64 |
| “Critics question Trump policy decision” | Negative | -0.55 |
| “Trump discusses economy in TV interview” | Neutral | 0.05 |

---

## 🧪 Key Highlights

✅ Real-time **NLP sentiment classification**  
✅ End-to-end **data engineering pipeline**  
✅ Modular & **containerized architecture**  
✅ Interactive **Kibana dashboards**  
✅ Scalable and **production-ready** design  

---

## 🔮 Future Enhancements

- 🤖 Integrate **transformer-based sentiment models** (e.g., BERT, RoBERTa).  
- 📦 Add **MongoDB or PostgreSQL** for historical archiving.  
- 🧱 Include **alerting system** for sentiment spikes.  
- 🕸️ Expand to multiple political entities or keywords.  

---

## 📜 License
This project is licensed under the [MIT License](LICENSE).

---

## 🌟 Acknowledgments

Special thanks to:
- **News API** for live data streams  
- **Apache Kafka** for distributed streaming  
- **Elasticsearch & Kibana** for analytics  
- **Docker** for seamless deployment  

---

> _"Turning real-time data into real-time insight — powered by AI and modern data engineering."_
