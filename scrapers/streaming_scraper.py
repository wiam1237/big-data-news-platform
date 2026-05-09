import time
import json
import os
import random
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'news_streaming'

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def scrape_bbc_mock():
    # En situation réelle, on scraperait les vrais articles.
    # Pour ne pas surcharger le serveur BBC et risquer un ban IP, on simule 
    # la récupération des articles, ou l'on scrape un flux RSS simple.
    
    # Ici, nous simulerons des événements continus pour démontrer le streaming.
    categories = ['Politics', 'Technology', 'Sports', 'Health', 'Economy']
    authors = ['John Doe', 'Jane Smith', 'Alice Johnson', 'Bob Brown']
    titles = [
        "Global Markets Rally on Tech Earnings",
        "New AI Model Surpasses Human Performance",
        "Climate Change Summit Ends with New Pledges",
        "Football Championship Finals Reached Record Viewership",
        "Breakthrough in Quantum Computing Announced"
    ]
    
    article = {
        "title": random.choice(titles) + f" - {random.randint(1,100)}",
        "author": random.choice(authors),
        "published_date": datetime.utcnow().isoformat(),
        "category": random.choice(categories),
        "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ceci est un contenu d'article simulé pour le streaming en continu sans HTML complexe.",
        "source": "BBC streaming mock",
        "url": f"https://bbc.com/news/mock-{random.randint(1000, 9999)}",
        "type": "streaming"
    }
    return article

def start_streaming():
    print(f"Starting streaming scraper... connecting to {KAFKA_BROKER}")
    while True:
        try:
            article = scrape_bbc_mock()
            producer.send(TOPIC_NAME, article)
            print(f"Sent article to Kafka: {article['title']}")
            time.sleep(10) # Envoi d'un événement toutes les 10 secondes
        except Exception as e:
            print(f"Error scraping or sending to Kafka: {e}")
            time.sleep(5)

if __name__ == "__main__":
    # Attendre que Kafka soit prêt
    time.sleep(15) 
    start_streaming()
