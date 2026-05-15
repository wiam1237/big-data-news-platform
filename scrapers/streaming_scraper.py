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



import feedparser

def scrape_bbc_live():
    """
    Scrape le flux RSS de BBC News pour obtenir des articles réels en streaming.
    """
    rss_url = "http://feeds.bbci.co.uk/news/rss.xml"
    feed = feedparser.parse(rss_url)
    articles = []
    
    for entry in feed.entries:
        article = {
            "title": entry.title,
            "author": entry.get('author', 'BBC News'),
            "published_date": entry.get('published', datetime.utcnow().isoformat()),
            "category": entry.get('category', 'News'),
            "content": entry.summary if 'summary' in entry else entry.title,
            "source": "BBC News",
            "url": entry.link,
            "type": "streaming"
        }
        articles.append(article)
    return articles

def start_streaming():
    print(f"Starting REAL streaming scraper... connecting to {KAFKA_BROKER}")
    producer = None
    processed_urls = set()
    
    while True:
        try:
            if producer is None:
                producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
            
            articles = scrape_bbc_live()
            new_articles_count = 0
            
            for article in articles:
                if article['url'] not in processed_urls:
                    producer.send(TOPIC_NAME, article)
                    processed_urls.add(article['url'])
                    new_articles_count += 1
                    # Maintenir la taille du set pour éviter de saturer la mémoire
                    if len(processed_urls) > 1000:
                        processed_urls.clear()
            
            if new_articles_count > 0:
                print(f"Sent {new_articles_count} NEW articles to Kafka from BBC.")
            
            time.sleep(30) # Vérifier les nouveaux articles toutes les 30 secondes
        except Exception as e:
            print(f"Error in streaming scraper: {e}")
            producer = None
            time.sleep(10)

if __name__ == "__main__":
    # Attendre que Kafka soit prêt
    time.sleep(20) 
    start_streaming()
