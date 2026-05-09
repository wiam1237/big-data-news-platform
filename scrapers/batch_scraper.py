import json
import os
import time
import hashlib
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer

# Ce script est conçu pour être lancé soit manuellement, soit par Airflow
# Il publiera également sur Kafka, mais avec un statut de "batch"

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'news_batch'

def scrape_hespress_rss():
    """
    Scrape les articles d'Hespress via leur flux RSS et BeautifulSoup.
    C'est plus stable que de scraper le HTML complet et évite les blocages.
    """
    url = "https://fr.hespress.com/feed"
    articles = []
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, features="xml")
            items = soup.findAll('item')
            
            for item in items:
                title = item.title.text if item.title else "Sans titre"
                link = item.link.text if item.link else ""
                pubDate = item.pubDate.text if item.pubDate else datetime.utcnow().isoformat()
                category = item.category.text if item.category else "Général"
                description = item.description.text if item.description else ""
                creator = item.find('dc:creator')
                author = creator.text if creator else "Hespress"
                
                article = {
                    "title": title,
                    "author": author,
                    "published_date": pubDate,
                    "category": category,
                    "content": description,
                    "source": "Hespress",
                    "url": link,
                    "type": "batch",
                    "hash": hashlib.md5(link.encode('utf-8')).hexdigest()
                }
                articles.append(article)
    except Exception as e:
        print(f"Erreur lors du scraping de Hespress: {e}")
        
    return articles

def run_batch():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("Démarrage du Batch Scraper...")
    articles = scrape_hespress_rss()
    print(f"{len(articles)} articles récupérés depuis Hespress.")
    
    for article in articles:
        producer.send(TOPIC_NAME, article)
        
    producer.flush()
    print("Batch Scraping terminé.")

if __name__ == "__main__":
    run_batch()
