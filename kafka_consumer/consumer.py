import json
import os
import time
import uuid
import io
from datetime import datetime
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPICS = ['news_streaming', 'news_batch']

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000').replace('http://', '')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
BRONZE_BUCKET = 'bronze'

# Initialisation client MinIO
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def ensure_bucket():
    try:
        if not minio_client.bucket_exists(BRONZE_BUCKET):
            minio_client.make_bucket(BRONZE_BUCKET)
            print(f"Bucket '{BRONZE_BUCKET}' créé.")
    except Exception as e:
        print(f"Erreur Minio: {e}")

def save_to_minio(data):
    try:
        # Création d'un nom de fichier unique basé sur la date et l'heure pour partitionnement
        now = datetime.utcnow()
        partition_path = now.strftime("year=%Y/month=%m/day=%d")
        filename = f"{partition_path}/article_{uuid.uuid4().hex}.json"
        
        json_data = json.dumps(data).encode('utf-8')
        data_stream = io.BytesIO(json_data)
        
        minio_client.put_object(
            BRONZE_BUCKET,
            filename,
            data_stream,
            length=len(json_data),
            content_type='application/json'
        )
        print(f"[{data.get('type', 'unknown')}] Sauvegardé dans {BRONZE_BUCKET}/{filename}")
    except Exception as e:
        print(f"Erreur de sauvegarde Minio: {e}")

def consume_messages():
    ensure_bucket()
    print(f"Connexion à Kafka {KAFKA_BROKER} sur les topics {TOPICS}...")
    
    # Attente pour que Kafka soit bien démarré
    time.sleep(15)
    
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='data_lake_ingestion_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("Démarrage de la consommation en temps réel...")
    for message in consumer:
        article = message.value
        save_to_minio(article)

if __name__ == "__main__":
    consume_messages()
