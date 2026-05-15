import os
import json
import pandas as pd
from minio import Minio
import langdetect
from bs4 import BeautifulSoup
import io

# Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000').replace('http://', '')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

BRONZE_BUCKET = 'bronze'
SILVER_BUCKET = 'silver'

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def ensure_bucket(bucket_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

def clean_html(raw_html):
    if not raw_html:
        return ""
    try:
        soup = BeautifulSoup(raw_html, "html.parser")
        return soup.get_text(separator=" ").strip()
    except:
        return raw_html

def detect_language(text):
    if not text or len(text) < 10:
        return "unknown"
    try:
        return langdetect.detect(text)
    except:
        return "unknown"

def process_bronze_to_silver():
    ensure_bucket(SILVER_BUCKET)
    
    print("Début du traitement Bronze -> Silver")
    objects = minio_client.list_objects(BRONZE_BUCKET, recursive=True)
    
    data_list = []
    processed_files = []
    
    for obj in objects:
        if not obj.object_name.endswith('.json'):
            continue
            
        try:
            response = minio_client.get_object(BRONZE_BUCKET, obj.object_name)
            content = response.read().decode('utf-8')
            article = json.loads(content)
            
            # Qualité de données: Exclusion si titre manquant, contenu trop court ou DATE manquante
            if not article.get('title') or len(str(article.get('content', ''))) < 20 or not article.get('published_date'):
                print(f"Article rejeté (Qualité): {obj.object_name}")
                continue
                
            # Nettoyage
            article['content'] = clean_html(article.get('content', ''))
            article['title'] = clean_html(article.get('title', ''))
            
            # Détection de la langue
            article['language'] = detect_language(article['content'])
            
            # Mapping pays simplifié (peut être enrichi)
            source = article.get('source', '').lower()
            if 'hespress' in source or 'akhbarona' in source or 'barlamane' in source:
                article['country'] = 'Morocco'
            elif 'bbc' in source or 'reuters' in source or 'cnn' in source:
                article['country'] = 'International'
            else:
                article['country'] = 'Unknown'
            
            data_list.append(article)
            processed_files.append(obj.object_name)
        except Exception as e:
            print(f"Erreur traitement {obj.object_name}: {e}")
            
    if not data_list:
        print("Aucune nouvelle donnée trouvée dans Bronze.")
        return
        
    df = pd.DataFrame(data_list)
    
    # Suppression des doublons basés sur l'URL
    if 'url' in df.columns:
        df = df.drop_duplicates(subset=['url'])
        
    # Enregistrement en Parquet
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    filename = f"silver_data_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.parquet"
    
    minio_client.put_object(
        SILVER_BUCKET,
        filename,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    
    # Optionnel: supprimer ou archiver les fichiers bronze traités
    for f in processed_files:
        minio_client.remove_object(BRONZE_BUCKET, f)
        
    print(f"Traitement terminé. {len(data_list)} articles enregistrés dans Silver: {filename}")

if __name__ == "__main__":
    process_bronze_to_silver()
