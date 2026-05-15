import os
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine
import io
import re
from collections import Counter

# Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000').replace('http://', '')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
DB_CONN = os.getenv('DB_CONN', 'postgresql+psycopg2://dwh_user:dwh_pass@localhost:5432/dwh_db')

SILVER_BUCKET = 'silver'
GOLD_BUCKET = 'gold'

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

engine = create_engine(DB_CONN)

def ensure_bucket(bucket_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

def extract_keywords(text_series, top_n=20):
    # Simple keyword extraction (word frequency)
    all_text = " ".join(text_series.fillna('').astype(str))
    words = re.findall(r'\w{4,}', all_text.lower()) # words with 4+ chars
    # Basic stop words
    stop_words = {'avec', 'dans', 'pour', 'plus', 'avec', 'cette', 'fait', 'etre', 'mais', 'nous', 'vous', 'elles'}
    words = [w for w in words if w not in stop_words and not w.isdigit()]
    counts = Counter(words)
    return counts.most_common(top_n)

def process_silver_to_gold():
    ensure_bucket(GOLD_BUCKET)
    
    print("Début du traitement Silver -> Gold")
    objects = minio_client.list_objects(SILVER_BUCKET, recursive=True)
    
    df_list = []
    processed_files = []
    
    for obj in objects:
        if not obj.object_name.endswith('.parquet'):
            continue
        try:
            response = minio_client.get_object(SILVER_BUCKET, obj.object_name)
            parquet_data = io.BytesIO(response.read())
            df = pd.read_parquet(parquet_data)
            df_list.append(df)
            processed_files.append(obj.object_name)
        except Exception as e:
            print(f"Erreur lecture {obj.object_name}: {e}")
            
    if not df_list:
        print("Aucune donnée Parquet trouvée dans Silver.")
        return
        
    final_df = pd.concat(df_list, ignore_index=True)
    
    # Enrichissement supplémentaire (par exemple, formater les dates)
    final_df['published_date'] = pd.to_datetime(final_df['published_date'], errors='coerce')
    final_df['ingested_at'] = pd.Timestamp.now()
    final_df['date_only'] = final_df['published_date'].dt.date
    
    # 1. Ecriture dans Data Warehouse: PostgreSQL (Gold)
    # Insert Fact Table
    fact_cols = ['title', 'author', 'published_date', 'category', 'source', 'url', 'language', 'country', 'ingested_at']
    fact_df = final_df[[c for c in fact_cols if c in final_df.columns]].copy()
    fact_df.to_sql('fact_articles', engine, if_exists='append', index=False)
    
    # Insert Aggregations
    if 'date_only' in final_df.columns:
        agg_day = final_df.groupby('date_only').size().reset_index(name='article_count')
        agg_day.rename(columns={'date_only': 'published_date'}, inplace=True)
        agg_day.to_sql('agg_articles_per_day', engine, if_exists='replace', index=False)
        
    if 'category' in final_df.columns:
        agg_theme = final_df.groupby('category').size().reset_index(name='article_count')
        agg_theme.to_sql('agg_articles_per_theme', engine, if_exists='replace', index=False)
        
    if 'source' in final_df.columns:
        agg_source = final_df.groupby('source').size().reset_index(name='article_count')
        agg_source.to_sql('agg_articles_per_source', engine, if_exists='replace', index=False)

    if 'country' in final_df.columns:
        agg_country = final_df.groupby('country').size().reset_index(name='article_count')
        agg_country.to_sql('agg_articles_per_country', engine, if_exists='replace', index=False)

    # Keyword extraction from titles
    if 'title' in final_df.columns:
        top_k = extract_keywords(final_df['title'])
        k_df = pd.DataFrame(top_k, columns=['keyword', 'frequency'])
        k_df.to_sql('agg_top_keywords', engine, if_exists='replace', index=False)


    # 2. Sauvegarde sous forme analytique dans le Gold Bucket (Parquet)
    parquet_buffer = io.BytesIO()
    final_df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    filename = f"gold_analytics_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.parquet"
    minio_client.put_object(
        GOLD_BUCKET,
        filename,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    
    # Optionnel : nettoyer le Silver pour éviter de retraiter les mêmes données
    for f in processed_files:
        minio_client.remove_object(SILVER_BUCKET, f)
        
    print(f"Traitement terminé. Données poussées vers DWH et Gold bucket: {filename}")

if __name__ == "__main__":
    process_silver_to_gold()
