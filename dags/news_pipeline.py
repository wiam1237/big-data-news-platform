from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Pipeline principal exécuté toutes les heures
with DAG(
    'news_processing_pipeline',
    default_args=default_args,
    description='Pipeline d\'ingestion et traitement des actualités',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['news', 'medaillon'],
) as dag:

    # 1. Scraping Batch (Hespress RSS) -> Envoie vers Kafka topic: news_batch
    # L'envoi vers MinIO sera géré par notre Kafka Consumer en streaming.
    run_batch_scraper = BashOperator(
        task_id='run_batch_scraper',
        bash_command='python /opt/airflow/scrapers/batch_scraper.py',
        env={
            'KAFKA_BROKER': 'kafka:9092'
        }
    )

    # 2. Transformation Bronze (brut) vers Silver (nettoyé)
    # L'exécution du script va lire MinIO Bronze, nettoyer et enregistrer dans Silver.
    process_bronze_to_silver = BashOperator(
        task_id='process_bronze_to_silver',
        bash_command='python /opt/airflow/scripts/bronze_to_silver.py',
        env={
            'MINIO_ENDPOINT': 'minio:9000',
            'MINIO_ACCESS_KEY': 'minioadmin',
            'MINIO_SECRET_KEY': 'minioadmin'
        }
    )

    # 3. Transformation Silver vers Gold (analytique) et Data Warehouse
    process_silver_to_gold = BashOperator(
        task_id='process_silver_to_gold',
        bash_command='python /opt/airflow/scripts/silver_to_gold.py',
        env={
            'MINIO_ENDPOINT': 'minio:9000',
            'MINIO_ACCESS_KEY': 'minioadmin',
            'MINIO_SECRET_KEY': 'minioadmin',
            'DB_CONN': 'postgresql+psycopg2://dwh_user:dwh_pass@postgres:5432/dwh_db'
        }
    )

    # Ordre d'exécution
    run_batch_scraper >> process_bronze_to_silver >> process_silver_to_gold
