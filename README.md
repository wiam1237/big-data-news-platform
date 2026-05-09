# Projet d'Architecture de Données Big Data - Scraping et Analyse d'Actualités

Ce projet implémente une plateforme Big Data de bout en bout capable de scraper des articles de presse, de les traiter en flux continu et par lots, et d'en tirer des indicateurs analytiques visualisables sur des Dashboards.

## Architecture

L'architecture s'articule autour des composants suivants :
1. **Scraping** : `BeautifulSoup` (Batch sur RSS) et Streaming émulé vers **Kafka**.
2. **Streaming & Ingestion** : **Apache Kafka** reçoit les articles et un Kafka Consumer Python gère l'écriture temps réel.
3. **Data Lake (S3 Compatible)** : **MinIO** centralise les données dans une Architecture Médaillon (`bronze`, `silver`, `gold`).
4. **ETL & Traitements** : Scripts Python (`pandas`, `langdetect`) chargés de nettoyer, transformer et charger les données aux différents stades.
5. **Orchestration** : **Apache Airflow** lance les DAGs pour orchestrer le scraping Batch et l'ETL (Bronze -> Silver -> Gold -> Data Warehouse).
6. **Data Warehouse** : **PostgreSQL** reçoit les données Gold pour faciliter l'analyse décisionnelle.
7. **Visualisation** : **Metabase** (connecté à PostgreSQL) pour la création et la visualisation de tableaux de bord en temps réel.

## Déploiement avec Docker Compose

L'intégralité de l'environnement est packagé sous forme de conteneurs Docker.

### Prérequis
- Docker et Docker Compose installés sur la machine.

### Installation et Démarrage
1. Placez-vous à la racine du projet :
   ```bash
   cd "projet architecture de donnees"
   ```
2. Démarrez la plateforme avec Docker Compose en téléchargeant et construisant les images (processus d'environ 2 à 5 minutes) :
   ```bash
   docker-compose up -d --build
   ```
3. Vérifiez le statut des conteneurs :
   ```bash
   docker-compose ps
   ```

### Accès aux services (après démarrage)
- **Airflow Web UI** : [http://localhost:8080](http://localhost:8080) (Identifiants : `admin` / `admin`)
- **MinIO Console** : [http://localhost:9001](http://localhost:9001) (Identifiants : `minioadmin` / `minioadmin`)
- **Metabase UI** : [http://localhost:3000](http://localhost:3000) (Créer le compte à la première connexion, ajouter `postgres` comme base, db `dwh_db`, user `dwh_user`, mot de passe `dwh_pass`).
- **Kafka / Zookeeper** : Ports internes 9092/9093.
- **PostgreSQL Data Warehouse** : Port `5432` (User `dwh_user`, mot de passe `dwh_pass`, DataBase `dwh_db`).

## Déroulement du Pipeline

1. Lors du lancement, Airflow intègre le DAG `news_processing_pipeline`. Ce DAG s'exécute de façon périodique (Batch).
2. Le scraper Streaming envoie des articles aléatoires vers Kafka toutes les 10 secondes. Le Consumer Kafka les persiste en direct dans le bucket MinIO `bronze`.
3. Le DAG Airflow déclenche :
   - Un scraping d'Hespress et envoie vers Kafka.
   - Une transformation **Bronze vers Silver** (Nettoyage HTML, détection langue, rejet des données invalides).
   - Une transformation **Silver vers Gold** (Agrégations par source, langue, création de base de faits) ainsi que l'insertion dans **PostgreSQL**.
4. Les Dashboards dans Metabase se mettent à jour automatiquement selon les requêtes SQL ciblant les tables `fact_articles`, `agg_articles_per_day`, `agg_articles_per_theme`, etc.

## Gouvernance et Qualité des Données
- Le niveau *Silver* vérifie que les longueurs de texte et les dates sont correctes, supprime les balises HTML indésirables, et enrichit avec la langue.
- Le niveau *Gold* valide la complétude en calculant un timestamp d'ingestion. La traçabilité peut se faire via l'historique complet disponible dans MinIO.
