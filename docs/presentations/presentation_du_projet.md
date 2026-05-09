# Plateforme Big Data d'Analyse d'Actualités - Présentation du Projet 📰📊

## 1. Contexte du Projet
Dans un monde où l'information circule à une vitesse fulgurante avec des milliers d'articles de presse publiés quotidiennement, la capacité à capter, analyser et extraire de la valeur de ce flux de données est primordiale. Les entreprises, les acteurs des médias et les observatoires de l'information ont besoin d'outils automatisés pour :
- Identifier les tendances dominantes (Top Keywords).
- Suivre le volume de publication et les thématiques en temps réel.
- Disposer d'une base consolidée, nettoyée et facilement exploitable (Single Source of Truth).

**Objectif du Projet** : Concevoir et déployer une plateforme Big Data complète permettant l'ingestion, le nettoyage, le stockage et la restitution analytique des articles de presse, via une Architecture distribuée moderne.

---

## 2. L'Architecture Médaillon
Le projet repose sur l'approche reconnue "Medallion Architecture" qui assure qualité, évolutivité et clarté sur la maturité de la donnée.

### 🥉 Couche Bronze (Données Brutes / Raw)
- C'est la zone d'atterrissage. Elle contient les données directement ingérées par le pipeline en Streaming (Simulé) et en Batch (via flux RSS de *Hespress* ou autre plateforme).
- Format stocké : Enregistrements **JSON** isolés.
- Les données contiennent toutes les balises originelles (HTML, caractères non standards).

### 🥈 Couche Silver (Données Standardisées / Cleansed)
- Il s'agit des données purgées et validées.
- **Traitements appliqués** : Suppression des balises HTML (parsing BeautifulSoup), détection de la langue de l'article avec la librairie `langdetect`, exclusion des articles ne dépassant pas une taille minimale (contrôle de Qualité).
- Format stocké : **Parquet**.

### 🥇 Couche Gold (Données Analytiques / Aggregated)
- Les données sont prêtes à l'emploi.
- Génération des tables de Faits et de Dimensions au sein de notre Data Warehouse.
- **Traitements appliqués** : Agrégation par journée, sources et catégorie. Format adapté aux modèles en étoile de la BI.

---

## 3. Choix Technologiques 🛠️

L'ensemble de la Stack est **Open Source** et conteneurisé.

| Composant | Technologie | Justification |
| :--- | :--- | :--- |
| **Ingestion Streaming** | **Apache Kafka** | La norme de l'industrie pour les architectures distribuées tolérantes aux pannes reposant sur le modèle Publisher/Subscriber. |
| **Scraping & Python ETL** | **Python (Pandas, BeautifulSoup)** | Excellentes librairies pour extraire l'information et transformer un grand volume de données en mémoire. |
| **Data Lake** | **MinIO** | Stockage Objet distribué compatible avec S3. Solution idéale, facile à scaler pour gérer la Data. |
| **Data Warehouse** | **PostgreSQL** | Moteur de Base de Données Relationnelle robuste, supportant très bien les workloads analytiques de taille moyenne. |
| **Orchestration** | **Apache Airflow** | Parfait pour représenter l'ETL de façon acyclique (DAGs), définir l'ordre et monitorer les tâches planifiées. |
| **Visualisation** | **Metabase** | Solution de Business Intelligence performante, conviviale et facile à déployer pour nos dashboards. |
| **Supervision / Monitoring** | **Prometheus & Grafana** | Stack reconnue pour scrapper, conserver des historiques et afficher des alertes et tableaux de bord de santé du cluster. |
| **Packaging & Scaling** | **Docker Compose / Kubernetes (Helm)** | Permet la reproductibilité de l'environnement (Infrastructure as Code). K8s permet l'orchestration à l'échelle cloud. |

---

## 4. Pipeline et Flux de Données 🔄

1. **Génération & Collecte** : 
   - Le script de **Streaming** génère en continu des événements envoyés dans *Kafka* (Topic `news_streaming`).
   - Le Job de **Batch** piloté par Airflow s'active périodiquement, extrait des articles via un flux RSS, et insère les éléments JSON sur Kafka (Topic `news_batch`).
2. **Stockage Lake (Ingestion)** : Le `Kafka Consumer` écoute ces topics et stocke chaque article au format JSON dans MinIO (Bucket *Bronze*).
3. **ETL & Transformations** :
   - `Airflow` effectue le traitement *Bronze vers Silver* : lecture des JSON via Pandas, parsing HTML, déduction de langue, puis dump dans MinIO (Bucket *Silver*) en format Parquet.
   - `Airflow` effectue ensuite la phase *Silver vers Gold* : consolidation, agrégation, puis injection dans PostgreSQL et sauvegarde sur MinIO (bucket *Gold*).
4. **Restitution** : L'utilisateur se connecte sur *Metabase*, conçoit des requêtes SQL ou graphiques interrogeant le DWH (*PostgreSQL*) pour tracer l'actualité.

---

## 5. Gouvernance et Monitoring

### Qualité des Données :
- **Validité & Complétude** : Les scripts de transformation suppriment ou archivent les anomalies en étape *Silver*. L'intégrité référentielle en BDD est maintenue.
- Chaque article stocké bénéficie de la traçabilité via son hash ou son timestamp d'ingestion.

### Monitoring Opérationnel :
- Les données de performances CPU/RAM des conteneurs ainsi que les métriques exposées par l'infrastructure (*Kafka*, *MinIO*) sont aspirées par *Prometheus*.
- *Grafana* permet d'afficher l'état de l'orchestration.

---

> Ce projet intègre les paradigmes classiques de l'ingénierie Data avancée, préparant ce socle technologique pour n'importe quelle expansion structurelle (ajout de composants Big Data majeurs comme Apache Spark / Hadoop ou intégration IA LLMs).
