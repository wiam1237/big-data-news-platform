-- Initialisation des bases de données supplémentaires
CREATE DATABASE dwh_db;
CREATE USER dwh_user WITH PASSWORD 'dwh_pass';
GRANT ALL PRIVILEGES ON DATABASE dwh_db TO dwh_user;

\c dwh_db
GRANT ALL ON SCHEMA public TO dwh_user;

-- Création des tables analytiques pour la couche Gold
CREATE TABLE IF NOT EXISTS fact_articles (
    id SERIAL PRIMARY KEY,
    article_hash VARCHAR(255) UNIQUE,
    title TEXT,
    author VARCHAR(255),
    published_date TIMESTAMP,
    category VARCHAR(255),
    source VARCHAR(255),
    url TEXT,
    language VARCHAR(50),
    country VARCHAR(100),
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agg_articles_per_day (
    published_date DATE PRIMARY KEY,
    article_count INT
);

CREATE TABLE IF NOT EXISTS agg_articles_per_theme (
    category VARCHAR(255),
    article_count INT,
    PRIMARY KEY (category)
);

CREATE TABLE IF NOT EXISTS agg_articles_per_source (
    source VARCHAR(255),
    article_count INT,
    PRIMARY KEY (source)
);

CREATE TABLE IF NOT EXISTS agg_articles_per_country (
    country VARCHAR(100),
    article_count INT,
    PRIMARY KEY (country)
);

CREATE TABLE IF NOT EXISTS agg_top_keywords (
    keyword VARCHAR(100),
    frequency INT,
    PRIMARY KEY (keyword)
);
