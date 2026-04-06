-- Kører automatisk ved første container-opstart (tomt data-volume).
-- Her gør vi alle relevante extensions tilgængelige i databasen `demo`.

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Installeret via image
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS vector;

-- TimescaleDB (kommer fra base image)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- pg_cron kræver shared_preload_libraries og cron.database_name (sat i docker-compose)
CREATE EXTENSION IF NOT EXISTS pg_cron;

