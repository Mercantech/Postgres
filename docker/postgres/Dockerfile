FROM timescale/timescaledb:latest-pg16

RUN set -eux; \
  apt-get update; \
  apt-get install -y --no-install-recommends \
    ca-certificates \
    postgresql-16-cron \
    postgresql-16-postgis-3 \
    postgresql-16-pgvector \
  ; \
  rm -rf /var/lib/apt/lists/*

