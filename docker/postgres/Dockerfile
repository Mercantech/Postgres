FROM postgres:16-bookworm

RUN set -eux; \
  apt-get update; \
  apt-get install -y --no-install-recommends ca-certificates curl gnupg; \
  rm -rf /var/lib/apt/lists/*; \
  \
  curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey | gpg --dearmor > /usr/share/keyrings/timescaledb.gpg; \
  echo "deb [signed-by=/usr/share/keyrings/timescaledb.gpg] https://packagecloud.io/timescale/timescaledb/debian/ bookworm main" > /etc/apt/sources.list.d/timescaledb.list; \
  \
  apt-get update; \
  apt-get install -y --no-install-recommends \
    postgresql-16-cron \
    postgresql-16-postgis-3 \
    postgresql-16-pgvector \
    timescaledb-2-postgresql-16 \
  ; \
  rm -rf /var/lib/apt/lists/*

