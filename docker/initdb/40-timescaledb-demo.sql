-- TimescaleDB “større demo”: hypertables, chunk-inspektion, gapfill, latest pr. sensor,
-- moving average, continuous aggregates, retention/compression policies.

CREATE TABLE IF NOT EXISTS conditions (
    time        TIMESTAMPTZ       NOT NULL,
    sensor_id   INTEGER           NOT NULL,
    value       DOUBLE PRECISION  NULL
);

-- Gør tabellen til en hypertable (idempotent)
SELECT create_hypertable('conditions', by_range('time', INTERVAL '7 days'), if_not_exists => TRUE);

-- Gode indekser til typiske tidsserie-queries
CREATE INDEX IF NOT EXISTS idx_conditions_time_desc ON conditions (time DESC);
CREATE INDEX IF NOT EXISTS idx_conditions_sensor_time_desc ON conditions (sensor_id, time DESC);

-- Generér data (kun hvis tabellen er tom)
INSERT INTO conditions (time, sensor_id, value)
SELECT
  generate_series(
    timestamp '2024-01-01 00:00:00',
    timestamp '2024-03-01 00:00:00',
    interval '1 hour'
  ) AS time,
  (random() * 5 + 1)::int AS sensor_id,
  random() * 100 AS value
WHERE NOT EXISTS (SELECT 1 FROM conditions LIMIT 1);

-- Time-bucket eksempel
CREATE OR REPLACE VIEW v_conditions_weekly AS
SELECT
  time_bucket('7 days', time) AS bucket,
  count(*) AS number_of_rows,
  avg(value) AS average_value
FROM conditions
GROUP BY bucket
ORDER BY bucket;

-- “Latest reading” pr. sensor (klassisk IoT use-case)
CREATE OR REPLACE VIEW v_latest_reading_per_sensor AS
SELECT DISTINCT ON (sensor_id)
  sensor_id,
  time,
  value
FROM conditions
ORDER BY sensor_id, time DESC;

-- Moving average over sidste 24 målinger pr. sensor
CREATE OR REPLACE VIEW v_moving_avg_24h AS
SELECT
  time,
  sensor_id,
  value,
  avg(value) OVER (
    PARTITION BY sensor_id
    ORDER BY time
    ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
  ) AS moving_avg_24
FROM conditions;

-- Gapfill (udfyld manglende tidsintervaller) – super til visualisering/dashboards
-- Bemærk: vi genererer “huller” ved at filtrere nogle rækker væk.
CREATE OR REPLACE VIEW v_gapfill_hourly_sensor_1 AS
WITH sparse AS (
  SELECT *
  FROM conditions
  WHERE sensor_id = 1
    AND (extract(hour from time)::int % 6) <> 0  -- fjerner hver 6. time som “hul”
)
SELECT
  time_bucket_gapfill('1 hour', time) AS bucket,
  locf(avg(value)) AS avg_value_locf
FROM sparse
WHERE time >= '2024-01-10' AND time < '2024-01-13'
GROUP BY bucket
ORDER BY bucket;

-- Kontinuerlig aggregering (lækkert “wow” demo)
CREATE MATERIALIZED VIEW IF NOT EXISTS conditions_daily_avg
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', time) AS day,
  sensor_id,
  avg(value) AS avg_value,
  count(*) AS n
FROM conditions
GROUP BY day, sensor_id;

-- Flere continuous aggregates (typisk “dashboard-lag”)
CREATE MATERIALIZED VIEW IF NOT EXISTS conditions_hourly_avg
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS hour,
  sensor_id,
  avg(value) AS avg_value,
  min(value) AS min_value,
  max(value) AS max_value,
  count(*) AS n
FROM conditions
GROUP BY hour, sensor_id;

-- Chunk/metadata: så elever kan se at hypertable = mange chunks
CREATE OR REPLACE VIEW v_conditions_chunks AS
SELECT
  hypertable_name,
  chunk_name,
  range_start,
  range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'conditions'
ORDER BY range_start;

-- Policies (idempotent-ish via exception handling)
DO $$
BEGIN
  -- Drop data ældre end 180 dage (giver mening i “metrics” verden)
  PERFORM add_retention_policy('conditions', INTERVAL '180 days');
EXCEPTION WHEN OTHERS THEN
  NULL;
END $$;

DO $$
BEGIN
  -- Slå compression til og komprimér data ældre end 30 dage
  EXECUTE 'ALTER TABLE conditions SET (timescaledb.compress, timescaledb.compress_segmentby = ''sensor_id'')';
EXCEPTION WHEN OTHERS THEN
  NULL;
END $$;

DO $$
BEGIN
  PERFORM add_compression_policy('conditions', INTERVAL '30 days');
EXCEPTION WHEN OTHERS THEN
  NULL;
END $$;

DO $$
BEGIN
  -- Auto-refresh af continuous aggregates (så de opdateres løbende)
  PERFORM add_continuous_aggregate_policy(
    'conditions_hourly_avg',
    start_offset => INTERVAL '7 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '10 minutes'
  );
EXCEPTION WHEN OTHERS THEN
  NULL;
END $$;


