CREATE TABLE IF NOT EXISTS conditions (
    time        TIMESTAMPTZ       NOT NULL,
    sensor_id   INTEGER           NOT NULL,
    value       DOUBLE PRECISION  NULL
);

-- Gør tabellen til en hypertable (idempotent)
SELECT create_hypertable('conditions', by_range('time', INTERVAL '7 days'), if_not_exists => TRUE);

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

