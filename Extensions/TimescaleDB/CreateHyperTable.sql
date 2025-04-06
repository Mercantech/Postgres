-- @block Først sikrer vi at timescale extension er installeret
CREATE EXTENSION IF NOT EXISTS timescaledb;


-- @block Vi laver en tabel med data, som vi kan bruge til at teste hypertable
CREATE TABLE conditions (
    time        TIMESTAMPTZ       NOT NULL,
    sensor_id   INTEGER           NOT NULL,
    value       DOUBLE PRECISION  NULL
);


-- @block Her laver vi en hypertable, på vores normale PostgreSQL tabel. Vi opdeler data i 7 dages intervaller.
SELECT create_hypertable(
  'conditions',
  by_range('time', INTERVAL '7 days')
);

-- @block Vi indsætter data i vores hypertable. Vi genererer data over en periode på 2 måneders tid.
INSERT INTO conditions (time, sensor_id, value)
SELECT
  generate_series(
    timestamp '2024-01-01 00:00:00',
    timestamp '2024-03-01 00:00:00',
    interval '1 hour'
  ) as time,
  (random() * 5 + 1)::int as sensor_id,
  random() * 100 as value;

-- @block Vis data grupperet efter uge - vi kan se, at data er opdelt i 7 dages intervaller. Hvilket den gør automatisk, når vi laver en hypertable.
SELECT 
  time_bucket('7 days', time) as bucket,
  count(*) as number_of_rows,
  avg(value) as average_value
FROM conditions 
GROUP BY bucket 
ORDER BY bucket;

-- @block Vis total antal rækker - vi kan se, at der er 1442 rækker i vores hypertable.
SELECT count(*) as total_rows FROM conditions;

-- @block Først opretter vi en normal tabel til sammenligning
CREATE TABLE normal_conditions (
    time        TIMESTAMPTZ       NOT NULL,
    sensor_id   INTEGER           NOT NULL,
    value       DOUBLE PRECISION  NULL
);

-- @block Opret indeks på time kolonnen for fair sammenligning
CREATE INDEX ON normal_conditions(time DESC);

-- @block Indsæt samme data i normal tabellen
INSERT INTO normal_conditions (time, sensor_id, value)
SELECT
  generate_series(
    timestamp '2024-01-01 00:00:00',
    timestamp '2024-03-01 00:00:00',
    interval '1 hour'
  ) as time,
  (random() * 5 + 1)::int as sensor_id,
  random() * 100 as value;

-- @block Nu kan vi lave nogle sammenligninger
--  Test 1: Simpel aggregering over en tidsperiode
EXPLAIN ANALYZE
SELECT time_bucket('1 day', time) as bucket,
       avg(value) as avg_value,
       count(*) as num_readings
FROM conditions
WHERE time >= '2024-01-01' AND time < '2024-02-01'
GROUP BY bucket
ORDER BY bucket;

EXPLAIN ANALYZE
SELECT date_trunc('day', time) as bucket,
       avg(value) as avg_value,
       count(*) as num_readings
FROM normal_conditions
WHERE time >= '2024-01-01' AND time < '2024-02-01'
GROUP BY bucket
ORDER BY bucket;

-- @block Test 2: Seneste værdier for hver sensor
EXPLAIN ANALYZE
SELECT DISTINCT ON (sensor_id)
    sensor_id,
    time,
    value
FROM conditions
ORDER BY sensor_id, time DESC;

EXPLAIN ANALYZE
SELECT DISTINCT ON (sensor_id)
    sensor_id,
    time,
    value
FROM normal_conditions
ORDER BY sensor_id, time DESC;

-- @block Test 3: Beregn gennemsnit med et rullende vindue
EXPLAIN ANALYZE
SELECT time,
       avg(value) OVER (
           ORDER BY time
           ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
       ) as moving_avg
FROM conditions
WHERE time >= '2024-01-01' AND time < '2024-01-08';

EXPLAIN ANALYZE
SELECT time,
       avg(value) OVER (
           ORDER BY time
           ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
       ) as moving_avg
FROM normal_conditions
WHERE time >= '2024-01-01' AND time < '2024-01-08';




