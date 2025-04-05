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



