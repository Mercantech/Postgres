CREATE TABLE IF NOT EXISTS places (
  place_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  geom GEOGRAPHY(Point, 4326) NOT NULL
);

INSERT INTO places (name, geom) VALUES
  ('Aalborg', ST_GeogFromText('POINT(9.9217 57.0488)')),
  ('Aarhus',  ST_GeogFromText('POINT(10.2039 56.1629)')),
  ('Odense',  ST_GeogFromText('POINT(10.3883 55.4038)')),
  ('København', ST_GeogFromText('POINT(12.5683 55.6761)'))
ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_places_geom ON places USING GIST (geom);

-- Find nærmeste by til et punkt (fx “hvor er jeg?”)
CREATE OR REPLACE FUNCTION nearest_place(lon double precision, lat double precision)
RETURNS TABLE(name text, meters double precision) AS $$
  SELECT p.name, ST_Distance(p.geom, ST_MakePoint(lon, lat)::geography) AS meters
  FROM places p
  ORDER BY p.geom <-> ST_MakePoint(lon, lat)::geography
  LIMIT 1;
$$ LANGUAGE sql;

SELECT * FROM nearest_place(10.0, 56.0);

