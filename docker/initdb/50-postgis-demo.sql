-- PostGIS “kæmpe-demo”: points, lines, polygons, indekser, afstand, buffer, within/intersects, KNN og GeoJSON.
--
-- Tip til undervisning: kør queries enkeltvis og forklar hvad hver funktion gør.

-- 1) Punkter (geography): lette, korrekte afstande i meter
CREATE TABLE IF NOT EXISTS places (
  place_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  geom GEOGRAPHY(Point, 4326) NOT NULL
);

INSERT INTO places (name, geom) VALUES
  ('Aalborg', ST_GeogFromText('POINT(9.9217 57.0488)')),
  ('Aarhus',  ST_GeogFromText('POINT(10.2039 56.1629)')),
  ('Odense',  ST_GeogFromText('POINT(10.3883 55.4038)')),
  ('København', ST_GeogFromText('POINT(12.5683 55.6761)')),
  ('Esbjerg', ST_GeogFromText('POINT(8.4519 55.4765)')),
  ('Randers', ST_GeogFromText('POINT(10.0364 56.4607)')),
  ('Kolding', ST_GeogFromText('POINT(9.4722 55.4904)')),
  ('Horsens', ST_GeogFromText('POINT(9.8500 55.8607)'))
ON CONFLICT (name) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_places_geom ON places USING GIST (geom);

-- Nærmeste sted (KNN): <-> bruger indekset til “find nærmeste hurtigt”
CREATE OR REPLACE FUNCTION nearest_place(lon double precision, lat double precision)
RETURNS TABLE(name text, meters double precision) AS $$
  SELECT p.name, ST_Distance(p.geom, ST_MakePoint(lon, lat)::geography) AS meters
  FROM places p
  ORDER BY p.geom <-> ST_MakePoint(lon, lat)::geography
  LIMIT 1;
$$ LANGUAGE sql STABLE;

-- 2) Polygons (geometry): typisk godt til “områder” (kommune, zone, geofence)
CREATE TABLE IF NOT EXISTS zones (
  zone_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  geom GEOMETRY(Polygon, 4326) NOT NULL
);

-- To simple demozoner (approx): “Jylland-ish” og “Sjælland-ish”
INSERT INTO zones (name, geom) VALUES
  (
    'Jylland (demo-zone)',
    ST_GeomFromText('POLYGON((8.0 54.6, 11.2 54.6, 11.2 57.8, 8.0 57.8, 8.0 54.6))', 4326)
  ),
  (
    'Sjælland (demo-zone)',
    ST_GeomFromText('POLYGON((11.0 54.9, 13.6 54.9, 13.6 56.2, 11.0 56.2, 11.0 54.9))', 4326)
  )
ON CONFLICT (name) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_zones_geom ON zones USING GIST (geom);

-- 3) Linjer (geometry): ruter/strækninger. Her en “Aarhus ↔ København” linje.
CREATE TABLE IF NOT EXISTS routes (
  route_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  geom GEOMETRY(LineString, 4326) NOT NULL
);

INSERT INTO routes (name, geom) VALUES
  (
    'Aarhus -> København (demo-rute)',
    ST_MakeLine(
      ST_SetSRID(ST_MakePoint(10.2039, 56.1629), 4326),
      ST_SetSRID(ST_MakePoint(12.5683, 55.6761), 4326)
    )
  )
ON CONFLICT (name) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_routes_geom ON routes USING GIST (geom);

-- ------------------------------------------------------------
-- “KØR DEMO” QUERIES (de her er lækre at køre i web-appen)
-- ------------------------------------------------------------

-- A) Afstande i meter (geography)
CREATE OR REPLACE VIEW v_place_distances_to_cph AS
SELECT
  p.name,
  round(ST_Distance(p.geom, (SELECT geom FROM places WHERE name='København'))::numeric, 0) AS meters_to_cph
FROM places p
ORDER BY meters_to_cph;

-- B) Alle steder indenfor 150 km fra København (buffer som geography)
CREATE OR REPLACE VIEW v_places_within_150km_of_cph AS
SELECT
  p.name,
  round(ST_Distance(p.geom, c.geom)::numeric, 0) AS meters
FROM places p
CROSS JOIN (SELECT geom FROM places WHERE name='København') c
WHERE ST_DWithin(p.geom, c.geom, 150000)
ORDER BY meters;

-- C) Point-in-polygon: hvilke byer ligger i hvilke zoner
CREATE OR REPLACE VIEW v_places_in_zones AS
SELECT
  p.name AS place,
  z.name AS zone
FROM places p
JOIN zones z
  ON ST_Within(p.geom::geometry, z.geom)
ORDER BY zone, place;

-- D) Intersections: hvilke zoner skærer ruten igennem
CREATE OR REPLACE VIEW v_route_zone_intersections AS
SELECT
  r.name AS route,
  z.name AS zone,
  ST_AsText(ST_Intersection(r.geom, z.geom)) AS intersection_wkt
FROM routes r
JOIN zones z
  ON ST_Intersects(r.geom, z.geom)
ORDER BY route, zone;

-- E) Bounding box “pre-filter” (hurtig grov filtrering før præcis geometri-test)
--    Her: steder i en “kasse” omkring Aarhus.
CREATE OR REPLACE VIEW v_places_in_bbox_around_aarhus AS
WITH bbox AS (
  SELECT ST_MakeEnvelope(9.5, 55.8, 11.0, 56.7, 4326) AS g
)
SELECT p.name
FROM places p, bbox
WHERE p.geom::geometry && bbox.g
ORDER BY p.name;

-- F) Transform (4326 -> 3857) og længde i meter af en linje
--    (geometry længde i 4326 giver grader; transformér før meter)
CREATE OR REPLACE VIEW v_route_length_meters AS
SELECT
  name,
  round(ST_Length(ST_Transform(geom, 3857))::numeric, 0) AS approx_length_m
FROM routes;

-- G) GeoJSON output (perfekt til web/visualisering)
CREATE OR REPLACE VIEW v_places_geojson AS
SELECT
  place_id,
  name,
  ST_AsGeoJSON(geom::geometry)::json AS geojson
FROM places
ORDER BY name;

-- H) Nærmeste sted til et punkt (funktion-call)
SELECT * FROM nearest_place(10.0, 56.0);

