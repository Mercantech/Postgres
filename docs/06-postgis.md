## PostGIS: geodata i databasen

PostGIS tilføjer geometri/geografi-typer og rumlige funktioner til Postgres.

### Geometri vs geografi

- **Geometry**: “flad” projektion, hurtig, men afstande afhænger af projektion.
- **Geography**: tager højde for jordens krumning (WGS84), afstande i meter (typisk lettere at forklare).

I demoen bruges begge:

- `GEOGRAPHY(Point, 4326)` til afstande i meter (nemt at forklare)
- `GEOMETRY(..., 4326)` til områder/ruter (polygons/lines) og klassiske rumlige operationer

### Rumlige indekser

GiST indekser gør “find nærmeste” og “find inden for” hurtige.

- `CREATE INDEX ... USING GIST (geom);`
- Operatoren `<->` kan bruges til nearest-neighbor søgning (KNN) når indekset understøtter det.

### Klassiske funktioner

- `ST_Distance(a, b)` (afstand)
- `ST_Within(a, polygon)` (inde i område)
- `ST_Buffer(geom, meters)` (zone rundt om)
- `ST_DWithin(a, b, dist)` (hurtig “indenfor afstand”)
- `ST_Intersects(a, b)` (skærer/overlapper)
- `ST_Intersection(a, b)` (selve overlap-geometrien)
- `ST_AsGeoJSON(geom)` (klar til web/visning)

### Et vigtigt performance-trick: bounding box først

Mange operationer kan speedes op ved at lave en grov pre-filter med bounding boxes før den præcise geometri-test:

- `geom && ST_MakeEnvelope(...)`

`&&` er billig (rektangel-overlap), og derefter kan man lave `ST_Within`/`ST_Intersects` på de kandidater der er tilbage.

### Coordinate transforms (meter vs grader)

SRID 4326 (WGS84) bruger grader. For “meter” på **geometry** skal du ofte:

- `ST_Transform(geom, 3857)` (eller et lokalt/projektions-SRID)
- derefter `ST_Length` / `ST_Area`

### Demo i dette repo

Init-scriptet `docker/initdb/50-postgis-demo.sql`:

- opretter `places` (punkter), `zones` (polygons) og `routes` (lines)
- indekserer med GiST
- laver `nearest_place(lon, lat)`
- laver views der er perfekte til undervisning og “kør og forklar”

#### Kør disse i web-appen

```sql
SELECT * FROM v_place_distances_to_cph;
SELECT * FROM v_places_within_150km_of_cph;
SELECT * FROM v_places_in_zones;
SELECT * FROM v_route_zone_intersections;
SELECT * FROM v_places_geojson;
```

Øvelser:

- Tilføj flere byer og mål “nearest”.
- Lav en “find alle steder indenfor 50km” funktion med `ST_DWithin`.
- Tegn en ny zone (polygon) og find hvilke byer der ligger i den.
- Lav en rute (LineString) og find zoner den skærer.

