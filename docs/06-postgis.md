## PostGIS: geodata i databasen

PostGIS tilføjer geometri/geografi-typer og rumlige funktioner til Postgres.

### Geometri vs geografi

- **Geometry**: “flad” projektion, hurtig, men afstande afhænger af projektion.
- **Geography**: tager højde for jordens krumning (WGS84), afstande i meter (typisk lettere at forklare).

I demoen bruges `GEOGRAPHY(Point, 4326)`.

### Rumlige indekser

GiST indekser gør “find nærmeste” og “find inden for” hurtige.

- `CREATE INDEX ... USING GIST (geom);`
- Operatoren `<->` kan bruges til nearest-neighbor søgning (KNN) når indekset understøtter det.

### Klassiske funktioner

- `ST_Distance(a, b)` (afstand)
- `ST_Within(a, polygon)` (inde i område)
- `ST_Buffer(geom, meters)` (zone rundt om)

### Demo i dette repo

Init-scriptet `docker/initdb/50-postgis-demo.sql`:

- opretter `places` med danske byer
- indekserer geometri
- laver `nearest_place(lon, lat)` og kalder den

Øvelser:

- Tilføj flere byer og mål “nearest”.
- Lav en “find alle steder indenfor 50km” funktion.

