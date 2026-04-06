## Oversigt: Postgres udvidelser (vis hvad Postgres kan)

PostgreSQL er ikke kun “tabeller og SELECT”. Med **udvidelser** kan Postgres også være:

- søgemaskine (fuzzy + full text)
- job-scheduler (cron)
- tidsserie-database (TimescaleDB)
- geodatabase (PostGIS)
- vektor-søgning (pgvector)
- og have indbygget kryptografi (pgcrypto)

### Det skal du kunne, når du er færdig

- **Forklare** hvad en Postgres extension er (og at den aktiveres pr. database).
- **Køre demoer** og forstå output (scores, distance, rank, chunks, osv.).
- **Vælge værktøj**: hvornår er en extension en god idé, og hvornår er det “overkill”.
- **Tænke drift/sikkerhed**: rettigheder, performance og data-håndtering.

### Sådan bruger du det her pensum (hurtigt)

Du kan køre alt i den visuelle web-app.

- **Læs** modulet i venstre side (Pensum)
- Tryk **Kør demo for dette modul** (så bliver tabeller/views oprettet)
- Kør de foreslåede SQL-queries i editoren og læs forklaringerne i Resultater

Hvis du bruger `psql`, kan du stadig forbinde direkte til databasen (se `01-docker-og-setup.md`).

### Moduler (i rækkefølge)

- **`01-docker-og-setup.md`**: start miljøet, reset volume, verificér extensions.
- **`02-pgcrypto.md`**: hashing vs kryptering, login-verifikation, følsomme felter.
- **`03-search.md`**: trigram/fuzzy + full text + ranking + indekser.
- **`04-pg-cron.md`**: planlagte jobs i DB, logs, drift og faldgruber.
- **`05-timescaledb.md`**: hypertables, chunk pruning, gapfill, continuous aggregates, policies.
- **`06-postgis.md`**: points/polygons/lines, afstand, nearest-neighbor, GeoJSON + kort.
- **`07-pgvector.md`**: embeddings, distance, nearest-neighbor og indeks-ideen.

### Mini-tjekliste (så du ved du er i mål)

- Du kan forklare forskellen på **hashing** og **kryptering**.
- Du kan forklare hvorfor et **indeks** kan gøre en query hurtigere.
- Du kan forklare hvad **rank/similarity/distance** betyder i søgning/vektor.
- Du kan forklare hvad en **hypertable/chunk** er, og hvorfor det hjælper.
- Du kan forklare hvorfor **geography** ofte er nemt (meter), og hvorfor **geometry** er fleksibelt.

