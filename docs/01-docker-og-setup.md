## Docker og setup (hurtig start)

Målet er, at alle elever kan få samme miljø på få minutter – uanset Windows/Mac/Linux.

### Forudsætninger

- Docker Desktop installeret
- `docker compose` virker i en terminal

### Start databasen

Kør i repo-roden:

```bash
docker compose up --build
```

Containeren starter:

- en PostgreSQL (pg16) med udvidelser preinstalleret
- en lille web-app (Streamlit) til at køre SQL og demoer visuelt

Init-scripts kører **første gang** (når data-volume er tomt).

Standard login:

- **host**: `localhost`
- **port**: `5432`
- **db**: `demo`
- **user**: `postgres`
- **password**: `postgres`

Eksempel med `psql`:

```bash
psql "postgresql://postgres:postgres@localhost:5432/demo"
```

### “Jeg vil starte forfra”

Docker-init scripts kører kun ved “første opstart” af et nyt data-volume. Hvis du vil nulstille:

```bash
docker compose down -v
docker compose up --build
```

### Hvad ligger hvor

- `docker-compose.yml`: starter databasen + konfigurerer preload libs
- `docker/postgres/Dockerfile`: bygger image med ekstra pakker
- `docker/initdb/*.sql`: kører automatisk ved første opstart
- `webapp/`: simpel web-app til at køre SQL og demoer

### Hurtig verifikation i SQL

```sql
SELECT extname, extversion
FROM pg_extension
ORDER BY extname;
```

### Web-app (visuel demo)

- Åbn `http://localhost:8501`
- Brug SQL editoren til at køre kommandoer
- Brug demo-dropdown til at køre hele demo-scripts

