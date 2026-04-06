## Postgres (teori, opgaver og extensions-demo i Docker)

Dette repo indeholder undervisningsmateriale til PostgreSQL – med fokus på at vise, hvor meget mere man kan, når man bruger udvidelser.

### Kom i gang (Docker)

Kør i repo-roden:

```bash
docker compose up --build
```

Forbind:

```bash
psql "postgresql://postgres:postgres@localhost:5432/demo"
```

Nulstil alt (kør init-scripts igen):

```bash
docker compose down -v
docker compose up --build
```

### Materiale

- **Teori/guide**: se `docs/`
- **Auto-demo SQL**: se `docker/initdb/`
- **Ældre/løse scripts**: se `Extensions/`

