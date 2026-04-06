## `pgvector`: vektorer og “semantisk” søgning

`pgvector` tilføjer en `vector` datatype og indeksmuligheder til nærmeste-nabo søgning (k-NN).

### Hvad er en embedding?

En embedding er en liste af tal (en vektor), som repræsenterer tekst/billeder/ting i et rum, hvor “nærhed” ~ “lighed”.

I praksis kommer embeddings typisk fra en ML-model, men her bruger vi små 3D vektorer, så du kan forstå ideen uden ML først.

### Distance-mål

`pgvector` understøtter flere mål, typisk:

- L2 (euclidisk) distance
- cosine distance
- inner product

I demoen bruges L2 operatoren:

- `embedding <-> query_vector`

### Indeks: `ivfflat`

`ivfflat` er et approximate indeks (hurtigt, men kan misse den absolut bedste nabo).

- Kræver `ANALYZE` og ofte tuning (`lists`, `probes`).
- Godt til store datasæt, men til små demoer er det primært for at vise konceptet.

### Demo i dette repo

Init-scriptet `docker/initdb/60-pgvector-demo.sql`:

- opretter `documents` med en `vector(3)`
- indsætter 3 demo dokumenter
- laver et `ivfflat` indeks
- kører en nearest-neighbor query

Øvelser:

- Tilføj 20 rækker og prøv forskellige query-vektorer.
- Skift til cosine distance og diskuter forskellen.

