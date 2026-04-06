## Search

Søgning i Postgres på mange niveauer – fra simple `LIKE` til fuzzy matching og “rigtig” full text search.

### 1) `LIKE` og hvorfor det ofte er dårligt

`LIKE '%foo%'` kan være fint på små tabeller, men:

- **Indekser bruges ofte ikke** (leading wildcard)
- Det skalerer dårligt når tabellen vokser

### 2) Fuzzy matching med `pg_trgm`

`pg_trgm` (trigram) deler tekst i overlappende 3-tegns bidder og kan beregne “lighed”.

Vigtige funktioner:

- `similarity(a, b)`: tal mellem 0 og 1
- `word_similarity(a, b)`: ofte bedre til ord

Indekser:

- **GIN** med `gin_trgm_ops` giver hurtig søgning på `similarity`/trigram queries.

Use cases:

- stavefejl: “iphnoe” → “iphone”
- nær-lighed: “samsnug” → “samsung”

### 3) Full text search (FTS)

FTS er designet til dokument- og tekstsøgning med:

- tokenisering (ord)
- stopord
- stemming (bøjninger)
- ranking (relevans)

I Postgres repræsenteres dokumenter som `tsvector`, og queries som `tsquery`.

Nyttige funktioner:

- `to_tsvector('danish', text)`
- `websearch_to_tsquery('danish', query_text)` (brugervenlig syntaks)
- `ts_rank(vector, query)` (score)

### `unaccent`: gør søgning mere robust

`unaccent()` kan gøre “København” matchbar med “Kobenhavn”.

### Demo i dette repo

Init-scriptet `docker/initdb/20-search-demo.sql` viser:

- trigram-indekser og `search_products()`
- materialized view som søgeindeks (`product_search_index`)
- `fulltext_search_products()` med `websearch_to_tsquery`

**Grafer i web-appen:** Søjlediagrammer giver først mening, når der er **flere rækker** at sammenligne. Demo-queries bruger derfor fx søgetermen `elektronik` (findes i alle tags) og en FTS-query med `OR`, så du kan se forskel på similarity/rank.

Øvelser:

- Prøv at ændre threshold i `search_products()` — hvornår forsvinder rækker (og grafen)?
- Sammenlign `EXPLAIN` på søgninger med/uden trigram indeks
- Tilføj flere produkter og se hvordan rank ændrer sig
