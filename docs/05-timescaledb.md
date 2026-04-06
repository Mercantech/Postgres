## TimescaleDB: tidsserier på steroider

TimescaleDB er en udvidelse der optimerer Postgres til tidsserie-data (IoT, metrics, logs).

### Kerneidé: hypertables

En **hypertable** er en logisk tabel, men fysisk opdelt i mange “chunks” over tid (og evt. space).

Fordele:

- hurtigere queries over tid (chunk pruning)
- nemmere retention (drop chunks)
- bedre write performance ved store mængder

### `time_bucket()`

`time_bucket(interval, time)` gør “group by tid” nemt og hurtigt:

- `time_bucket('1 hour', time)`
- `time_bucket('1 day', time)`

### Hvorfor hypertables er hurtige

TimescaleDB kan lave **chunk pruning**: hvis du spørger på et tid-interval, kan den undlade at læse chunks der ligger udenfor intervallet.

Det giver typisk:

- mindre IO
- færre indekssider der skal læses
- bedre cache-hit rate

### “Latest value” og vinduer (klassisk monitoring)

Tidsserie-arbejde handler ofte om:

- seneste måling pr. device/sensor
- rullende gennemsnit (moving averages)
- alarmer baseret på vinduer

Det kan løses i ren SQL med indekser der matcher access patterns (fx `(sensor_id, time DESC)`).

### Gapfilling (til grafer)

Når data mangler (huller), bliver grafer “takkede”. TimescaleDB kan gapfille i queries:

- `time_bucket_gapfill(...)`
- `locf(...)` (last observation carried forward)

### Continuous aggregates

I stedet for at regne samme aggregering igen og igen kan TimescaleDB vedligeholde et materialiseret resultat løbende.

Fordele:

- dashboards bliver hurtigere
- mindre CPU ved gentagne forespørgsler

### Policies: retention og compression (drift!)

TimescaleDB kan automatisere to meget vigtige drifts-ting:

- **Retention**: drop gamle chunks (fx “slet data ældre end 180 dage”)
- **Compression**: komprimér ældre chunks (spar plads og gør scanninger hurtigere)

Det gør, at man kan have store datasæt uden at det vokser ukontrolleret.

### Demo i dette repo

Init-scriptet `docker/initdb/40-timescaledb-demo.sql`:

- opretter `conditions`
- gør den til hypertable
- genererer data
- laver views til “latest”, moving average og gapfill
- laver chunk-inspektion (`v_conditions_chunks`)
- laver continuous aggregates (`conditions_daily_avg`, `conditions_hourly_avg`)
- forsøger at tilføje retention/compression/refresh policies (ignorerer hvis de allerede findes)

Prøv:

```sql
SELECT * FROM v_conditions_weekly;
SELECT * FROM v_latest_reading_per_sensor;
SELECT * FROM v_gapfill_hourly_sensor_1;

SELECT * FROM conditions_daily_avg ORDER BY day DESC, sensor_id;
SELECT * FROM conditions_hourly_avg ORDER BY hour DESC, sensor_id LIMIT 100;

SELECT * FROM v_conditions_chunks;
```

Øvelser:

- Lav en retention policy (diskutér drift/krav)
- Sammenlign en query på hypertable vs normal tabel (EXPLAIN)
- Skift gapfill-vinduet og forklar `locf()`
- Diskutér: hvornår er compression en fordel/ulempe?

