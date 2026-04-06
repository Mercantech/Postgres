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

### Continuous aggregates

I stedet for at regne samme aggregering igen og igen kan TimescaleDB vedligeholde et materialiseret resultat løbende.

Fordele:

- dashboards bliver hurtigere
- mindre CPU ved gentagne forespørgsler

### Demo i dette repo

Init-scriptet `docker/initdb/40-timescaledb-demo.sql`:

- opretter `conditions`
- gør den til hypertable
- genererer data
- laver view `v_conditions_weekly`
- laver continuous aggregate `conditions_daily_avg`

Prøv:

```sql
SELECT * FROM v_conditions_weekly;
SELECT * FROM conditions_daily_avg ORDER BY day DESC, sensor_id;
```

Øvelser:

- Lav en retention policy (diskutér drift/krav)
- Sammenlign en query på hypertable vs normal tabel (EXPLAIN)

