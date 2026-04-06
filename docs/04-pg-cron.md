## `pg_cron`: jobs/scheduling inde i Postgres

`pg_cron` gør det muligt at planlægge SQL-kommandoer som cron-jobs direkte i databasen.

### Hvad er ideen?

Mange opgaver er “batch”:

- aggregere data til rapportering
- rydde gamle logs
- sende reminders/udløse processer
- vedligeholde materialized views

Normalt ligger dette i en app eller en ekstern scheduler, men `pg_cron` kan flytte det tættere på data.

### Krav og faldgruber

- `pg_cron` kræver `shared_preload_libraries=pg_cron` (ellers kan extension ikke bruges).
- Man skal sætte `cron.database_name`, så cron ved hvilken DB den skal køre i.
- Jobs kører som SQL-kommandoer, så **rettigheder** og **rolleadgang** er kritisk.

### Stabil praksis

- Jobbet bør være **idempotent** (tåle at køre igen).
- Log altid: starttid, status, varighed, antal rækker.
- Hold jobs små og forudsigelige – lange jobs kan spænde ben for drift.

### Cron-syntaks

Standard 5-felts cron:

- minut time dag måned ugedag

Eksempler:

- `0 2 * * *` = hver dag 02:00
- `*/5 * * * *` = hvert 5. minut

### Demo i dette repo

Init-scriptet `docker/initdb/30-pg-cron-demo.sql`:

- opretter `sales_data`, `sales_daily_summary` og `cron_job_logs`
- laver funktionen `aggregate_daily_sales()`
- kører den én gang (så I ser output med det samme)
- planlægger den derefter hvert minut

SQL til at kigge på jobs:

```sql
SELECT * FROM v_cron_jobs;
SELECT * FROM v_cron_job_run_details ORDER BY start_time DESC LIMIT 20;
SELECT * FROM cron_job_logs ORDER BY execution_time DESC LIMIT 20;
```

Øvelser til elever:

- Skift schedule til hvert 5. minut og forklar forskellen.
- Tilføj en cleanup-funktion til `cron_job_logs`.
- Diskutér: hvornår er ekstern scheduler bedre end `pg_cron`?

