-- @block Først installerer vi pg_cron extension
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- @block Opret en log tabel til at spore vores cron jobs
CREATE TABLE cron_job_logs (
    log_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100),
    execution_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    status TEXT,
    affected_rows INTEGER,
    execution_duration INTERVAL
);

-- @block Opret en tabel til salgsdata som eksempel
CREATE TABLE sales_data (
    sale_id SERIAL PRIMARY KEY,
    sale_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(10,2),
    processed BOOLEAN DEFAULT FALSE
);

-- @block Indsæt noget test data
INSERT INTO sales_data (sale_date, amount) 
SELECT 
    NOW() - (random() * interval '30 days'),
    random() * 1000
FROM generate_series(1, 100);

-- @block Funktion til at aggregere daglig salgsdata
CREATE OR REPLACE FUNCTION aggregate_daily_sales() RETURNS void AS $$
DECLARE
    start_time TIMESTAMPTZ;
    rows_affected INTEGER;
BEGIN
    start_time := CLOCK_TIMESTAMP();
    
    -- Opret tabel hvis den ikke eksisterer
    CREATE TABLE IF NOT EXISTS sales_daily_summary (
        summary_date DATE PRIMARY KEY,
        total_sales DECIMAL(12,2),
        transaction_count INTEGER,
        last_updated TIMESTAMPTZ DEFAULT NOW()
    );
    
    -- Indsæt eller opdater daglig sammenfatning
    WITH daily_totals AS (
        SELECT 
            DATE(sale_date) as sale_day,
            SUM(amount) as daily_total,
            COUNT(*) as daily_count
        FROM sales_data
        WHERE NOT processed
        GROUP BY DATE(sale_date)
    )
    INSERT INTO sales_daily_summary (summary_date, total_sales, transaction_count)
    SELECT 
        sale_day,
        daily_total,
        daily_count
    FROM daily_totals
    ON CONFLICT (summary_date) DO UPDATE
    SET 
        total_sales = sales_daily_summary.total_sales + EXCLUDED.total_sales,
        transaction_count = sales_daily_summary.transaction_count + EXCLUDED.transaction_count,
        last_updated = NOW();

    -- Marker data som behandlet
    UPDATE sales_data 
    SET processed = TRUE 
    WHERE NOT processed;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    
    -- Log resultatet
    INSERT INTO cron_job_logs (job_name, status, affected_rows, execution_duration)
    VALUES (
        'aggregate_daily_sales',
        'SUCCESS',
        rows_affected,
        CLOCK_TIMESTAMP() - start_time
    );
EXCEPTION WHEN OTHERS THEN
    -- Log fejl hvis noget går galt
    INSERT INTO cron_job_logs (job_name, status, affected_rows, execution_duration)
    VALUES (
        'aggregate_daily_sales',
        'ERROR: ' || SQLERRM,
        0,
        CLOCK_TIMESTAMP() - start_time
    );
END;
$$ LANGUAGE plpgsql;

-- @block Funktion til at rense gamle logs
CREATE OR REPLACE FUNCTION cleanup_old_logs() RETURNS void AS $$
DECLARE
    start_time TIMESTAMPTZ;
    rows_affected INTEGER;
BEGIN
    start_time := CLOCK_TIMESTAMP();
    
    DELETE FROM cron_job_logs 
    WHERE execution_time < NOW() - INTERVAL '30 days';
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    
    -- Log cleanup aktiviteten
    INSERT INTO cron_job_logs (job_name, status, affected_rows, execution_duration)
    VALUES (
        'cleanup_old_logs',
        'SUCCESS',
        rows_affected,
        CLOCK_TIMESTAMP() - start_time
    );
END;
$$ LANGUAGE plpgsql;

-- @block Planlæg cron jobs
-- Kør daglig aggregering hver nat kl. 02:00
SELECT cron.schedule('daily-sales-aggregation', '0 2 * * *', 'SELECT aggregate_daily_sales()');

-- @block kør hver 10 sekunder
-- SELECT cron.schedule('every-10-seconds', '*/10 * * * * *', 'SELECT aggregate_daily_sales()');

-- Kør log cleanup hver søndag kl. 03:00
SELECT cron.schedule('weekly-log-cleanup', '0 3 * * 0', 'SELECT cleanup_old_logs()');

-- Opret en funktion til at vise aktive cron jobs
CREATE OR REPLACE FUNCTION show_cron_jobs() RETURNS TABLE (
    job_id INTEGER,
    job_name TEXT,
    schedule TEXT,
    last_run TIMESTAMPTZ,
    next_run TIMESTAMPTZ,
    last_success BOOLEAN
) AS $$
    SELECT 
        jobid,
        jobname,
        schedule,
        last_run,
        next_run,
        last_success
    FROM cron.job;
$$ LANGUAGE SQL;

-- @block Hjælpefunktion til at se job status og logs
CREATE OR REPLACE FUNCTION check_job_status(
    p_job_name VARCHAR,
    p_hours INTEGER DEFAULT 24
) RETURNS TABLE (
    job_name VARCHAR,
    last_execution TIMESTAMPTZ,
    status TEXT,
    affected_rows INTEGER,
    duration INTERVAL
) AS $$
    SELECT 
        job_name,
        execution_time,
        status,
        affected_rows,
        execution_duration
    FROM cron_job_logs
    WHERE 
        job_name = p_job_name
        AND execution_time > NOW() - (p_hours || ' hours')::INTERVAL
    ORDER BY execution_time DESC;
$$ LANGUAGE SQL;

-- @block Eksempel på brug og test queries
-- Vis alle planlagte jobs
SELECT * FROM show_cron_jobs();

-- Tjek status for et specifikt job de sidste 24 timer
SELECT * FROM check_job_status('aggregate_daily_sales');

-- Se den daglige salgssammenfatning
SELECT * FROM sales_daily_summary ORDER BY summary_date DESC;

-- Se de seneste log entries
SELECT * FROM cron_job_logs ORDER BY execution_time DESC LIMIT 5;

/*
DOKUMENTATION:

Dette script demonstrerer brugen af pg_cron til at automatisere forskellige database opgaver.

Hovedfunktioner:
1. aggregate_daily_sales()
   - Kører hver nat kl. 02:00
   - Aggregerer daglige salgsdata
   - Markerer behandlede rækker
   - Logger resultatet

2. cleanup_old_logs()
   - Kører hver søndag kl. 03:00
   - Fjerner logs ældre end 30 dage
   - Logger cleanup aktiviteten

Hjælpefunktioner:
- show_cron_jobs(): Viser alle aktive cron jobs
- check_job_status(): Viser status for specifikke jobs

Tabeller:
- cron_job_logs: Gemmer execution logs
- sales_data: Eksempel salgstabel
- sales_daily_summary: Aggregeret salgsdata

For at administrere jobs:
- Tilføj nyt job: SELECT cron.schedule('job-name', 'cron-schedule', 'SQL-command');
- Slet job: SELECT cron.unschedule('job-name');
- Se jobs: SELECT * FROM show_cron_jobs();

Cron tidsformat:
* * * * * = minut time dag måned ugedag
Eksempel: '0 2 * * *' = Hver dag kl. 02:00

Fejlhåndtering:
- Alle jobs logges i cron_job_logs
- Fejl fanges og logges med fejlbeskrivelse
- Status kan tjekkes med check_job_status()
*/
