CREATE TABLE IF NOT EXISTS cron_job_logs (
    log_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100),
    execution_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    status TEXT,
    affected_rows INTEGER,
    execution_duration INTERVAL
);

CREATE TABLE IF NOT EXISTS sales_data (
    sale_id SERIAL PRIMARY KEY,
    sale_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(10,2),
    processed BOOLEAN DEFAULT FALSE
);

INSERT INTO sales_data (sale_date, amount)
SELECT
    NOW() - (random() * interval '30 days'),
    round((random() * 1000)::numeric, 2)
FROM generate_series(1, 100)
WHERE NOT EXISTS (SELECT 1 FROM sales_data LIMIT 1);

CREATE OR REPLACE FUNCTION aggregate_daily_sales() RETURNS void AS $$
DECLARE
    start_time TIMESTAMPTZ;
    rows_affected INTEGER;
BEGIN
    start_time := CLOCK_TIMESTAMP();

    CREATE TABLE IF NOT EXISTS sales_daily_summary (
        summary_date DATE PRIMARY KEY,
        total_sales DECIMAL(12,2),
        transaction_count INTEGER,
        last_updated TIMESTAMPTZ DEFAULT NOW()
    );

    WITH daily_totals AS (
        SELECT
            DATE(sale_date) AS sale_day,
            SUM(amount) AS daily_total,
            COUNT(*) AS daily_count
        FROM sales_data
        WHERE NOT processed
        GROUP BY DATE(sale_date)
    )
    INSERT INTO sales_daily_summary (summary_date, total_sales, transaction_count)
    SELECT sale_day, daily_total, daily_count
    FROM daily_totals
    ON CONFLICT (summary_date) DO UPDATE
    SET
        total_sales = sales_daily_summary.total_sales + EXCLUDED.total_sales,
        transaction_count = sales_daily_summary.transaction_count + EXCLUDED.transaction_count,
        last_updated = NOW();

    UPDATE sales_data
    SET processed = TRUE
    WHERE NOT processed;

    GET DIAGNOSTICS rows_affected = ROW_COUNT;

    INSERT INTO cron_job_logs (job_name, status, affected_rows, execution_duration)
    VALUES ('aggregate_daily_sales', 'SUCCESS', rows_affected, CLOCK_TIMESTAMP() - start_time);
EXCEPTION WHEN OTHERS THEN
    INSERT INTO cron_job_logs (job_name, status, affected_rows, execution_duration)
    VALUES ('aggregate_daily_sales', 'ERROR: ' || SQLERRM, 0, CLOCK_TIMESTAMP() - start_time);
END;
$$ LANGUAGE plpgsql;

-- Kør job én gang nu (så klassen kan se effekten med det samme)
SELECT aggregate_daily_sales();

-- Planlæg hvert minut (så man kan se det køre uden at vente til i nat)
SELECT cron.schedule('every-minute-sales-aggregation', '* * * * *', 'SELECT aggregate_daily_sales()')
WHERE NOT EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'every-minute-sales-aggregation');

-- Hjælpevisninger
CREATE OR REPLACE VIEW v_cron_jobs AS
SELECT jobid, jobname, schedule, command, nodename, nodeport, database, active
FROM cron.job;

CREATE OR REPLACE VIEW v_cron_job_run_details AS
SELECT *
FROM cron.job_run_details;

