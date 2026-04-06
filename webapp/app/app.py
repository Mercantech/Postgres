from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import psycopg
import sqlparse
import streamlit as st


@dataclass(frozen=True)
class DemoScript:
    key: str
    title: str
    path: Path


@dataclass(frozen=True)
class SyllabusPage:
    key: str
    title: str
    path: Path


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def _conninfo() -> str:
    host = _env("PGHOST", "db")
    port = _env("PGPORT", "5432")
    db = _env("PGDATABASE", "demo")
    user = _env("PGUSER", "postgres")
    pw = _env("PGPASSWORD", "postgres")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def _conninfo_safe() -> str:
    host = _env("PGHOST", "db")
    port = _env("PGPORT", "5432")
    db = _env("PGDATABASE", "demo")
    user = _env("PGUSER", "postgres")
    return f"postgresql://{user}:****@{host}:{port}/{db}"


@st.cache_resource
def get_conn() -> psycopg.Connection:
    return psycopg.connect(_conninfo(), autocommit=True)


def list_demo_scripts() -> list[DemoScript]:
    base = Path(_env("DEMO_SQL_DIR", "/demo-sql"))
    scripts: list[DemoScript] = [
        DemoScript("pgcrypto", "pgcrypto (hashing + kryptering)", base / "10-pgcrypto-demo.sql"),
        DemoScript("search", "Søgning (pg_trgm + full text)", base / "20-search-demo.sql"),
        DemoScript("pg_cron", "pg_cron (jobs/scheduling)", base / "30-pg-cron-demo.sql"),
        DemoScript("timescaledb", "TimescaleDB (hypertables + aggregates)", base / "40-timescaledb-demo.sql"),
        DemoScript("postgis", "PostGIS (geodata + nearest)", base / "50-postgis-demo.sql"),
        DemoScript("pgvector", "pgvector (vektor-søgning)", base / "60-pgvector-demo.sql"),
    ]
    return [s for s in scripts if s.path.exists()]


def list_syllabus_pages() -> list[SyllabusPage]:
    base = Path(_env("SYLLABUS_DIR", "/syllabus"))
    if not base.exists():
        return []

    pages: list[SyllabusPage] = []
    for p in sorted(base.glob("*.md")):
        key = p.stem
        title = p.stem.replace("-", " ").replace("_", " ")
        pages.append(SyllabusPage(key=key, title=title, path=p))
    return pages


def suggested_sql_for_page(page_key: str) -> str:
    # Kort, “kør-nu” SQL som matcher objekterne fra init-scripts.
    if page_key.startswith("00") or page_key.startswith("01"):
        return """\
SELECT now() AS time, version() AS postgres_version;

SELECT extname, extversion
FROM pg_extension
ORDER BY extname;
"""

    if "pgcrypto" in page_key:
        return """\
SELECT verify_user('alice', 'password123') AS login_success;
SELECT verify_user('alice', 'wrongPassword') AS login_failure;

SELECT user_id, username, email
FROM users
ORDER BY user_id;
"""

    if "soegning" in page_key or "search" in page_key:
        return """\
SELECT * FROM search_products('iphone', 0.30);
SELECT * FROM search_products('telefon', 0.25);

SELECT * FROM fulltext_search_products('apple mobil');
"""

    if "pg-cron" in page_key or "cron" in page_key:
        return """\
SELECT * FROM v_cron_jobs ORDER BY jobid;
SELECT * FROM v_cron_job_run_details ORDER BY start_time DESC LIMIT 20;
SELECT * FROM cron_job_logs ORDER BY execution_time DESC LIMIT 20;
"""

    if "timescaledb" in page_key:
        return """\
SELECT count(*) AS total_rows FROM conditions;
SELECT * FROM v_conditions_weekly;

SELECT *
FROM conditions_daily_avg
ORDER BY day DESC, sensor_id
LIMIT 50;
"""

    if "postgis" in page_key:
        return """\
SELECT * FROM nearest_place(10.0, 56.0);

SELECT name
FROM places
ORDER BY geom <-> ST_MakePoint(12.5683, 55.6761)::geography
LIMIT 3;
"""

    if "pgvector" in page_key or "vector" in page_key:
        return """\
SELECT title, embedding <-> '[0.85, 0.1, 0.1]' AS distance
FROM documents
ORDER BY embedding <-> '[0.85, 0.1, 0.1]'
LIMIT 3;
"""

    return "SELECT now() AS time, version() AS postgres_version;"


def _sync_sql_editor_with_page() -> None:
    picked: SyllabusPage | None = st.session_state.get("picked_page_obj")
    if picked is None:
        return
    st.session_state.sql_text = suggested_sql_for_page(picked.key)


def split_sql(script: str) -> list[str]:
    parts = [p.strip() for p in sqlparse.split(script)]
    return [p for p in parts if p and not p.isspace()]


def _fetch_df(cur: psycopg.Cursor) -> pd.DataFrame:
    rows = cur.fetchall()
    cols = [d.name for d in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def explain_statement(stmt: str, result: dict[str, Any]) -> str:
    s = " ".join(stmt.strip().split()).lower()
    ok = result.get("ok") is True
    if not ok:
        return (
            "Dette statement fejlede, så vi stoppede resten af kørslen. "
            "Læs fejlteksten og sammenlign med SQL’en."
        )

    if "df" in result:
        df: pd.DataFrame = result["df"]
        if df.empty:
            return (
                "Statementet kørte OK, men returnerede **0 rækker**. "
                "Det betyder typisk, at din WHERE/query ikke matchede noget (eller at data ikke findes endnu)."
            )

    # Kendte “pensum” queries
    if "select now()" in s and "version()" in s:
        return (
            "Viser at forbindelsen virker, samt hvilken Postgres-version containeren kører. "
            "Det er en god sanity-check før man går i gang."
        )

    if "from pg_extension" in s:
        return (
            "Lister alle installerede extensions i den aktuelle database. "
            "Det bruges til at bekræfte at fx `pgcrypto`, `pg_cron`, `timescaledb`, `postgis` og `vector` er slået til."
        )

    if "verify_user" in s:
        return (
            "`verify_user()` returnerer **true/false** afhængigt af om password-hash matcher. "
            "Bemærk: vi sammenligner ved at hashe input med salt/parametre fra det lagrede hash."
        )

    if "from users" in s:
        return (
            "Viser rækker fra `users`. Passwords er **hashet** (kan ikke dekrypteres). "
            "Følsomme data er **krypteret** (kan dekrypteres med korrekt nøgle)."
        )

    if "search_products" in s:
        return (
            "Fuzzy søgning med `pg_trgm`. Kolonnen `similarity` er en score \(0..1\) hvor højere = mere lignende. "
            "GIN trigram-indekser gør det hurtigt på større datasæt."
        )

    if "fulltext_search_products" in s or "websearch_to_tsquery" in s:
        return (
            "Full text search (dansk). `rank` er relevansscoren: højere = bedre match. "
            "Her bruges et tsvector-indeks (GIN) til at gøre søgningen effektiv."
        )

    if "from v_cron_jobs" in s or "from cron.job" in s:
        return (
            "Viser planlagte `pg_cron` jobs. Her kan I se schedule og om job er aktivt. "
            "Pointen: jobs kører inde i databasen – smart, men kræver styr på rettigheder og drift."
        )

    if "from v_cron_job_run_details" in s or "from cron.job_run_details" in s:
        return (
            "Viser historik for job-kørsler (start/slut, status, evt. fejl). "
            "Brug det til at forklare “observability”: man skal kunne se om automatisering virker."
        )

    if "from cron_job_logs" in s:
        return (
            "Dette er vores **egen** log-tabel (ikke pg_cron’s). "
            "Den viser *hvad* vi gjorde (fx aggregering), om det lykkedes, og hvor lang tid det tog."
        )

    if "from v_conditions_weekly" in s or "time_bucket" in s:
        return (
            "TimescaleDB `time_bucket()` grupperer tidsserier i faste intervaller (her 7 dage). "
            "Det er en standard måde at lave dashboards og aggregeringer på tidsdata."
        )

    if "from conditions_daily_avg" in s:
        return (
            "Dette er en **continuous aggregate**. TimescaleDB vedligeholder aggregerede data løbende, "
            "så gentagne queries bliver hurtige."
        )

    if "nearest_place" in s or "st_distance" in s or "<-> st_makepoint" in s:
        return (
            "PostGIS “nærmeste-nabo” / afstand. Resultatet viser hvilken by der er tættest på koordinatet, "
            "og `meters` er distancen i meter (geography-type)."
        )

    if "embedding <->" in s or "from documents" in s:
        return (
            "`pgvector` nearest-neighbor. `distance` er afstanden mellem query-vektor og lagrede embeddings "
            "(lavere = tættere = mere lignende)."
        )

    # Generisk forklaring
    if "df" in result:
        df = result["df"]
        return (
            f"Returnerede **{len(df)} rækker**. Kig på kolonnenavne og værdier for at forstå, hvad query’en matcher. "
            "Hvis det er langsomt på store tabeller, så brug `EXPLAIN (ANALYZE, BUFFERS)` for at se planen."
        )

    return (
        "Statementet returnerede ikke en tabel (typisk DDL/DML som CREATE/INSERT/UPDATE). "
        "Se `Rowcount` for hvor mange rækker der blev påvirket."
    )


def run_statements(stmts: Iterable[str]) -> list[dict[str, Any]]:
    conn = get_conn()
    out: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        for stmt in stmts:
            started = time.time()
            try:
                cur.execute(stmt)
                elapsed_ms = int((time.time() - started) * 1000)

                if cur.description:
                    df = _fetch_df(cur)
                    out.append({"ok": True, "sql": stmt, "ms": elapsed_ms, "df": df})
                else:
                    out.append({"ok": True, "sql": stmt, "ms": elapsed_ms, "rowcount": cur.rowcount})
            except Exception as e:  # noqa: BLE001 (undervisningsværktøj, vis fejl direkte)
                out.append({"ok": False, "sql": stmt, "error": str(e)})
                break
    return out


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


st.set_page_config(page_title="Postgres Extensions Demo", layout="wide")

st.title("Postgres Extensions Demo")
st.caption("Pensum + visuelle demoer: kør SQL live og se resultater med det samme.")

with st.sidebar:
    st.subheader("Pensum")
    pages = list_syllabus_pages()
    if not pages:
        st.warning("Fandt ingen pensum-filer. Tjek at /syllabus er mounted.")
        picked_page = None
    else:
        if "picked_page_obj" not in st.session_state:
            st.session_state.picked_page_obj = pages[0]
        picked_page = st.selectbox(
            "Vælg modul",
            pages,
            format_func=lambda p: p.title,
            key="picked_page_obj",
            on_change=_sync_sql_editor_with_page,
        )

    st.divider()
    st.subheader("Forbindelse")
    st.code(_conninfo_safe())
    if st.button("Reconnect"):
        get_conn.clear()  # type: ignore[attr-defined]
        st.rerun()

    st.subheader("Demo-scripts")
    scripts = list_demo_scripts()
    if not scripts:
        st.warning("Fandt ingen demo SQL-filer. Tjek at /demo-sql er mounted.")
    else:
        picked = st.selectbox("Vælg demo", scripts, format_func=lambda s: s.title)
        run_demo = st.button("Kør valgt demo")

col_left, col_right = st.columns([1.25, 1], gap="large")

with col_left:
    if picked_page is not None:
        st.subheader("Pensum")
        st.markdown(read_text(picked_page.path))

    st.subheader("SQL editor")
    if "sql_text" not in st.session_state:
        st.session_state.sql_text = (
            suggested_sql_for_page(picked_page.key)
            if picked_page is not None
            else "SELECT now() AS time, version() AS postgres_version;"
        )

    sql_text = st.text_area(
        "Skriv SQL (flere statements er OK).",
        key="sql_text",
        height=260,
    )

    show_preview = st.checkbox("Vis SQL med syntax highlighting", value=True)
    if show_preview:
        st.code(sql_text, language="sql")
    col_a, col_b = st.columns([1, 1])
    with col_a:
        run_sql = st.button("Kør SQL", type="primary")
    with col_b:
        show_split = st.checkbox("Vis statement-split", value=False)

    if show_split:
        st.code("\n\n---\n\n".join(split_sql(sql_text)), language="sql")

with col_right:
    st.subheader("Resultater")

    results: list[dict[str, Any]] | None = None

    if "run_demo" not in st.session_state:
        st.session_state.run_demo = False

    if scripts and "picked_key" not in st.session_state:
        st.session_state.picked_key = scripts[0].key

    if scripts and "picked_key" in st.session_state:
        # sync selectbox by key (Streamlit gemmer objektet, så vi holder en key også)
        st.session_state.picked_key = picked.key  # type: ignore[has-type]

    if scripts and run_demo:
        script_text = read_text(picked.path)
        stmts = split_sql(script_text)
        st.write(f"Kører **{len(stmts)}** statements fra `{picked.path.name}` …")
        results = run_statements(stmts)

    if run_sql:
        stmts = split_sql(sql_text)
        st.write(f"Kører **{len(stmts)}** statements …")
        results = run_statements(stmts)

    if results:
        for i, r in enumerate(results, start=1):
            if r["ok"] is False:
                st.error(f"Fejl ved statement {i}: {r['error']}")
                st.code(r["sql"], language="sql")
                with st.expander("Forklaring", expanded=True):
                    st.markdown(explain_statement(r["sql"], r))
                break

            st.success(f"Statement {i} OK ({r['ms']} ms)")
            with st.expander("Vis SQL", expanded=False):
                st.code(r["sql"], language="sql")

            with st.expander("Forklaring", expanded=True):
                st.markdown(explain_statement(r["sql"], r))

            if "df" in r:
                st.dataframe(r["df"], use_container_width=True)
            else:
                st.write(f"Rowcount: `{r.get('rowcount')}`")

