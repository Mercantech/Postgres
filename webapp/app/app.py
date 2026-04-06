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


def split_sql(script: str) -> list[str]:
    parts = [p.strip() for p in sqlparse.split(script)]
    return [p for p in parts if p and not p.isspace()]


def _fetch_df(cur: psycopg.Cursor) -> pd.DataFrame:
    rows = cur.fetchall()
    cols = [d.name for d in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


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
st.caption("Kør SQL live og tryk på demo-knapper for udvidelser.")

with st.sidebar:
    st.subheader("Forbindelse")
    st.code(_conninfo(), language="text")
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

col_left, col_right = st.columns([1, 1], gap="large")

with col_left:
    st.subheader("SQL editor")
    default_sql = "SELECT now() AS time, version() AS postgres_version;"
    sql_text = st.text_area("Skriv SQL (flere statements er OK).", value=default_sql, height=260)
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
                break

            st.success(f"Statement {i} OK ({r['ms']} ms)")
            with st.expander("Vis SQL", expanded=False):
                st.code(r["sql"], language="sql")

            if "df" in r:
                st.dataframe(r["df"], use_container_width=True)
            else:
                st.write(f"Rowcount: `{r.get('rowcount')}`")

