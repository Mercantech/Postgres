from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
import json
from typing import Any, Iterable, Literal

import pandas as pd
import psycopg
import sqlparse
import streamlit as st
import pydeck as pdk


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

SELECT * FROM v_latest_reading_per_sensor;
SELECT * FROM v_gapfill_hourly_sensor_1;
SELECT * FROM v_conditions_chunks;

SELECT * FROM conditions_daily_avg ORDER BY day DESC, sensor_id LIMIT 50;
SELECT * FROM conditions_hourly_avg ORDER BY hour DESC, sensor_id LIMIT 50;
"""

    if "postgis" in page_key:
        return """\
SELECT * FROM nearest_place(10.0, 56.0);

SELECT * FROM v_place_distances_to_cph;
SELECT * FROM v_places_within_150km_of_cph;
SELECT * FROM v_places_in_zones;
SELECT * FROM v_route_zone_intersections;

SELECT * FROM v_route_length_meters;
SELECT * FROM v_places_geojson LIMIT 5;
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


def fetch_rows(sql: str) -> list[tuple[Any, ...]]:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def _geojson_featurecollection(rows: list[tuple[Any, ...]], id_col: str) -> dict[str, Any]:
    # rows: (id, name, geojson(json or str))
    features: list[dict[str, Any]] = []
    for row in rows:
        oid, name, gj = row
        geom = gj if isinstance(gj, dict) else json.loads(gj)
        features.append(
            {
                "type": "Feature",
                "id": oid,
                "properties": {"name": name},
                "geometry": geom,
            }
        )
    return {"type": "FeatureCollection", "features": features, "properties": {"id_col": id_col}}


def postgis_layers() -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    try:
        places = fetch_rows("SELECT place_id, name, geojson FROM v_places_geojson;")
    except Exception:
        places = []
    try:
        zones = fetch_rows("SELECT zone_id, name, geojson FROM v_zones_geojson;")
    except Exception:
        zones = []
    try:
        routes = fetch_rows("SELECT route_id, name, geojson FROM v_routes_geojson;")
    except Exception:
        routes = []

    places_fc = _geojson_featurecollection(places, "place_id") if places else None
    zones_fc = _geojson_featurecollection(zones, "zone_id") if zones else None
    routes_fc = _geojson_featurecollection(routes, "route_id") if routes else None
    return places_fc, zones_fc, routes_fc


def render_postgis_map() -> None:
    st.subheader("Kort (PostGIS)")
    st.caption("Viser punkter (places), zoner (polygons) og ruter (lines) fra demo-data.")

    places_fc, zones_fc, routes_fc = postgis_layers()
    if not any([places_fc, zones_fc, routes_fc]):
        st.warning(
            "Ingen GeoJSON-layers fundet endnu. Kør PostGIS-demoen (\"Kør demo for dette modul\"), "
            "så tabeller/views bliver oprettet."
        )
        return

    layers: list[pdk.Layer] = []
    if zones_fc:
        layers.append(
            pdk.Layer(
                "GeoJsonLayer",
                zones_fc,
                stroked=True,
                filled=True,
                get_fill_color=[255, 140, 0, 50],
                get_line_color=[255, 140, 0, 180],
                line_width_min_pixels=2,
                pickable=True,
            )
        )
    if routes_fc:
        layers.append(
            pdk.Layer(
                "GeoJsonLayer",
                routes_fc,
                stroked=True,
                filled=False,
                get_line_color=[0, 120, 255, 220],
                line_width_min_pixels=4,
                pickable=True,
            )
        )
    if places_fc:
        layers.append(
            pdk.Layer(
                "GeoJsonLayer",
                places_fc,
                stroked=True,
                filled=True,
                get_fill_color=[0, 200, 140, 180],
                get_line_color=[0, 120, 90, 220],
                point_radius_min_pixels=6,
                pickable=True,
            )
        )

    view_state = pdk.ViewState(latitude=56.2, longitude=10.2, zoom=5.5)
    deck = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        tooltip={"text": "{name}"},
        map_style="mapbox://styles/mapbox/light-v10",
    )
    st.pydeck_chart(deck, use_container_width=True)


def explain_statement(stmt: str, result: dict[str, Any]) -> str:
    s = " ".join(stmt.strip().split()).lower()
    ok = result.get("ok") is True
    if not ok:
        err = (result.get("error") or "").lower()
        if "does not exist" in err and ("relation" in err or "view" in err or "table" in err):
            return (
                "Det her fejler fordi objektet (fx view/table) **ikke findes endnu**. "
                "I vores undervisnings-setup bliver mange demo-objekter oprettet af demo-scriptet.\n\n"
                "- Løsning i web-appen: vælg den relevante demo i **Demo-scripts** og tryk **Kør valgt demo**\n"
                "- Alternativt (hvis du forventer at init kører): kør `docker compose down -v` og start igen, "
                "så init-scripts kører på en frisk database."
            )
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

    if "from conditions_hourly_avg" in s:
        return (
            "Continuous aggregate på time-niveau. Den er god til dashboards, fordi den gemmer "
            "min/max/gennemsnit og antal pr. time og sensor."
        )

    if "from v_latest_reading_per_sensor" in s or "distinct on (sensor_id)" in s:
        return (
            "Viser **seneste måling pr. sensor** (typisk 'hvad er status lige nu?'). "
            "Patternet bruger sortering på `(sensor_id, time DESC)` og kan optimeres med et matchende indeks."
        )

    if "from v_gapfill_hourly_sensor_1" in s or "time_bucket_gapfill" in s:
        return (
            "Gapfill laver et **kontinuerligt tids-akse** (her pr. time) og udfylder manglende bucket’s. "
            "`locf()` betyder 'last observation carried forward' – sidste kendte værdi bæres frem."
        )

    if "from v_conditions_chunks" in s or "timescaledb_information.chunks" in s:
        return (
            "Viser hvilke **chunks** hypertablen består af (range_start/range_end). "
            "Det er den konkrete mekanik bag performance: queries kan springe hele chunks over (chunk pruning)."
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

    # DDL/DML: forklar statement-typen mere konkret
    rowcount = result.get("rowcount")

    if s.startswith("create extension"):
        return (
            "Aktiverer en extension i databasen (gør nye funktioner/typer/indekser tilgængelige). "
            "Hvis den allerede er aktiveret, sker der typisk ikke noget (ved `IF NOT EXISTS`)."
        )

    if s.startswith("create table"):
        return (
            "Opretter en ny tabel (skema/struktur). "
            "Det her definerer kolonner, datatyper og constraints (fx PRIMARY KEY/UNIQUE)."
        )

    if s.startswith("create index") or s.startswith("create unique index"):
        return (
            "Opretter et indeks for at gøre bestemte opslag hurtige. "
            "I PostGIS/FTS/trigram bruges indekser ofte for at gøre søgning/nærmeste-nabo effektiv."
        )

    if s.startswith("create materialized view"):
        return (
            "Opretter en materialized view: et gemt resultat af en query. "
            "Det kan gøre gentagne forespørgsler meget hurtigere, men skal opdateres (refreshes)."
        )

    if s.startswith("create view") or s.startswith("create or replace view"):
        return (
            "Opretter et view: en gemt query der gør komplekse joins/udregninger nemmere at genbruge og forklare."
        )

    if "create or replace function" in s or s.startswith("create function"):
        return (
            "Opretter/opdaterer en database-funktion (genbrugelig logik tæt på data). "
            "I demoerne bruger vi det til fx login-verifikation eller hjælpe-queries."
        )

    if s.startswith("insert into"):
        return (
            f"Indsætter data i en tabel. Rowcount viser hvor mange rækker der blev indsat (her: `{rowcount}`). "
            "Hvis der bruges `ON CONFLICT ... DO NOTHING`, kan nogle rækker blive sprunget over."
        )

    if s.startswith("update "):
        return (
            f"Opdaterer eksisterende rækker. Rowcount viser hvor mange rækker der blev ændret (her: `{rowcount}`)."
        )

    if s.startswith("delete from"):
        return (
            f"Sletter rækker. Rowcount viser hvor mange rækker der blev slettet (her: `{rowcount}`)."
        )

    if s.startswith("alter table"):
        return "Ændrer en eksisterende tabel (fx add/drop kolonne, ændre settings, slå compression til osv.)."

    if "cron.schedule" in s:
        return (
            "Planlægger et `pg_cron` job. Det betyder at databasen selv kører den angivne SQL på et fast schedule."
        )

    if s.startswith("do $$"):
        return (
            "Kører en lille PL/pgSQL-blok (server-side script). "
            "Vi bruger det typisk for at gøre opsætning idempotent med `EXCEPTION WHEN OTHERS THEN NULL`."
        )

    return (
        "Statementet returnerede ikke en tabel. "
        "Se `Rowcount` for hvor mange rækker der blev påvirket (nogle kommandoer viser `-1`, hvilket blot betyder "
        "at driveren ikke rapporterer et præcist antal for den type statement)."
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


def demo_key_for_page(page_key: str) -> str | None:
    if "pgcrypto" in page_key:
        return "pgcrypto"
    if "soegning" in page_key or "search" in page_key:
        return "search"
    if "pg-cron" in page_key or "cron" in page_key:
        return "pg_cron"
    if "timescaledb" in page_key:
        return "timescaledb"
    if "postgis" in page_key:
        return "postgis"
    if "pgvector" in page_key or "vector" in page_key:
        return "pgvector"
    return None


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
        demo_by_key = {s.key: s for s in scripts}

        page_obj: SyllabusPage | None = st.session_state.get("picked_page_obj")
        auto_key = demo_key_for_page(page_obj.key) if page_obj is not None else None
        if auto_key is not None and auto_key in demo_by_key:
            if st.button("Kør demo for dette modul"):
                st.session_state.auto_run_demo_key = auto_key
                st.rerun()

        run_demo = st.button("Kør valgt demo")

col_left, col_right = st.columns([1.25, 1], gap="large")

with col_left:
    if picked_page is not None:
        st.subheader("Pensum")
        st.markdown(read_text(picked_page.path))

        if "postgis" in picked_page.key:
            with st.expander("Vis kort", expanded=True):
                render_postgis_map()

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

    if scripts and st.session_state.get("auto_run_demo_key"):
        key = st.session_state.get("auto_run_demo_key")
        match = next((s for s in scripts if s.key == key), None)
        if match is not None:
            script_text = read_text(match.path)
            stmts = split_sql(script_text)
            st.write(f"Kører **{len(stmts)}** statements fra `{match.path.name}` (auto for modul) …")
            results = run_statements(stmts)
        st.session_state.auto_run_demo_key = None

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

