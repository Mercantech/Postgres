from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
import json
import re
from typing import Any, Iterable

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
-- Flere rækker: 'elektronik' findes i alle demo-tags (god til sammenligning af similarity)
SELECT * FROM search_products('elektronik', 0.12);

-- Flere rækker: 'mobil' rammer typisk flere produkter (navn/beskrivelse/tags)
SELECT * FROM search_products('mobil', 0.12);

-- Full text: bred query så flere dokumenter kan score (rank sammenlignes bedst med 2+ rækker)
SELECT * FROM fulltext_search_products('apple OR samsung OR bose');
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


def _leading_comment_block(stmt: str) -> str | None:
    """
    Returnerer en kort, menneskelig forklaring ud fra de første `--`-linjer i statementet.
    Hvis statementet starter med kommentarer, bruger vi dem som “custom forklaring”.
    """
    lines = stmt.strip().splitlines()
    out: list[str] = []
    for ln in lines:
        s = ln.strip()
        if s == "":
            if out:
                # stop ved første tom linje efter kommentar-blok
                break
            continue
        if s.startswith("--"):
            text = s[2:].strip()
            if text:
                out.append(text)
            continue
        break
    if not out:
        return None
    return "\n".join(f"- {t}" for t in out)


def _extract_name(pattern: str, s: str) -> str | None:
    m = re.search(pattern, s, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1)


def _custom_ddl_dml_explain(stmt: str, rowcount: Any) -> str:
    """
    Giver en “per-statement” forklaring ved at udlede objektnavne fra SQL’en.
    Målet er at undgå generiske forklaringer for vores undervisningsdemoer.
    """
    s = " ".join(stmt.strip().split())

    ext = _extract_name(r"create\s+extension\s+if\s+not\s+exists\s+([a-zA-Z0-9_]+)", s) or _extract_name(
        r"create\s+extension\s+([a-zA-Z0-9_]+)", s
    )
    if ext:
        return (
            f"Slår extensionen **`{ext}`** til i databasen. "
            "Det gør at nye funktioner/typer/operatører bliver tilgængelige (fx `postgis`, `pg_cron`, `timescaledb`)."
        )

    tbl = _extract_name(r"create\s+table\s+if\s+not\s+exists\s+([a-zA-Z0-9_]+)", s) or _extract_name(
        r"create\s+table\s+([a-zA-Z0-9_]+)", s
    )
    if tbl:
        return (
            f"Opretter tabellen **`{tbl}`** (hvis den ikke findes). "
            "Her defineres kolonner, datatyper og constraints (fx PRIMARY KEY/UNIQUE), som styrer datakvalitet og relationer."
        )

    idx = _extract_name(
        r"create\s+unique\s+index\s+if\s+not\s+exists\s+([a-zA-Z0-9_]+)", s
    ) or _extract_name(r"create\s+index\s+if\s+not\s+exists\s+([a-zA-Z0-9_]+)", s)
    if idx:
        return (
            f"Opretter indekset **`{idx}`** (hvis det ikke findes). "
            "Indekset gør bestemte opslag hurtige og er ofte afgørende for performance (fx PostGIS GiST/KNN eller FTS GIN)."
        )

    view = _extract_name(
        r"create\s+or\s+replace\s+view\s+([a-zA-Z0-9_]+)", s
    ) or _extract_name(r"create\s+view\s+if\s+not\s+exists\s+([a-zA-Z0-9_]+)", s)
    if view:
        return (
            f"Opretter view’et **`{view}`**. "
            "Et view er en navngiven query, som gør det nemmere at genbruge og forklare logik i undervisningen."
        )

    mview = _extract_name(
        r"create\s+materialized\s+view\s+if\s+not\s+exists\s+([a-zA-Z0-9_]+)", s
    )
    if mview:
        return (
            f"Opretter materialized view’et **`{mview}`**. "
            "Det gemmer resultater fysisk og kan gøre søgning/dashboards meget hurtige (men skal opdateres/refreshes)."
        )

    fn = _extract_name(
        r"create\s+or\s+replace\s+function\s+([a-zA-Z0-9_]+)", s
    ) or _extract_name(r"create\s+function\s+([a-zA-Z0-9_]+)", s)
    if fn:
        return (
            f"Opretter/opdaterer funktionen **`{fn}()`**. "
            "Funktioner bruges til genbrugelig logik tæt på data (fx validering, søgning eller hjælpe-API’er i SQL)."
        )

    if re.search(r"^\s*insert\s+into\s+([a-zA-Z0-9_]+)", stmt, flags=re.IGNORECASE | re.MULTILINE):
        t = _extract_name(r"insert\s+into\s+([a-zA-Z0-9_]+)", s) or "tabel"
        return (
            f"Indsætter rækker i **`{t}`**. Rowcount rapporteres som `{rowcount}` (nogle drivers viser `-1` for visse statement-typer). "
            "I demoer med `ON CONFLICT ... DO NOTHING` kan indsæt blive sprunget over hvis en række allerede findes."
        )

    if re.search(r"^\s*update\s+([a-zA-Z0-9_]+)", stmt, flags=re.IGNORECASE | re.MULTILINE):
        t = _extract_name(r"update\s+([a-zA-Z0-9_]+)", s) or "tabel"
        return f"Opdaterer rækker i **`{t}`**. Rowcount viser hvor mange rækker der blev ændret (her: `{rowcount}`)."

    if re.search(r"^\s*delete\s+from\s+([a-zA-Z0-9_]+)", stmt, flags=re.IGNORECASE | re.MULTILINE):
        t = _extract_name(r"delete\s+from\s+([a-zA-Z0-9_]+)", s) or "tabel"
        return f"Sletter rækker fra **`{t}`**. Rowcount viser hvor mange rækker der blev slettet (her: `{rowcount}`)."

    if re.search(r"^\s*alter\s+table\s+([a-zA-Z0-9_]+)", stmt, flags=re.IGNORECASE | re.MULTILINE):
        t = _extract_name(r"alter\s+table\s+([a-zA-Z0-9_]+)", s) or "tabel"
        return (
            f"Ændrer tabellen **`{t}`** (ALTER TABLE). "
            "Det bruges fx til at slå Timescale compression til eller ændre table-settings uden at genskabe data."
        )

    if "cron.schedule" in s.lower():
        return (
            "Planlægger et `pg_cron` job. Det betyder at databasen selv kører den angivne SQL på et fast schedule "
            "(godt til batch/vedligehold, men kræver styr på drift og rettigheder)."
        )

    if stmt.strip().lower().startswith("do $$"):
        return (
            "Kører en PL/pgSQL-blok (`DO $$ ... $$`). Vi bruger det i demoerne for at gøre opsætning idempotent "
            "(fx 'prøv at tilføje en policy; hvis den allerede findes, så ignorer fejlen')."
        )

    return (
        "Statementet returnerede ikke en tabel, men udførte en ændring/opsætning i databasen. "
        f"Rowcount rapporteres som `{rowcount}` (kan være `-1` afhængigt af statement-type/driver)."
    )

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

    # 1) Hvis statementet selv starter med kommentarer, bruger vi dem som “custom forklaring”
    comment_explain = _leading_comment_block(stmt)
    if comment_explain:
        return f"**Forklaring (fra demo-scriptets kommentarer):**\n{comment_explain}"

    # 2) Ellers laver vi en custom forklaring ud fra statementets konkrete objekter
    return _custom_ddl_dml_explain(stmt, result.get("rowcount"))


def render_auto_charts(df: pd.DataFrame) -> None:
    """Tegn simple grafer når resultat-kolonner matcher kendte demo-mønstre."""
    if df.empty or len(df) > 400:
        return

    lc = {str(c).lower(): c for c in df.columns}

    with st.expander("Graf (auto)", expanded=True):
        try:
            if "similarity" in lc:
                lab = lc.get("name") or lc.get("product_id") or lc.get("title")
                if lab and len(df) >= 2:
                    st.caption("Trigram-lighed (højere = tættere match)")
                    st.bar_chart(df[[lab, lc["similarity"]]].set_index(lab))
                    return
                if lab and len(df) < 2:
                    st.caption(
                        "Kun én (eller ingen) række — søjlediagram giver lidt mening. "
                        "Prøv lavere threshold eller en bredere søgeterm (fx `elektronik`, `mobil`)."
                    )
                    return

            if "rank" in lc:
                lab = lc.get("name") or lc.get("product_id") or lc.get("title")
                if lab and len(df) >= 2:
                    st.caption("Full text rank (højere = mere relevant)")
                    st.bar_chart(df[[lab, lc["rank"]]].set_index(lab))
                    return
                if lab and len(df) < 2:
                    st.caption(
                        "Kun én (eller ingen) række — prøv en bredere FTS-query (fx `apple OR samsung OR bose`)."
                    )
                    return

            if "distance" in lc and (lc.get("title") or lc.get("name")):
                lab = lc.get("title") or lc.get("name")
                if lab:
                    st.caption("Vektor-afstand (lavere = tættere på query)")
                    st.bar_chart(df[[lab, lc["distance"]]].set_index(lab))
                    return

            if "meters_to_cph" in lc and "name" in lc:
                st.caption("Afstand til København (meter)")
                ch = df[[lc["name"], lc["meters_to_cph"]]].set_index(lc["name"]).sort_values(lc["meters_to_cph"])
                st.bar_chart(ch)
                return

            if "meters" in lc and "name" in lc and "meters_to_cph" not in lc:
                st.caption("Afstand (meter)")
                ch = df[[lc["name"], lc["meters"]]].set_index(lc["name"]).sort_values(lc["meters"])
                st.bar_chart(ch)
                return

            if "day" in lc and "avg_value" in lc:
                st.caption("Gennemsnit pr. dag (alle sensorer lagt sammen hvis flere)")
                sub = df[[lc["day"], lc["avg_value"]]].copy()
                sub[lc["day"]] = pd.to_datetime(sub[lc["day"]], utc=True, errors="coerce")
                g = sub.groupby(sub[lc["day"]].dt.tz_convert(None).dt.date, as_index=True)[lc["avg_value"]].mean()
                st.line_chart(g)
                return

            if "hour" in lc and "avg_value" in lc and "day" not in lc:
                st.caption("Gennemsnit pr. time (aggregeret over sensorer)")
                sub = df[[lc["hour"], lc["avg_value"]]].copy()
                sub[lc["hour"]] = pd.to_datetime(sub[lc["hour"]], utc=True, errors="coerce")
                g = sub.groupby(sub[lc["hour"]].dt.tz_convert(None), as_index=True)[lc["avg_value"]].mean()
                st.line_chart(g)
                return

            if "bucket" in lc:
                val_c = lc.get("average_value") or lc.get("avg_value_locf") or lc.get("number_of_rows")
                if val_c:
                    st.caption("Udvikling over tid (bucket)")
                    sub = df[[lc["bucket"], val_c]].copy()
                    sub[lc["bucket"]] = pd.to_datetime(sub[lc["bucket"]], utc=True, errors="coerce")
                    sub = sub.sort_values(lc["bucket"]).set_index(lc["bucket"])[val_c]
                    st.line_chart(sub)
                    return

            if "job_name" in lc and "affected_rows" in lc and len(df) <= 50:
                st.caption("Job-log: påvirkede rækker")
                st.bar_chart(df[[lc["job_name"], lc["affected_rows"]]].set_index(lc["job_name"]))
                return

            if "sensor_id" in lc and "value" in lc and len(df) <= 200:
                st.caption("Gennemsnitlig værdi pr. sensor (udsnit)")
                st.bar_chart(df[[lc["sensor_id"], lc["value"]]].groupby(lc["sensor_id"])[lc["value"]].mean())
        except Exception:
            st.caption("Kunne ikke lave auto-graf for dette resultat (dataform eller type).")


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

with st.expander("Start her — sådan får du mest ud af demoen", expanded=True):
    st.markdown(
        """
**1. Vælg modul** i **Pensum** (sidebar).

**2. Kør demoen først** med **Kør demo for dette modul** (under *Demo-scripts*).  
Så oprettes tabeller, views og demo-data. Springer du det over, får du typisk fejl som `relation ... does not exist`.

**3. Kør SQL** i editoren. I **Resultater** får du tabel, **forklaring** og ofte en **Graf (auto)**.

**Ekstra:** Under **PostGIS**-modulet ligger der et **kort** med zoner, ruter og byer.
        """
    )

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
                render_auto_charts(r["df"])
            else:
                st.write(f"Rowcount: `{r.get('rowcount')}`")

