"""Accès PostgreSQL : chargements idempotents, journal d'exécution, disjoncteur."""
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values

from . import config

log = logging.getLogger(__name__)


@contextmanager
def connect(dsn: str | None = None):
    conn = psycopg2.connect(dsn or config.PG_DSN)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# UPSERT : « insère, et si la ligne existe déjà, mets-la à jour ».
# La condition WHERE évite d'écraser une donnée par une version plus ancienne
# (cas d'un rejeu d'historique arrivant après une donnée plus fraîche).
# ---------------------------------------------------------------------------
UPSERT_SQL = {
    "generation": """
        INSERT INTO staging.generation (ba, period_utc, fuel, mw, source, event_id, ingested_at)
        VALUES %s
        ON CONFLICT (ba, period_utc, fuel) DO UPDATE SET
            mw = EXCLUDED.mw, source = EXCLUDED.source, event_id = EXCLUDED.event_id,
            ingested_at = EXCLUDED.ingested_at, loaded_at = now()
        WHERE staging.generation.ingested_at <= EXCLUDED.ingested_at
    """,
    "demand": """
        INSERT INTO staging.demand (ba, period_utc, demand_mw, forecast_mw, net_generation_mw,
                                    interchange_mw, source, event_id, ingested_at)
        VALUES %s
        ON CONFLICT (ba, period_utc) DO UPDATE SET
            demand_mw = EXCLUDED.demand_mw, forecast_mw = EXCLUDED.forecast_mw,
            net_generation_mw = EXCLUDED.net_generation_mw,
            interchange_mw = EXCLUDED.interchange_mw, source = EXCLUDED.source,
            event_id = EXCLUDED.event_id, ingested_at = EXCLUDED.ingested_at, loaded_at = now()
        WHERE staging.demand.ingested_at <= EXCLUDED.ingested_at
    """,
    "weather": """
        INSERT INTO staging.weather (ba, period_utc, temperature_c, wind_speed_100m_kmh,
                                     shortwave_radiation_wm2, cloud_cover_pct, source,
                                     event_id, ingested_at)
        VALUES %s
        ON CONFLICT (ba, period_utc) DO UPDATE SET
            temperature_c = EXCLUDED.temperature_c,
            wind_speed_100m_kmh = EXCLUDED.wind_speed_100m_kmh,
            shortwave_radiation_wm2 = EXCLUDED.shortwave_radiation_wm2,
            cloud_cover_pct = EXCLUDED.cloud_cover_pct, source = EXCLUDED.source,
            event_id = EXCLUDED.event_id, ingested_at = EXCLUDED.ingested_at, loaded_at = now()
        WHERE staging.weather.ingested_at <= EXCLUDED.ingested_at
    """,
}

COLUMNS = {
    "generation": ("ba", "period_utc", "fuel", "mw", "source", "event_id", "ingested_at"),
    "demand": ("ba", "period_utc", "demand_mw", "forecast_mw", "net_generation_mw",
               "interchange_mw", "source", "event_id", "ingested_at"),
    "weather": ("ba", "period_utc", "temperature_c", "wind_speed_100m_kmh",
                "shortwave_radiation_wm2", "cloud_cover_pct", "source", "event_id",
                "ingested_at"),
}
# Clé naturelle de chaque table (pour dédoublonner un lot avant l'UPSERT)
NATURAL_KEYS = {"generation": ("ba", "period_utc", "fuel"),
                "demand": ("ba", "period_utc"), "weather": ("ba", "period_utc")}


def upsert_events(conn, dataset: str, events: list[dict]) -> int:
    """Charge des événements validés. Renvoie le nombre de lignes traitées."""
    if not events:
        return 0
    # PostgreSQL refuse qu'un même INSERT ... ON CONFLICT touche deux fois la
    # même ligne : on garde la dernière version de chaque clé dans le lot.
    latest = {}
    for e in events:
        latest[tuple(e[k] for k in NATURAL_KEYS[dataset])] = e
    rows = [tuple(e.get(c) for c in COLUMNS[dataset]) for e in latest.values()]
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL[dataset], rows, page_size=1000)
    return len(rows)


def insert_dead_letters(conn, items: list[tuple[str, list[str], str]]) -> None:
    """items = [(topic d'origine, raisons, payload brut)]"""
    if items:
        with conn.cursor() as cur:
            execute_values(cur, "INSERT INTO ops.dead_letters (origin_topic, reasons, payload) VALUES %s",
                           items)


def heartbeat(conn, topic: str, offset: int, count: int) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ops.consumer_heartbeat (topic, last_message_at, last_offset, messages_total)
            VALUES (%s, now(), %s, %s)
            ON CONFLICT (topic) DO UPDATE SET last_message_at = now(),
                last_offset = EXCLUDED.last_offset,
                messages_total = ops.consumer_heartbeat.messages_total + EXCLUDED.messages_total
        """, (topic, offset, count))


# ---------------------------------------------------------------------------
# Journal des exécutions
# ---------------------------------------------------------------------------
def start_run(conn, source: str, dataset: str, mode: str) -> int:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO ops.ingestion_runs (source, dataset, mode) VALUES (%s,%s,%s) RETURNING run_id",
                    (source, dataset, mode))
        run_id = cur.fetchone()[0]
    conn.commit()  # visible tout de suite, même si la suite échoue
    return run_id


def finish_run(conn, run_id: int, status: str, fetched=0, valid=0, rejected=0,
               raw_file=None, error=None) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE ops.ingestion_runs SET finished_at = now(), status = %s, rows_fetched = %s,
                rows_valid = %s, rows_rejected = %s, raw_file = %s, error = %s
            WHERE run_id = %s
        """, (status, fetched, valid, rejected, raw_file, error, run_id))
    conn.commit()


# ---------------------------------------------------------------------------
# Disjoncteur (circuit breaker)
#   closed    : tout va bien, on appelle la source
#   open      : trop d'échecs, on n'appelle plus jusqu'à opened_until
#   half_open : délai écoulé, on tente UN appel ; succès -> closed, échec -> open
# ---------------------------------------------------------------------------
def circuit_allows(conn, source: str, now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("SELECT state, opened_until FROM ops.source_health WHERE source = %s", (source,))
        row = cur.fetchone()
        if row is None or row[0] == "closed":
            return True
        state, opened_until = row
        if state == "open" and opened_until and now >= opened_until:
            cur.execute("UPDATE ops.source_health SET state = 'half_open' WHERE source = %s", (source,))
            conn.commit()
            return True
        return state == "half_open"


def record_success(conn, source: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ops.source_health (source, state, consecutive_failures, last_success_at)
            VALUES (%s, 'closed', 0, now())
            ON CONFLICT (source) DO UPDATE SET state = 'closed', consecutive_failures = 0,
                last_success_at = now(), opened_until = NULL
        """, (source,))
    conn.commit()


def record_failure(conn, source: str, error: str) -> str:
    """Enregistre un échec et renvoie le nouvel état du disjoncteur."""
    cooldown = timedelta(minutes=config.CIRCUIT_COOLDOWN_MINUTES)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ops.source_health (source, consecutive_failures, last_failure_at, last_error)
            VALUES (%s, 1, now(), %s)
            ON CONFLICT (source) DO UPDATE SET
                consecutive_failures = ops.source_health.consecutive_failures + 1,
                last_failure_at = now(), last_error = EXCLUDED.last_error
            RETURNING consecutive_failures, state
        """, (source, error[:1000]))
        failures, state = cur.fetchone()
        if state == "half_open" or failures >= config.CIRCUIT_FAILURE_THRESHOLD:
            cur.execute("UPDATE ops.source_health SET state = 'open', opened_until = now() + %s WHERE source = %s",
                        (cooldown, source))
            state = "open"
    conn.commit()
    return state
