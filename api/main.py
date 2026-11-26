"""API de restitution EnergyPulse (FastAPI).

Documentation interactive : http://localhost:8030/docs
Tableau de bord          : http://localhost:8030/
"""
import os
from datetime import datetime
from pathlib import Path

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from psycopg2.extras import RealDictCursor

DSN = (f"host={os.environ.get('POSTGRES_HOST', 'localhost')} port={os.environ.get('POSTGRES_PORT', '5440')} "
       f"dbname={os.environ.get('POSTGRES_DB', 'energypulse')} user={os.environ.get('POSTGRES_USER', 'energypulse')} "
       f"password={os.environ.get('POSTGRES_PASSWORD', 'energypulse')}")
BA_CODES = {"CISO", "ERCO", "PJM", "NYIS"}

app = FastAPI(title="EnergyPulse API", version="1.0.0",
              description="Indicateurs énergétiques (EIA + Open-Meteo) et supervision du pipeline.")


def query(sql: str, params: tuple = ()) -> list[dict]:
    with psycopg2.connect(DSN) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def check_ba(ba: str) -> str:
    ba = ba.upper()
    if ba not in BA_CODES:
        raise HTTPException(404, f"Zone inconnue : {ba}. Zones disponibles : {sorted(BA_CODES)}")
    return ba


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/health", tags=["supervision"])
def health():
    """Vivacité de l'API et de la base."""
    try:
        query("SELECT 1")
        return {"status": "ok", "database": "ok"}
    except psycopg2.Error as exc:
        raise HTTPException(503, f"base indisponible : {exc}") from exc


@app.get("/zones", tags=["référentiel"])
def zones():
    return query("SELECT * FROM staging.balancing_authority ORDER BY ba")


@app.get("/generation/{ba}", tags=["énergie"])
def generation(ba: str, hours: int = Query(48, ge=1, le=24 * 31)):
    """Production horaire par filière et météo, sur les N dernières heures disponibles."""
    ba = check_ba(ba)
    return query("""
        SELECT * FROM marts.energy_hourly
        WHERE ba = %s AND period_utc > (SELECT MAX(period_utc) FROM marts.energy_hourly WHERE ba = %s)
                                       - make_interval(hours => %s)
        ORDER BY period_utc
    """, (ba, ba, hours))


@app.get("/generation/{ba}/mix", tags=["énergie"])
def generation_mix(ba: str, start: datetime | None = None, end: datetime | None = None):
    """Production totale par filière (MWh) sur une période (par défaut : 7 derniers jours de données)."""
    ba = check_ba(ba)
    return query("""
        WITH bounds AS (
            SELECT COALESCE(%s::timestamptz, MAX(period_utc) - interval '7 days') AS s,
                   COALESCE(%s::timestamptz, MAX(period_utc)) AS e
            FROM staging.generation WHERE ba = %s
        )
        SELECT g.fuel, f.label, f.is_renewable, ROUND(SUM(g.mw)) AS mwh, COUNT(*) AS hours
        FROM staging.generation g JOIN staging.fuel_type f USING (fuel), bounds
        WHERE g.ba = %s AND g.period_utc BETWEEN bounds.s AND bounds.e
        GROUP BY g.fuel, f.label, f.is_renewable ORDER BY mwh DESC
    """, (start, end, ba, ba))


@app.get("/live/{ba}", tags=["énergie"])
def live(ba: str, limit: int = Query(48, ge=1, le=500)):
    """Mix renouvelable calculé en continu par Spark Structured Streaming."""
    ba = check_ba(ba)
    return query("""SELECT * FROM marts.generation_mix_live WHERE ba = %s
                    ORDER BY window_start DESC LIMIT %s""", (ba, limit))


@app.get("/daily/{ba}", tags=["énergie"])
def daily(ba: str, days: int = Query(30, ge=1, le=366)):
    ba = check_ba(ba)
    return query("SELECT * FROM marts.energy_daily WHERE ba = %s ORDER BY day_utc DESC LIMIT %s", (ba, days))


@app.get("/weather-correlation", tags=["énergie"])
def weather_correlation():
    """Corrélation solaire/rayonnement, éolien/vent, demande/température."""
    return query("SELECT * FROM marts.weather_correlation ORDER BY ba")


@app.get("/anomalies", tags=["qualité"])
def anomalies(ba: str | None = None, limit: int = Query(100, ge=1, le=1000)):
    if ba:
        ba = check_ba(ba)
    return query("""SELECT * FROM ops.anomalies WHERE (%s::text IS NULL OR ba = %s)
                    ORDER BY period_utc DESC LIMIT %s""", (ba, ba, limit))


@app.get("/sla", tags=["supervision"])
def sla():
    """Dernier résultat de chaque contrôle (fraîcheur, complétude, pipeline, DLQ, anomalies)."""
    return query("""
        SELECT DISTINCT ON (check_name, target) check_name, target, status, measured, threshold,
               details, checked_at
        FROM ops.sla_checks ORDER BY check_name, target, checked_at DESC
    """)


@app.get("/incidents", tags=["supervision"])
def incidents(open_only: bool = True, limit: int = Query(100, ge=1, le=1000)):
    return query("""SELECT * FROM ops.incidents WHERE (NOT %s OR resolved_at IS NULL)
                    ORDER BY opened_at DESC LIMIT %s""", (open_only, limit))


@app.get("/pipeline", tags=["supervision"])
def pipeline():
    """État des sources (disjoncteur), dernières exécutions, consumer, messages rejetés."""
    return {
        "sources": query("SELECT * FROM ops.source_health ORDER BY source"),
        "last_runs": query("SELECT * FROM ops.ingestion_runs ORDER BY run_id DESC LIMIT 20"),
        "consumer": query("SELECT *, now() - last_message_at AS silence FROM ops.consumer_heartbeat ORDER BY topic"),
        "dead_letters_24h": query("""SELECT origin_topic, COUNT(*) AS messages, MAX(received_at) AS last
                                     FROM ops.dead_letters WHERE received_at > now() - interval '24 hours'
                                     GROUP BY origin_topic"""),
    }
