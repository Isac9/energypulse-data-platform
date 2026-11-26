"""Simulation d'incidents pour tester la robustesse du pipeline.

    docker compose run --rm app python /opt/scripts/chaos.py corrupt
    docker compose run --rm app python /opt/scripts/chaos.py source-down
    docker compose run --rm app python /opt/scripts/chaos.py source-recover
    docker compose run --rm app python /opt/scripts/chaos.py gap --ba ERCO --hours 6

corrupt        : envoie des messages invalides dans Kafka -> doivent finir en DLQ
source-down    : simule une panne d'Open-Meteo (URL injoignable) -> disjoncteur ouvert
source-recover : force le disjoncteur en « half_open » et relance une vraie ingestion
gap            : supprime N heures de données d'une zone -> incident de complétude
                 (on répare ensuite avec le DAG energypulse_backfill)
"""
import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

from energypulse import config, db, pipeline
from energypulse.events import generation_event, hour_iso
from energypulse.kafka_io import EventPublisher
from energypulse.sources import open_meteo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("chaos")


def corrupt() -> None:
    p = EventPublisher()
    now = hour_iso(datetime.now(timezone.utc) - timedelta(hours=3))
    bad = [
        (config.TOPIC_GENERATION, b"{ceci n'est pas du JSON"),
        (config.TOPIC_GENERATION, json.dumps(generation_event("CISO", now, "SUN", -99999, "chaos")).encode()),
        (config.TOPIC_GENERATION, json.dumps(generation_event("XXXX", now, "WND", 120, "chaos")).encode()),
        (config.TOPIC_GENERATION, json.dumps(generation_event("ERCO", "2099-01-01T00:00:00+00:00", "NG", 5000, "chaos")).encode()),
        (config.TOPIC_GENERATION, json.dumps(generation_event("PJM", now, "NUC", "beaucoup", "chaos")).encode()),
        (config.TOPIC_DEMAND, json.dumps(generation_event("NYIS", now, "WAT", 300, "chaos")).encode()),  # mauvais topic
    ]
    for topic, raw in bad:
        p.send_raw(topic, "chaos", raw)
    p.flush()
    log.info("%d messages corrompus envoyés. Vérifie : SELECT * FROM ops.dead_letters ORDER BY id DESC;", len(bad))


def source_down(attempts: int) -> None:
    open_meteo.FORECAST_URL = "https://open-meteo.invalid/v1/forecast"  # domaine qui n'existe pas
    import energypulse.http as http
    original = http.get_json
    http.get_json = lambda url, params, **kw: original(url, params, attempts=1, timeout=5)
    open_meteo.get_json = http.get_json
    for i in range(1, attempts + 1):
        log.info("--- Tentative d'ingestion %d/%d avec Open-Meteo en panne ---", i, attempts)
        pipeline.live_weather(past_days=1)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT state, consecutive_failures, opened_until FROM ops.source_health WHERE source='open_meteo'")
        log.info("État du disjoncteur open_meteo : %s", cur.fetchone())


def source_recover() -> None:
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("UPDATE ops.source_health SET opened_until = now() WHERE source = 'open_meteo' AND state = 'open'")
    log.info("Délai de coupure écoulé (forcé) : la prochaine ingestion teste la source (half_open).")
    pipeline.live_weather(past_days=2)
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT state, consecutive_failures FROM ops.source_health WHERE source='open_meteo'")
        log.info("État du disjoncteur open_meteo : %s", cur.fetchone())


def gap(ba: str, hours: int) -> None:
    with db.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT MAX(period_utc) FROM staging.generation WHERE ba = %s", (ba,))
        last = cur.fetchone()[0]
        if last is None:
            log.error("Aucune donnée pour %s", ba)
            return
        start, end = last - timedelta(hours=hours + 12), last - timedelta(hours=12)
        for table in ("staging.generation", "staging.demand"):
            cur.execute(f"DELETE FROM {table} WHERE ba = %s AND period_utc > %s AND period_utc <= %s",
                        (ba, start, end))
            log.info("%s : %d lignes supprimées (%s -> %s)", table, cur.rowcount, start, end)
    log.info("Lance le DAG energypulse_quality_sla : un incident « completeness » doit s'ouvrir pour %s.", ba)


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="scenario", required=True)
    sub.add_parser("corrupt")
    p = sub.add_parser("source-down"); p.add_argument("--attempts", type=int, default=config.CIRCUIT_FAILURE_THRESHOLD + 1)
    sub.add_parser("source-recover")
    p = sub.add_parser("gap"); p.add_argument("--ba", default="ERCO"); p.add_argument("--hours", type=int, default=6)
    args = parser.parse_args()
    if args.scenario == "corrupt":
        corrupt()
    elif args.scenario == "source-down":
        source_down(args.attempts)
    elif args.scenario == "source-recover":
        source_recover()
    elif args.scenario == "gap":
        gap(args.ba, args.hours)
    return 0


if __name__ == "__main__":
    sys.exit(main())
