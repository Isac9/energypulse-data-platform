"""Point d'entrée en ligne de commande d'EnergyPulse.

Exemples (dans le conteneur « app ») :
    python -m energypulse.pipeline init-topics
    python -m energypulse.pipeline live-eia --hours 6
    python -m energypulse.pipeline live-weather
    python -m energypulse.pipeline backfill-file --start 2026-06-01 --end 2026-06-30
    python -m energypulse.pipeline backfill-weather --start 2026-06-01 --end 2026-06-30
    python -m energypulse.pipeline replay --start 2026-06-10 --end 2026-06-11 --speed 144
    python -m energypulse.pipeline reprocess-raw --source open_meteo --day 2026-11-26
"""
import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone

from . import config, db, raw_store
from .http import SourceRejected, SourceUnavailable
from .sources import eia_api, eia_files, open_meteo
from .validation import validate

log = logging.getLogger("energypulse")


def _day(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _split_valid(events: list[dict]):
    valid, rejected = [], []
    for e in events:
        errors = validate(e)
        if errors:
            rejected.append((e, errors))
        else:
            valid.append(e)
    return valid, rejected


def _publisher():
    from .kafka_io import EventPublisher  # import tardif : pas de Kafka requis pour le batch
    return EventPublisher()


# ---------------------------------------------------------------------------
# Ingestion « live » : API -> fichiers bruts -> Kafka
# ---------------------------------------------------------------------------
def live_eia(hours: int) -> int:
    source = "eia_api"
    with db.connect() as conn:
        run_id = db.start_run(conn, source, "generation+demand", "live")
        if not config.EIA_API_KEY:
            db.finish_run(conn, run_id, "skipped", error="EIA_API_KEY absente du fichier .env")
            log.warning("Pas de clé EIA : ingestion API ignorée (utilise backfill-file ou replay).")
            return 0
        if not db.circuit_allows(conn, source):
            db.finish_run(conn, run_id, "skipped", error="disjoncteur ouvert : source mise en pause")
            log.warning("Disjoncteur ouvert pour %s : appel ignoré.", source)
            return 0

        end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(hours=hours)
        try:
            fuel_rows = eia_api.fetch("fuel-type-data", config.EIA_API_KEY, start, end)
            region_rows = eia_api.fetch("region-data", config.EIA_API_KEY, start, end)
        except (SourceUnavailable, SourceRejected) as exc:
            state = db.record_failure(conn, source, str(exc))
            db.finish_run(conn, run_id, "failed", error=str(exc))
            log.error("EIA indisponible (%s), disjoncteur : %s", exc, state)
            return 1

        raw_file = raw_store.save(source, "fuel_region",
                                  {"fuel-type-data": fuel_rows, "region-data": region_rows})
        events = eia_api.parse_fuel_type_rows(fuel_rows) + eia_api.parse_region_rows(region_rows)
        publisher = _publisher()
        valid, rejected = publisher.publish(events)
        publisher.flush()
        db.record_success(conn, source)
        db.finish_run(conn, run_id, "success", len(events), valid, rejected, raw_file)
        log.info("EIA API : %d événements publiés, %d rejetés", valid, rejected)
    return 0


def live_weather(past_days: int) -> int:
    source = "open_meteo"
    with db.connect() as conn:
        run_id = db.start_run(conn, source, "weather", "live")
        if not db.circuit_allows(conn, source):
            db.finish_run(conn, run_id, "skipped", error="disjoncteur ouvert : source mise en pause")
            log.warning("Disjoncteur ouvert pour %s : appel ignoré.", source)
            return 0
        payloads, events = {}, []
        try:
            for ba in config.BALANCING_AUTHORITIES:
                payload = open_meteo.fetch_recent(ba, past_days=past_days)
                payloads[ba.code] = payload
                events += open_meteo.parse(payload, ba.code, "open_meteo_forecast")
        except (SourceUnavailable, SourceRejected) as exc:
            state = db.record_failure(conn, source, str(exc))
            db.finish_run(conn, run_id, "failed", error=str(exc))
            log.error("Open-Meteo indisponible (%s), disjoncteur : %s", exc, state)
            return 1
        raw_file = raw_store.save(source, "forecast", payloads)
        # On n'envoie que les heures passées : une prévision n'est pas une observation.
        now_iso = datetime.now(timezone.utc).isoformat()
        events = [e for e in events if e["period_utc"] <= now_iso]
        publisher = _publisher()
        valid, rejected = publisher.publish(events)
        publisher.flush()
        db.record_success(conn, source)
        db.finish_run(conn, run_id, "success", len(events), valid, rejected, raw_file)
        log.info("Open-Meteo : %d événements publiés, %d rejetés", valid, rejected)
    return 0


# ---------------------------------------------------------------------------
# Batch : chargement d'historique directement dans PostgreSQL
# ---------------------------------------------------------------------------
def _load_batch(conn, events: list[dict], run_id: int, raw_file: str | None) -> None:
    valid, rejected = _split_valid(events)
    for dataset in ("generation", "demand", "weather"):
        db.upsert_events(conn, dataset, [e for e in valid if e["dataset"] == dataset])
    db.insert_dead_letters(conn, [("batch", errs, json.dumps(e)) for e, errs in rejected])
    conn.commit()
    db.finish_run(conn, run_id, "success", len(events), len(valid), len(rejected), raw_file)
    log.info("Batch : %d lignes chargées, %d rejetées", len(valid), len(rejected))


def backfill_file(start: datetime, end: datetime) -> int:
    end = end + timedelta(hours=23)
    with db.connect() as conn:
        for name in eia_files.files_for_period(start, end):
            run_id = db.start_run(conn, "eia_file", "generation+demand", "backfill")
            try:
                path = eia_files.download(name, config.RAW_DATA_DIR)
            except Exception as exc:  # fichier pas encore publié, réseau...
                db.finish_run(conn, run_id, "failed", error=str(exc))
                log.error("Téléchargement de %s impossible : %s", name, exc)
                return 1
            events = []
            for period, row in eia_files.iter_rows(path, start, end):
                events += eia_files.row_to_events(period, row)
            _load_batch(conn, events, run_id, str(path))
    return 0


def backfill_weather(start: datetime, end: datetime) -> int:
    with db.connect() as conn:
        run_id = db.start_run(conn, "open_meteo", "weather", "backfill")
        events, payloads = [], {}
        try:
            for ba in config.BALANCING_AUTHORITIES:
                payload = open_meteo.fetch_archive(ba, start.date(), end.date())
                payloads[ba.code] = payload
                events += open_meteo.parse(payload, ba.code, "open_meteo_archive")
        except (SourceUnavailable, SourceRejected) as exc:
            db.finish_run(conn, run_id, "failed", error=str(exc))
            log.error("Archive Open-Meteo indisponible : %s", exc)
            return 1
        raw_file = raw_store.save("open_meteo", "archive", payloads)
        _load_batch(conn, events, run_id, raw_file)
    return 0


# ---------------------------------------------------------------------------
# Rejeu accéléré : historique réel -> Kafka, au rythme du temps des données
# ---------------------------------------------------------------------------
def replay(start: datetime, end: datetime, speed: float, with_weather: bool = True) -> int:
    """Rejoue l'historique heure par heure. speed=144 : 24 h de données en 10 min.
    Les événements gardent leur vraie date (period_utc) : on ne fait PAS
    semblant que ce sont des mesures d'aujourd'hui."""
    end = end + timedelta(hours=23)
    by_hour: dict[str, list[dict]] = {}
    for name in eia_files.files_for_period(start, end):
        path = eia_files.download(name, config.RAW_DATA_DIR)
        for period, row in eia_files.iter_rows(path, start, end):
            by_hour.setdefault(period, []).extend(eia_files.row_to_events(period, row, "eia_file_replay"))
    if with_weather:
        for ba in config.BALANCING_AUTHORITIES:
            payload = open_meteo.fetch_archive(ba, start.date(), end.date())
            for e in open_meteo.parse(payload, ba.code, "open_meteo_replay"):
                if e["period_utc"] in by_hour:
                    by_hour[e["period_utc"]].append(e)

    hours = sorted(by_hour)
    if not hours:
        log.error("Aucune donnée à rejouer sur cette période.")
        return 1
    pause = 3600 / speed
    log.info("Rejeu de %d heures (%s -> %s), une heure de données toutes les %.1f s",
             len(hours), hours[0], hours[-1], pause)
    publisher = _publisher()
    with db.connect() as conn:
        run_id = db.start_run(conn, "eia_file", "replay", "replay")
        total_valid = total_rejected = 0
        try:
            for i, hour in enumerate(hours):
                valid, rejected = publisher.publish(by_hour[hour])
                publisher.producer.flush(10)
                total_valid += valid
                total_rejected += rejected
                log.info("[%d/%d] %s : %d événements", i + 1, len(hours), hour, valid)
                if i < len(hours) - 1:
                    time.sleep(pause)
            publisher.flush()
        except KeyboardInterrupt:
            db.finish_run(conn, run_id, "failed", total_valid + total_rejected, total_valid,
                          total_rejected, error="rejeu interrompu")
            return 1
        db.finish_run(conn, run_id, "success", total_valid + total_rejected, total_valid, total_rejected)
    return 0


def reprocess_raw(source: str, day: str) -> int:
    """Reprise après incident : relit les fichiers bruts d'une journée et les
    renvoie dans Kafka (sans rappeler l'API)."""
    files = [p for p in raw_store.list_files(source) if p.parent.name == day]
    if not files:
        log.error("Aucun fichier brut pour %s le %s", source, day)
        return 1
    events = []
    for path in files:
        payload = raw_store.load(str(path))["payload"]
        if source == "open_meteo":
            for ba_code, p in payload.items():
                events += open_meteo.parse(p, ba_code, "open_meteo_reprocess")
        elif source == "eia_api":
            events += eia_api.parse_fuel_type_rows(payload.get("fuel-type-data", []))
            events += eia_api.parse_region_rows(payload.get("region-data", []))
    publisher = _publisher()
    valid, rejected = publisher.publish(events)
    publisher.flush()
    log.info("Retraitement de %d fichier(s) : %d événements renvoyés, %d rejetés",
             len(files), valid, rejected)
    return 0


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s : %(message)s")
    parser = argparse.ArgumentParser(prog="energypulse")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-topics")
    p = sub.add_parser("live-eia"); p.add_argument("--hours", type=int, default=6)
    p = sub.add_parser("live-weather"); p.add_argument("--past-days", type=int, default=2)
    for name in ("backfill-file", "backfill-weather", "replay"):
        p = sub.add_parser(name)
        p.add_argument("--start", type=_day, required=True)
        p.add_argument("--end", type=_day, required=True)
        if name == "replay":
            p.add_argument("--speed", type=float, default=144)
            p.add_argument("--no-weather", action="store_true")
    p = sub.add_parser("reprocess-raw")
    p.add_argument("--source", choices=["open_meteo", "eia_api"], required=True)
    p.add_argument("--day", default=date.today().isoformat())
    args = parser.parse_args(argv)

    if args.command == "init-topics":
        from .kafka_io import ensure_topics
        ensure_topics()
        return 0
    if args.command == "live-eia":
        return live_eia(args.hours)
    if args.command == "live-weather":
        return live_weather(args.past_days)
    if args.command == "backfill-file":
        return backfill_file(args.start, args.end)
    if args.command == "backfill-weather":
        return backfill_weather(args.start, args.end)
    if args.command == "replay":
        return replay(args.start, args.end, args.speed, not args.no_weather)
    if args.command == "reprocess-raw":
        return reprocess_raw(args.source, args.day)
    return 2


if __name__ == "__main__":
    sys.exit(main())
