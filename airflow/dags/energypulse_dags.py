"""DAGs Airflow d'EnergyPulse.

1. energypulse_ingest_hourly  : toutes les heures, API EIA + Open-Meteo -> Kafka
2. energypulse_quality_sla    : toutes les heures, marts + contrôles + incidents
3. energypulse_backfill       : à la demande, chargement d'historique (batch)

Le code métier est dans le paquet « energypulse » (monté dans /opt/energypulse) :
les DAGs ne font qu'orchestrer. On peut donc tester ce code sans Airflow.
"""
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param

DEFAULT_ARGS = {
    "owner": "energypulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


def _check(code: int, what: str) -> None:
    if code != 0:
        raise AirflowException(f"{what} a échoué (code {code}), voir ops.ingestion_runs")


# ---------------------------------------------------------------------------
@dag(dag_id="energypulse_ingest_hourly", schedule="5 * * * *",
     start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False, max_active_runs=1,
     default_args=DEFAULT_ARGS, tags=["energypulse", "ingestion"],
     doc_md="Ingestion live : API EIA (6 dernières heures) et Open-Meteo (2 derniers jours) vers Kafka.")
def ingest_hourly():
    @task
    def init_topics():
        from energypulse.kafka_io import ensure_topics
        ensure_topics()

    @task
    def ingest_eia():
        from energypulse.pipeline import live_eia
        _check(live_eia(hours=6), "Ingestion EIA")

    @task
    def ingest_weather():
        from energypulse.pipeline import live_weather
        _check(live_weather(past_days=2), "Ingestion Open-Meteo")

    topics = init_topics()
    topics >> [ingest_eia(), ingest_weather()]


# ---------------------------------------------------------------------------
@dag(dag_id="energypulse_quality_sla", schedule="20 * * * *",
     start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False, max_active_runs=1,
     default_args={**DEFAULT_ARGS, "retries": 1}, tags=["energypulse", "qualité", "sla"],
     doc_md="Rafraîchit les marts, exécute les contrôles SLA, détecte les anomalies, ouvre/ferme les incidents.")
def quality_sla():
    def run_check(name: str) -> dict:
        from energypulse import db, quality
        with db.connect() as conn:
            results = getattr(quality, name)(conn)
            summary = quality.record(conn, results)
        print(f"{name} : {summary}")
        for r in results:
            if r.status != "ok":
                print(f"  [{r.status}] {r.target} : {r.details}")
        return summary

    @task
    def refresh_marts():
        from energypulse import db, quality
        with db.connect() as conn:
            quality.refresh_marts(conn)

    @task
    def freshness():
        return run_check("check_freshness")

    @task
    def completeness():
        return run_check("check_completeness")

    @task
    def pipeline_health():
        return run_check("check_pipeline")

    @task
    def anomalies():
        return run_check("detect_anomalies")

    @task
    def report(*summaries):
        total = {k: sum(s[k] for s in summaries) for k in summaries[0]}
        print(f"Bilan des contrôles : {total}")
        return total

    marts = refresh_marts()
    checks = [freshness(), completeness(), pipeline_health()]
    anomaly = anomalies()
    marts >> checks
    marts >> anomaly
    report(*checks, anomaly)


# ---------------------------------------------------------------------------
@dag(dag_id="energypulse_backfill", schedule=None,
     start_date=datetime(2026, 11, 1, tzinfo=timezone.utc), catchup=False, max_active_runs=1,
     default_args=DEFAULT_ARGS, tags=["energypulse", "batch"],
     params={
         "start": Param((datetime.now(timezone.utc) - timedelta(days=35)).strftime("%Y-%m-%d"),
                        type="string", format="date", description="Premier jour (AAAA-MM-JJ)"),
         "end": Param((datetime.now(timezone.utc) - timedelta(days=6)).strftime("%Y-%m-%d"),
                      type="string", format="date", description="Dernier jour (AAAA-MM-JJ)"),
     },
     doc_md="Chargement batch d'historique : fichiers publics EIA-930 + archive Open-Meteo, "
            "directement dans PostgreSQL (sans Kafka). Idempotent : relançable sans doublon.")
def backfill():
    @task
    def eia_files(params=None):
        from energypulse.pipeline import _day, backfill_file
        _check(backfill_file(_day(params["start"]), _day(params["end"])), "Backfill EIA-930")

    @task
    def weather_archive(params=None):
        from energypulse.pipeline import _day, backfill_weather
        _check(backfill_weather(_day(params["start"]), _day(params["end"])), "Backfill météo")

    @task
    def refresh_marts():
        from energypulse import db, quality
        with db.connect() as conn:
            quality.refresh_marts(conn)

    [eia_files(), weather_archive()] >> refresh_marts()


ingest_hourly()
quality_sla()
backfill()
