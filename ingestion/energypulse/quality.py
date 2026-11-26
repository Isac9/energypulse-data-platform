"""Contrôles qualité, SLA, détection d'anomalies et gestion des incidents.

Appelé par le DAG Airflow « energypulse_quality_sla » (toutes les heures).
Chaque contrôle produit un statut : ok / warning / breach.
  - breach  -> incident « critical » ouvert (ou mis à jour s'il existe déjà)
  - warning -> incident « warning »
  - ok      -> l'incident ouvert éventuel est automatiquement résolu
"""
import logging
from dataclasses import dataclass

from . import config

log = logging.getLogger(__name__)

# --- SLA (Service Level Agreement) : délai maximum toléré ---------------
# L'EIA publie avec 1 à 2 h de retard ; au-delà de 4 h on s'inquiète,
# au-delà de 8 h c'est une rupture de SLA.
FRESHNESS_SLA_HOURS = {
    "generation": (4, 8),   # (seuil warning, seuil breach)
    "demand": (4, 8),
    "weather": (3, 6),
}
COMPLETENESS_WINDOW_HOURS = 48
COMPLETENESS_WARNING, COMPLETENESS_BREACH = 0.98, 0.95
PIPELINE_MAX_SILENCE_HOURS = 2     # dernière ingestion réussie
DLQ_WARNING, DLQ_BREACH = 1, 100   # messages rejetés sur la dernière heure

# --- Anomalies -------------------------------------------------------------
ANOMALY_BASELINE_DAYS = 14
ANOMALY_Z_THRESHOLD = -3.0         # 3 écarts-types sous la normale
ANOMALY_MIN_DROP_PCT = -30.0       # ET au moins 30 % sous la normale
SOLAR_MIN_RADIATION = 300          # W/m² : il fait vraiment jour
SOLAR_MIN_RATIO = 0.5              # solaire < 50 % de ce que la météo permet


@dataclass
class CheckResult:
    check_name: str
    target: str
    status: str
    measured: float | None
    threshold: float | None
    details: str


# ---------------------------------------------------------------------------
# Fonctions pures (testées unitairement dans tests/)
# ---------------------------------------------------------------------------
def grade(value: float, warning: float, breach: float, higher_is_worse: bool = True) -> str:
    if higher_is_worse:
        return "breach" if value > breach else "warning" if value > warning else "ok"
    return "breach" if value < breach else "warning" if value < warning else "ok"


def is_drop_anomaly(observed: float, mean: float, std: float) -> tuple[bool, float | None, float | None]:
    """Baisse inhabituelle = z-score <= -3 ET baisse >= 30 %.
    Les deux conditions évitent les fausses alertes : le z-score seul crie
    au loup quand la variance est minuscule, le % seul ignore la variabilité."""
    if mean is None or mean <= 0 or std is None:
        return False, None, None
    deviation_pct = 100 * (observed - mean) / mean
    zscore = (observed - mean) / std if std > 0 else None
    flagged = (zscore is not None and zscore <= ANOMALY_Z_THRESHOLD
               and deviation_pct <= ANOMALY_MIN_DROP_PCT)
    return flagged, deviation_pct, zscore


def is_solar_anomaly(solar_mw: float, radiation: float, mw_per_wm2: float) -> tuple[bool, float | None]:
    """Le solaire dépend du rayonnement. mw_per_wm2 = rendement habituel de la
    zone (appris sur 14 jours). Si le soleil brille mais que la production est
    deux fois plus faible que prévu, ce n'est pas la météo : c'est un problème."""
    if radiation is None or radiation < SOLAR_MIN_RADIATION or not mw_per_wm2:
        return False, None
    expected = radiation * mw_per_wm2
    return solar_mw < SOLAR_MIN_RATIO * expected, expected


def incident_action(status: str, has_open_incident: bool) -> str:
    if status == "ok":
        return "resolve" if has_open_incident else "none"
    return "update" if has_open_incident else "open"


# ---------------------------------------------------------------------------
# Contrôles (SQL)
# ---------------------------------------------------------------------------
def refresh_marts(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY marts.energy_hourly")
    conn.commit()


def check_freshness(conn) -> list[CheckResult]:
    results = []
    with conn.cursor() as cur:
        for dataset, (warn, breach) in FRESHNESS_SLA_HOURS.items():
            # Pour la météo, on ignore les heures futures (prévisions).
            cur.execute(f"""
                SELECT ba.ba,
                       EXTRACT(EPOCH FROM now() - MAX(t.period_utc)) / 3600,
                       MAX(t.period_utc)
                FROM staging.balancing_authority ba
                LEFT JOIN staging.{dataset} t ON t.ba = ba.ba AND t.period_utc <= now()
                GROUP BY ba.ba ORDER BY ba.ba
            """)
            for ba, lag_hours, last_period in cur.fetchall():
                if lag_hours is None:
                    results.append(CheckResult("freshness", f"{dataset}:{ba}", "breach", None, breach,
                                               "aucune donnée reçue"))
                    continue
                lag = float(lag_hours)
                results.append(CheckResult("freshness", f"{dataset}:{ba}", grade(lag, warn, breach),
                                           round(lag, 1), breach,
                                           f"dernière heure reçue {last_period:%Y-%m-%d %H:%M} UTC, retard {lag:.1f} h"))
    return results


def check_completeness(conn) -> list[CheckResult]:
    """Sur les 48 dernières heures DISPONIBLES de chaque zone, combien d'heures
    manquent ? (indépendant de la fraîcheur : on cherche des trous)."""
    results = []
    with conn.cursor() as cur:
        for dataset in ("generation", "demand", "weather"):
            cur.execute(f"""
                WITH last AS (
                    SELECT ba, MAX(period_utc) AS last_period
                    FROM staging.{dataset} WHERE period_utc <= now() GROUP BY ba
                )
                SELECT l.ba, COUNT(DISTINCT t.period_utc), l.last_period
                FROM last l
                LEFT JOIN staging.{dataset} t
                  ON t.ba = l.ba AND t.period_utc >  l.last_period - interval '{COMPLETENESS_WINDOW_HOURS} hours'
                                 AND t.period_utc <= l.last_period
                GROUP BY l.ba, l.last_period ORDER BY l.ba
            """)
            for ba, hours, last_period in cur.fetchall():
                ratio = hours / COMPLETENESS_WINDOW_HOURS
                results.append(CheckResult(
                    "completeness", f"{dataset}:{ba}",
                    grade(ratio, COMPLETENESS_WARNING, COMPLETENESS_BREACH, higher_is_worse=False),
                    round(ratio, 3), COMPLETENESS_BREACH,
                    f"{hours}/{COMPLETENESS_WINDOW_HOURS} heures présentes avant {last_period:%Y-%m-%d %H:%M}"))
    return results


def check_pipeline(conn) -> list[CheckResult]:
    results = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.source,
                   EXTRACT(EPOCH FROM now() - MAX(r.finished_at) FILTER (WHERE r.status = 'success')) / 3600,
                   COUNT(*) FILTER (WHERE r.status = 'failed' AND r.started_at > now() - interval '3 hours'),
                   COALESCE(h.state, 'closed')
            FROM (VALUES ('eia_api'), ('open_meteo')) AS s(source)
            LEFT JOIN ops.ingestion_runs r ON r.source = s.source AND r.mode = 'live'
            LEFT JOIN ops.source_health h ON h.source = s.source
            GROUP BY s.source, h.state
        """)
        for source, silence, failures, state in cur.fetchall():
            if state == "open":
                status, details = "breach", "disjoncteur ouvert : la source est en pause"
            elif silence is None:
                status, details = "warning", "aucune ingestion live réussie pour l'instant"
            else:
                status = grade(float(silence), PIPELINE_MAX_SILENCE_HOURS / 2, PIPELINE_MAX_SILENCE_HOURS)
                details = f"dernière ingestion réussie il y a {float(silence):.1f} h, {failures} échec(s) sur 3 h"
            results.append(CheckResult("pipeline", source, status,
                                       None if silence is None else round(float(silence), 2),
                                       PIPELINE_MAX_SILENCE_HOURS, details))

        cur.execute("SELECT COUNT(*) FROM ops.dead_letters WHERE received_at > now() - interval '1 hour'")
        dlq = cur.fetchone()[0]
        status = "breach" if dlq >= DLQ_BREACH else "warning" if dlq >= DLQ_WARNING else "ok"
        results.append(CheckResult("dlq", "energy.dlq", status, dlq, DLQ_BREACH,
                                   f"{dlq} message(s) rejeté(s) sur la dernière heure"))
    return results


def detect_anomalies(conn) -> list[CheckResult]:
    """Compare chaque heure des 48 dernières heures disponibles à la même heure
    de la journée sur les 14 jours précédents."""
    found: dict[str, int] = {ba: 0 for ba in config.BA_CODES}
    with conn.cursor() as cur:
        cur.execute(f"""
            WITH last AS (SELECT ba, MAX(period_utc) AS last_period FROM marts.energy_hourly GROUP BY ba),
            recent AS (
                SELECT e.* FROM marts.energy_hourly e JOIN last l USING (ba)
                WHERE e.period_utc > l.last_period - interval '48 hours'
            ),
            baseline AS (
                SELECT r.ba, r.period_utc,
                       AVG(h.total_mw) AS mean_total, STDDEV_SAMP(h.total_mw) AS std_total,
                       AVG(h.renewable_mw) AS mean_ren, STDDEV_SAMP(h.renewable_mw) AS std_ren,
                       SUM(h.solar_mw) FILTER (WHERE h.shortwave_radiation_wm2 >= {SOLAR_MIN_RADIATION})
                         / NULLIF(SUM(h.shortwave_radiation_wm2) FILTER (WHERE h.shortwave_radiation_wm2 >= {SOLAR_MIN_RADIATION}), 0)
                         AS solar_mw_per_wm2
                FROM recent r
                JOIN marts.energy_hourly h
                  ON h.ba = r.ba
                 AND h.period_utc >= r.period_utc - interval '{ANOMALY_BASELINE_DAYS} days'
                 AND h.period_utc <  r.period_utc - interval '23 hours'   -- jours précédents seulement
                 AND EXTRACT(HOUR FROM h.period_utc) = EXTRACT(HOUR FROM r.period_utc)
                GROUP BY r.ba, r.period_utc
            )
            SELECT r.ba, r.period_utc, r.total_mw, r.renewable_mw, r.solar_mw, r.shortwave_radiation_wm2,
                   b.mean_total, b.std_total, b.mean_ren, b.std_ren, b.solar_mw_per_wm2
            FROM recent r JOIN baseline b USING (ba, period_utc)
        """)
        rows = cur.fetchall()
        anomalies = []
        for (ba, period, total, ren, solar, radiation,
             mean_total, std_total, mean_ren, std_ren, ratio) in rows:
            for group, obs, mean, std in (("total", total, mean_total, std_total),
                                          ("renewable", ren, mean_ren, std_ren)):
                if obs is None:
                    continue
                flagged, dev, z = is_drop_anomaly(float(obs), _f(mean), _f(std))
                if flagged:
                    anomalies.append((ba, group, period, obs, mean, dev, z))
            if solar is not None:
                flagged, expected = is_solar_anomaly(float(solar), _f(radiation), _f(ratio))
                if flagged:
                    dev = 100 * (float(solar) - expected) / expected
                    anomalies.append((ba, "solar_vs_weather", period, solar, expected, dev, None))
        for a in anomalies:
            found[a[0]] += 1
            cur.execute("""
                INSERT INTO ops.anomalies (ba, fuel_group, period_utc, observed_mw, expected_mw, deviation_pct, zscore)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (ba, fuel_group, period_utc) DO UPDATE SET observed_mw = EXCLUDED.observed_mw,
                    expected_mw = EXCLUDED.expected_mw, deviation_pct = EXCLUDED.deviation_pct,
                    zscore = EXCLUDED.zscore, detected_at = now()
            """, a)
    conn.commit()
    return [CheckResult("anomaly", f"production:{ba}", "warning" if n else "ok", n, 0,
                        f"{n} heure(s) anormale(s) sur les 48 dernières heures")
            for ba, n in found.items()]


def _f(value):
    return None if value is None else float(value)


# ---------------------------------------------------------------------------
# Enregistrement des résultats et gestion des incidents
# ---------------------------------------------------------------------------
def record(conn, results: list[CheckResult]) -> dict:
    summary = {"ok": 0, "warning": 0, "breach": 0, "opened": 0, "resolved": 0}
    with conn.cursor() as cur:
        for r in results:
            summary[r.status] += 1
            cur.execute("""INSERT INTO ops.sla_checks (check_name, target, status, measured, threshold, details)
                           VALUES (%s,%s,%s,%s,%s,%s)""",
                        (r.check_name, r.target, r.status, r.measured, r.threshold, r.details))
            cur.execute("SELECT incident_id FROM ops.incidents WHERE check_name=%s AND target=%s AND resolved_at IS NULL",
                        (r.check_name, r.target))
            row = cur.fetchone()
            action = incident_action(r.status, row is not None)
            severity = "critical" if r.status == "breach" else "warning"
            if action == "open":
                cur.execute("""INSERT INTO ops.incidents (check_name, target, severity, details)
                               VALUES (%s,%s,%s,%s)""", (r.check_name, r.target, severity, r.details))
                summary["opened"] += 1
                log.warning("INCIDENT ouvert [%s] %s : %s", r.check_name, r.target, r.details)
            elif action == "update":
                cur.execute("""UPDATE ops.incidents SET last_seen_at = now(), occurrences = occurrences + 1,
                               severity = %s, details = %s WHERE incident_id = %s""",
                            (severity, r.details, row[0]))
            elif action == "resolve":
                cur.execute("UPDATE ops.incidents SET resolved_at = now() WHERE incident_id = %s", (row[0],))
                summary["resolved"] += 1
                log.info("Incident résolu [%s] %s", r.check_name, r.target)
    conn.commit()
    return summary
