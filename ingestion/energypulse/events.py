"""Construction des événements envoyés dans Kafka.

Un événement = un dictionnaire JSON. Chaque événement a un identifiant
déterministe (event_id) calculé à partir de ce qui le rend unique.
Si la même mesure est envoyée deux fois (relance, rejeu...), elle aura le
même event_id : la base de données pourra l'ignorer ou la mettre à jour.
C'est ce qui rend le pipeline « idempotent ».
"""
import hashlib
from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def hour_iso(dt: datetime) -> str:
    """Normalise une date à l'heure pleine, en UTC, au format ISO 8601."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return dt.isoformat()


def make_event_id(*parts: str) -> str:
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()


def generation_event(ba: str, period_utc: str, fuel: str, mw, source: str) -> dict:
    return {
        "event_id": make_event_id("generation", ba, period_utc, fuel),
        "dataset": "generation",
        "source": source,
        "ba": ba,
        "period_utc": period_utc,
        "fuel": fuel,
        "mw": mw,
        "ingested_at": utc_now_iso(),
    }


def demand_event(ba: str, period_utc: str, source: str, demand_mw=None,
                 forecast_mw=None, net_generation_mw=None, interchange_mw=None) -> dict:
    return {
        "event_id": make_event_id("demand", ba, period_utc),
        "dataset": "demand",
        "source": source,
        "ba": ba,
        "period_utc": period_utc,
        "demand_mw": demand_mw,
        "forecast_mw": forecast_mw,
        "net_generation_mw": net_generation_mw,
        "interchange_mw": interchange_mw,
        "ingested_at": utc_now_iso(),
    }


def weather_event(ba: str, period_utc: str, source: str, temperature_c=None,
                  wind_speed_100m_kmh=None, shortwave_radiation_wm2=None,
                  cloud_cover_pct=None) -> dict:
    return {
        "event_id": make_event_id("weather", ba, period_utc),
        "dataset": "weather",
        "source": source,
        "ba": ba,
        "period_utc": period_utc,
        "temperature_c": temperature_c,
        "wind_speed_100m_kmh": wind_speed_100m_kmh,
        "shortwave_radiation_wm2": shortwave_radiation_wm2,
        "cloud_cover_pct": cloud_cover_pct,
        "ingested_at": utc_now_iso(),
    }
