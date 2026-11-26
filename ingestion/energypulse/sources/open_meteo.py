"""Source 2 : Open-Meteo (météo horaire, gratuite, sans clé, usage non commercial).

- archive  : historique (disponible avec ~5 jours de décalage)
- forecast : jours récents (past_days) + prévisions
"""
from datetime import date, datetime, timezone

from ..config import BalancingAuthority
from ..events import hour_iso, weather_event
from ..http import get_json

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = "temperature_2m,wind_speed_100m,shortwave_radiation,cloud_cover"


def fetch_archive(ba: BalancingAuthority, start: date, end: date) -> dict:
    return get_json(ARCHIVE_URL, {
        "latitude": ba.latitude, "longitude": ba.longitude,
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "hourly": HOURLY_VARS, "timezone": "UTC",
    })


def fetch_recent(ba: BalancingAuthority, past_days: int = 2, forecast_days: int = 1) -> dict:
    return get_json(FORECAST_URL, {
        "latitude": ba.latitude, "longitude": ba.longitude,
        "past_days": past_days, "forecast_days": forecast_days,
        "hourly": HOURLY_VARS, "timezone": "UTC",
    })


def parse(payload: dict, ba_code: str, source: str) -> list[dict]:
    """Open-Meteo renvoie des colonnes (une liste par variable). On les
    transforme en lignes : un événement par heure."""
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    columns = {
        "temperature_c": hourly.get("temperature_2m", []),
        "wind_speed_100m_kmh": hourly.get("wind_speed_100m", []),
        "shortwave_radiation_wm2": hourly.get("shortwave_radiation", []),
        "cloud_cover_pct": hourly.get("cloud_cover", []),
    }
    events = []
    for i, t in enumerate(times):
        values = {k: (v[i] if i < len(v) else None) for k, v in columns.items()}
        if all(v is None for v in values.values()):
            continue  # heure pas encore calculée par le modèle
        period = hour_iso(datetime.fromisoformat(t).replace(tzinfo=timezone.utc))
        events.append(weather_event(ba_code, period, source, **values))
    return events
