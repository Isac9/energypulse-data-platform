"""Source 1 : API v2 de l'EIA (U.S. Energy Information Administration).

Deux jeux de données horaires du « Hourly Electric Grid Monitor » (EIA-930) :
- fuel-type-data : production nette par filière (solaire, éolien, gaz...)
- region-data    : demande, prévision de demande, production totale, échanges

Clé gratuite : https://www.eia.gov/opendata/register.php
"""
from datetime import datetime, timedelta, timezone

from ..config import BA_CODES
from ..events import demand_event, generation_event, hour_iso
from ..http import get_json

BASE_URL = "https://api.eia.gov/v2/electricity/rto"
PAGE_SIZE = 5000
SOURCE = "eia_api"

# Convention du projet : period_utc = DÉBUT de l'heure, en UTC.
# L'EIA publie ses données horaires en « heure de fin » (hour ending) :
# la valeur de 01:00 couvre 00:00 -> 01:00. On retire donc une heure.
EIA_PERIOD_IS_HOUR_ENDING = True

REGION_TYPES = {"D": "demand_mw", "DF": "forecast_mw",
                "NG": "net_generation_mw", "TI": "interchange_mw"}


def _period_to_iso(period: str) -> str:
    dt = datetime.strptime(period, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
    if EIA_PERIOD_IS_HOUR_ENDING:
        dt -= timedelta(hours=1)
    return hour_iso(dt)


def _to_number(value):
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value  # on garde la valeur brute : la validation la rejettera
    return int(number) if number.is_integer() else number


def fetch(dataset: str, api_key: str, start: datetime, end: datetime,
          bas: list[str] | None = None) -> list[dict]:
    """Télécharge toutes les pages d'un jeu de données entre start et end."""
    params = {
        "api_key": api_key,
        "frequency": "hourly",
        "data[0]": "value",
        "start": start.strftime("%Y-%m-%dT%H"),
        "end": end.strftime("%Y-%m-%dT%H"),
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "length": PAGE_SIZE,
    }
    for i, ba in enumerate(bas or BA_CODES):
        params[f"facets[respondent][{i}]"] = ba

    rows, offset = [], 0
    while True:
        params["offset"] = offset
        payload = get_json(f"{BASE_URL}/{dataset}/data/", params)
        response = payload.get("response", {})
        page = response.get("data", [])
        rows.extend(page)
        total = int(response.get("total", 0) or 0)
        offset += len(page)
        if not page or offset >= total:
            return rows


def parse_fuel_type_rows(rows: list[dict]) -> list[dict]:
    return [
        generation_event(r.get("respondent"), _period_to_iso(r["period"]),
                         r.get("fueltype"), _to_number(r.get("value")), SOURCE)
        for r in rows
    ]


def parse_region_rows(rows: list[dict]) -> list[dict]:
    """L'API renvoie une ligne par (zone, heure, type). On les regroupe en un
    seul événement de demande par (zone, heure)."""
    grouped: dict[tuple, dict] = {}
    for r in rows:
        field = REGION_TYPES.get(r.get("type"))
        if not field:
            continue
        key = (r.get("respondent"), _period_to_iso(r["period"]))
        grouped.setdefault(key, {})[field] = _to_number(r.get("value"))
    return [demand_event(ba, period, SOURCE, **values)
            for (ba, period), values in grouped.items()]
