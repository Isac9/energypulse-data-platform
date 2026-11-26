"""Source 1 bis : fichiers publics EIA-930 (historique, sans clé API).

L'EIA publie chaque semestre un fichier CSV « BALANCE » par zone et par heure :
https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/EIA930_BALANCE_2026_Jan_Jun.csv

On s'en sert pour : 1) charger l'historique en batch, 2) rejouer cet
historique dans Kafka en accéléré, 3) continuer à travailler si l'API tombe.
"""
import csv
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from ..config import BA_CODES
from ..events import demand_event, generation_event, hour_iso

log = logging.getLogger(__name__)

FILE_URL = "https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/{name}"
SOURCE = "eia_file"

# Colonne du fichier -> code filière EIA
FUEL_COLUMNS = {
    "Net Generation (MW) from Coal": "COL",
    "Net Generation (MW) from Natural Gas": "NG",
    "Net Generation (MW) from Nuclear": "NUC",
    "Net Generation (MW) from All Petroleum Products": "OIL",
    "Net Generation (MW) from Hydropower Excluding Pumped Storage": "WAT",
    "Net Generation (MW) from Pumped Storage": "PS",
    "Net Generation (MW) from Solar without Integrated Battery Storage": "SUN",
    "Net Generation (MW) from Solar with Integrated Battery Storage": "SNB",
    "Net Generation (MW) from Wind without Integrated Battery Storage": "WND",
    "Net Generation (MW) from Wind with Integrated Battery Storage": "WNB",
    "Net Generation (MW) from Battery Storage": "BAT",
    "Net Generation (MW) from Other Energy Storage": "OES",
    "Net Generation (MW) from Unknown Energy Storage": "UES",
    "Net Generation (MW) from Geothermal": "GEO",
    "Net Generation (MW) from Other Fuel Sources": "OTH",
    "Net Generation (MW) from Unknown Fuel Sources": "UNK",
}
DEMAND_COLUMNS = {
    "Demand (MW)": "demand_mw",
    "Demand Forecast (MW)": "forecast_mw",
    "Net Generation (MW)": "net_generation_mw",
    "Total Interchange (MW)": "interchange_mw",
}


def file_name_for(day: datetime) -> str:
    half = "Jan_Jun" if day.month <= 6 else "Jul_Dec"
    return f"EIA930_BALANCE_{day.year}_{half}.csv"


def files_for_period(start: datetime, end: datetime) -> list[str]:
    names, day = [], start
    while day <= end:
        name = file_name_for(day)
        if name not in names:
            names.append(name)
        day += timedelta(days=1)
    return names


def download(name: str, target_dir: str, force: bool = False) -> Path:
    """Télécharge le fichier une seule fois (cache local dans data/raw/eia_files)."""
    target = Path(target_dir) / "eia_files" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and not force:
        return target
    tmp = target.with_suffix(".part")
    log.info("Téléchargement de %s", name)
    with requests.get(FILE_URL.format(name=name), stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    tmp.replace(target)  # le fichier n'apparaît qu'une fois complet
    return target


def _number(value: str):
    value = (value or "").replace(",", "").strip()
    if not value:
        return None
    try:
        number = float(value)
    except ValueError:
        return value
    return int(number) if number.is_integer() else number


def _period(row: dict) -> str:
    # « UTC Time at End of Hour » = heure de FIN -> on retire 1 h (convention : début d'heure)
    end = datetime.strptime(row["UTC Time at End of Hour"], "%m/%d/%Y %I:%M:%S %p")
    return hour_iso(end.replace(tzinfo=timezone.utc) - timedelta(hours=1))


def iter_rows(path: Path, start: datetime, end: datetime, bas: list[str] | None = None):
    """Lit le CSV et renvoie (period_utc, ligne) pour les zones et la période voulues."""
    wanted = set(bas or BA_CODES)
    start_iso, end_iso = hour_iso(start), hour_iso(end)
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("Balancing Authority") not in wanted:
                continue
            period = _period(row)
            if start_iso <= period <= end_iso:
                yield period, row


def row_to_events(period: str, row: dict, source: str = SOURCE) -> list[dict]:
    ba = row["Balancing Authority"]
    events = []
    for column, fuel in FUEL_COLUMNS.items():
        mw = _number(row.get(column))
        if mw is not None:  # colonne vide = filière absente de la zone
            events.append(generation_event(ba, period, fuel, mw, source))
    values = {field: _number(row.get(col)) for col, field in DEMAND_COLUMNS.items()}
    events.append(demand_event(ba, period, source, **values))
    return events
