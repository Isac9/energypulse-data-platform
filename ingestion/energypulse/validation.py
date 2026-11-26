"""Contrôle des événements AVANT leur entrée dans le pipeline.

validate(event) renvoie une liste d'erreurs. Liste vide = événement valide.
Un événement invalide n'est pas jeté : il part dans le topic « energy.dlq »
(dead letter queue) avec la raison du rejet, pour être analysé.
"""
from datetime import datetime, timedelta, timezone

from .config import BA_CODES, FUEL_TYPES

# Bornes de plausibilité. Une valeur hors bornes est presque toujours une
# erreur de la source (unité fausse, valeur sentinelle, champ décalé).
MAX_MW = 250_000          # aucune zone américaine ne dépasse ~200 GW
MIN_MW_STORAGE = -50_000  # stockage et catégories « fourre-tout » peuvent être négatifs
MIN_MW_PRODUCTION = -1_000  # petite consommation nocturne tolérée (ex. solaire)
# OTH et UNK ajoutés après analyse des rejets : CISO publie des valeurs
# « Autres » négatives (jusqu'à -10 GW) pendant des centaines d'heures.
# Ce n'est pas une corruption : ces catégories incluent du stockage net.
STORAGE_FUELS = {"BAT", "PS", "OES", "UES", "OTH", "UNK"}

WEATHER_BOUNDS = {
    "temperature_c": (-60, 60),
    "wind_speed_100m_kmh": (0, 400),
    "shortwave_radiation_wm2": (0, 1500),
    "cloud_cover_pct": (0, 100),
}


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_period(event: dict, errors: list) -> None:
    raw = event.get("period_utc")
    try:
        period = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        errors.append(f"period_utc illisible : {raw!r}")
        return
    if period.tzinfo is None:
        errors.append("period_utc sans fuseau horaire")
        return
    # Une prévision météo peut être dans le futur, pas une mesure électrique.
    horizon = timedelta(days=16) if event.get("dataset") == "weather" else timedelta(hours=2)
    if period > datetime.now(timezone.utc) + horizon:
        errors.append(f"period_utc dans le futur : {raw}")
    if period.year < 2015:
        errors.append(f"period_utc trop ancien : {raw}")


def validate(event: dict) -> list[str]:
    errors: list[str] = []
    if not isinstance(event, dict):
        return ["l'événement n'est pas un objet JSON"]

    for field in ("event_id", "dataset", "source", "ba", "period_utc"):
        if not event.get(field):
            errors.append(f"champ obligatoire manquant : {field}")
    if errors:
        return errors

    if event["ba"] not in BA_CODES:
        errors.append(f"zone inconnue : {event['ba']}")
    _check_period(event, errors)

    dataset = event["dataset"]
    if dataset == "generation":
        fuel, mw = event.get("fuel"), event.get("mw")
        if fuel not in FUEL_TYPES:
            errors.append(f"filière inconnue : {fuel!r}")
        if not _is_number(mw):
            errors.append(f"mw non numérique : {mw!r}")
        else:
            low = MIN_MW_STORAGE if fuel in STORAGE_FUELS else MIN_MW_PRODUCTION
            if not low <= mw <= MAX_MW:
                errors.append(f"mw hors bornes [{low}, {MAX_MW}] : {mw}")
    elif dataset == "demand":
        values = {k: event.get(k) for k in ("demand_mw", "forecast_mw",
                                            "net_generation_mw", "interchange_mw")}
        if all(v is None for v in values.values()):
            errors.append("aucune valeur de demande renseignée")
        for key, value in values.items():
            if value is None:
                continue
            if not _is_number(value):
                errors.append(f"{key} non numérique : {value!r}")
            elif key == "interchange_mw":
                if abs(value) > MAX_MW:
                    errors.append(f"{key} hors bornes : {value}")
            elif not 0 <= value <= MAX_MW:
                errors.append(f"{key} hors bornes [0, {MAX_MW}] : {value}")
    elif dataset == "weather":
        for key, (low, high) in WEATHER_BOUNDS.items():
            value = event.get(key)
            if value is None:
                continue
            if not _is_number(value):
                errors.append(f"{key} non numérique : {value!r}")
            elif not low <= value <= high:
                errors.append(f"{key} hors bornes [{low}, {high}] : {value}")
    else:
        errors.append(f"dataset inconnu : {dataset!r}")
    return errors
