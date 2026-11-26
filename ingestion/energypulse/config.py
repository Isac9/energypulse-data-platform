"""Configuration centrale d'EnergyPulse.

Toutes les valeurs viennent des variables d'environnement (fichier .env lu par
Docker Compose). Les valeurs par défaut permettent de lancer les tests en local.
"""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class BalancingAuthority:
    code: str          # code EIA de la zone d'équilibrage (balancing authority)
    name: str
    city: str          # point météo représentatif
    latitude: float
    longitude: float


# Quatre réseaux américains aux profils très différents :
# solaire (Californie), éolien (Texas), nucléaire/charbon (PJM), hydro (New York).
BALANCING_AUTHORITIES = [
    BalancingAuthority("CISO", "California ISO", "Fresno, CA", 36.74, -119.79),
    BalancingAuthority("ERCO", "ERCOT (Texas)", "Abilene, TX", 32.45, -99.73),
    BalancingAuthority("PJM", "PJM Interconnection", "Columbus, OH", 39.96, -83.00),
    BalancingAuthority("NYIS", "New York ISO", "Albany, NY", 42.65, -73.75),
]
BA_CODES = [ba.code for ba in BALANCING_AUTHORITIES]
BA_BY_CODE = {ba.code: ba for ba in BALANCING_AUTHORITIES}

# Codes de filière EIA -> libellé lisible. Les mêmes codes sont utilisés
# quelle que soit la source (API ou fichiers publics EIA-930).
FUEL_TYPES = {
    "COL": "Charbon",
    "NG": "Gaz naturel",
    "NUC": "Nucléaire",
    "OIL": "Pétrole",
    "WAT": "Hydraulique",
    "PS": "Pompage-turbinage",
    "SUN": "Solaire",
    "SNB": "Solaire + batterie",
    "WND": "Éolien",
    "WNB": "Éolien + batterie",
    "BAT": "Batteries",
    "GEO": "Géothermie",
    "OES": "Autre stockage",
    "UES": "Stockage inconnu",
    "OTH": "Autres",
    "UNK": "Inconnu",
}
RENEWABLE_FUELS = {"WAT", "SUN", "SNB", "WND", "WNB", "GEO"}

# Topics Kafka
TOPIC_GENERATION = "energy.generation"
TOPIC_DEMAND = "energy.demand"
TOPIC_WEATHER = "weather.hourly"
TOPIC_DLQ = "energy.dlq"
ALL_TOPICS = [TOPIC_GENERATION, TOPIC_DEMAND, TOPIC_WEATHER, TOPIC_DLQ]


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9094")
EIA_API_KEY = env("EIA_API_KEY", "")
RAW_DATA_DIR = env("RAW_DATA_DIR", "./data/raw")

PG_DSN = (
    f"host={env('POSTGRES_HOST', 'localhost')} "
    f"port={env('POSTGRES_PORT', '5440')} "
    f"dbname={env('POSTGRES_DB', 'energypulse')} "
    f"user={env('POSTGRES_USER', 'energypulse')} "
    f"password={env('POSTGRES_PASSWORD', 'energypulse')}"
)

# Disjoncteur (circuit breaker) : après N échecs consécutifs, on arrête
# d'appeler la source pendant COOLDOWN minutes.
CIRCUIT_FAILURE_THRESHOLD = int(env("CIRCUIT_FAILURE_THRESHOLD", "3"))
CIRCUIT_COOLDOWN_MINUTES = int(env("CIRCUIT_COOLDOWN_MINUTES", "30"))
