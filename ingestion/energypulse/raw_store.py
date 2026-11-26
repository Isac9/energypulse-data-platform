"""Historisation des données brutes (« bronze » / landing zone).

Chaque réponse d'API est enregistrée TELLE QUELLE, avant toute
transformation, dans data/raw/<source>/<AAAA-MM-JJ>/<heure>_<nom>.json.gz.
Si un bug de transformation est découvert plus tard, on peut tout
recalculer à partir de ces fichiers sans rappeler les API.
"""
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

from . import config


def save(source: str, name: str, payload, base_dir: str | None = None) -> str:
    now = datetime.now(timezone.utc)
    folder = Path(base_dir or config.RAW_DATA_DIR) / source / now.strftime("%Y-%m-%d")
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{now.strftime('%H%M%S')}_{name}.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump({"saved_at": now.isoformat(), "source": source, "payload": payload}, f)
    return str(path)


def load(path: str) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def list_files(source: str, base_dir: str | None = None) -> list[Path]:
    return sorted((Path(base_dir or config.RAW_DATA_DIR) / source).glob("*/*.json.gz"))
