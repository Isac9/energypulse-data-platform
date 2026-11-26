"""Appels HTTP avec relances (retry) et attente exponentielle (backoff).

Les API publiques tombent parfois (504, 429 « trop de requêtes », coupure
réseau). On réessaie quelques fois en attendant de plus en plus longtemps :
2 s, puis 4 s, puis 8 s. Si ça échoue toujours, on lève SourceUnavailable :
c'est l'appelant qui décide quoi faire (journaliser, ouvrir un incident...).
"""
import logging
import time

import requests

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class SourceUnavailable(Exception):
    """La source n'a pas répondu correctement après toutes les relances."""


class SourceRejected(Exception):
    """La source a répondu mais refuse la requête (ex. clé API invalide) :
    inutile de réessayer."""


def get_json(url: str, params: dict, timeout: int = 60, attempts: int = 4,
             base_delay: float = 2.0, session: requests.Session | None = None) -> dict:
    http = session or requests
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            response = http.get(url, params=params, timeout=timeout)
            if response.status_code in RETRYABLE_STATUS:
                last_error = f"HTTP {response.status_code}"
            elif response.status_code >= 400:
                raise SourceRejected(f"HTTP {response.status_code} : {response.text[:300]}")
            else:
                return response.json()
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = f"{type(exc).__name__} : {exc}"
        except ValueError as exc:  # réponse qui n'est pas du JSON
            last_error = f"réponse non JSON : {exc}"

        if attempt < attempts:
            delay = base_delay * 2 ** (attempt - 1)
            log.warning("Appel %s échoué (%s), essai %d/%d, nouvel essai dans %.0f s",
                        url, last_error, attempt, attempts, delay)
            time.sleep(delay)
    raise SourceUnavailable(f"{url} indisponible après {attempts} essais : {last_error}")
