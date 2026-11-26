"""Consumer Kafka -> PostgreSQL (couche staging).

Garantie de livraison : « au moins une fois » + écritures idempotentes.
  1. on lit un lot de messages ;
  2. on les écrit dans PostgreSQL (UPSERT) et on valide la transaction ;
  3. SEULEMENT ENSUITE on enregistre la position (offset) dans Kafka.
Si le consumer plante entre 2 et 3, il relira les mêmes messages au
redémarrage : l'UPSERT les réécrira sans créer de doublon. Résultat : aucune
perte et aucun doublon, même en cas de panne.

Lancement : python -m energypulse.consumer
"""
import json
import logging
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError

from . import config, db
from .validation import validate

log = logging.getLogger("energypulse.consumer")

DATA_TOPICS = {config.TOPIC_GENERATION: "generation", config.TOPIC_DEMAND: "demand",
               config.TOPIC_WEATHER: "weather"}
running = True


def _stop(signum, frame):
    global running
    log.info("Signal %s reçu : arrêt propre après le lot en cours", signum)
    running = False


def route(messages) -> tuple[dict, list, dict]:
    """Trie un lot : événements valides par dataset, rejets, stats par topic."""
    batches = {"generation": [], "demand": [], "weather": []}
    dead, stats = [], {}
    for msg in messages:
        topic, raw = msg.topic(), msg.value() or b""
        stat = stats.setdefault(topic, [0, -1])
        stat[0] += 1
        stat[1] = max(stat[1], msg.offset())
        text = raw.decode("utf-8", errors="replace")
        if topic == config.TOPIC_DLQ:
            try:
                body = json.loads(text)
                dead.append((body.get("origin_topic"), body.get("reasons", ["?"]), text))
            except ValueError:
                dead.append((topic, ["message DLQ illisible"], text))
            continue
        try:
            event = json.loads(text)
        except ValueError:
            dead.append((topic, ["JSON invalide"], text))
            continue
        # Défense en profondeur : on revalide même si le producteur l'a fait,
        # car n'importe qui peut écrire dans un topic.
        errors = validate(event)
        if not errors and event.get("dataset") != DATA_TOPICS[topic]:
            errors = [f"dataset {event.get('dataset')!r} reçu sur le topic {topic}"]
        if errors:
            dead.append((topic, errors, text))
        else:
            batches[DATA_TOPICS[topic]].append(event)
    return batches, dead, stats


def run() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s : %(message)s")
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": "energypulse-loader",
        "auto.offset.reset": "earliest",   # nouveau groupe : on lit depuis le début
        "enable.auto.commit": False,       # c'est NOUS qui validons les offsets
    })
    consumer.subscribe(list(DATA_TOPICS) + [config.TOPIC_DLQ])
    log.info("Consumer démarré, abonné à %s", list(DATA_TOPICS) + [config.TOPIC_DLQ])

    try:
        conn = None
        while running:
            messages = consumer.consume(num_messages=1000, timeout=1.0)
            messages = [m for m in messages if not _is_error(m)]
            if not messages:
                continue
            batches, dead, stats = route(messages)
            if conn is None or conn.closed:
                conn = _connect_with_retry()
            try:
                loaded = {ds: db.upsert_events(conn, ds, evs) for ds, evs in batches.items()}
                db.insert_dead_letters(conn, dead)
                for topic, (count, offset) in stats.items():
                    db.heartbeat(conn, topic, offset, count)
                conn.commit()
            except Exception:
                conn.rollback()
                log.exception("Écriture PostgreSQL impossible : offsets NON validés, "
                              "le lot sera relu au redémarrage")
                return 1  # Docker redémarre le conteneur (restart: unless-stopped)
            consumer.commit(asynchronous=False)
            log.info("Lot traité : %s, %d rejet(s)",
                     ", ".join(f"{k}={v}" for k, v in loaded.items() if v), len(dead))
    finally:
        consumer.close()
    return 0


def _is_error(msg) -> bool:
    if msg.error() is None:
        return False
    if msg.error().code() != KafkaError._PARTITION_EOF:
        log.error("Erreur Kafka : %s", msg.error())
    return True


def _connect_with_retry(attempts: int = 10):
    import psycopg2
    for i in range(1, attempts + 1):
        try:
            return psycopg2.connect(config.PG_DSN)
        except psycopg2.OperationalError as exc:
            log.warning("PostgreSQL injoignable (%s), essai %d/%d", exc, i, attempts)
            time.sleep(min(2 ** i, 30))
    raise RuntimeError("PostgreSQL toujours injoignable")


if __name__ == "__main__":
    sys.exit(run())
