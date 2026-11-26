"""Publication dans Kafka.

- acks=all + enable.idempotence : le broker confirme l'écriture et un
  message renvoyé après une coupure réseau n'est pas écrit deux fois.
- La clé du message est le code de la zone (ex. « CISO ») : tous les messages
  d'une zone vont dans la même partition, donc restent dans l'ordre.
- Les événements invalides partent dans le topic DLQ avec la raison du rejet.
"""
import json
import logging

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from . import config
from .validation import validate

log = logging.getLogger(__name__)

TOPIC_BY_DATASET = {
    "generation": config.TOPIC_GENERATION,
    "demand": config.TOPIC_DEMAND,
    "weather": config.TOPIC_WEATHER,
}


def ensure_topics(bootstrap: str | None = None, partitions: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap or config.KAFKA_BOOTSTRAP})
    existing = set(admin.list_topics(timeout=15).topics)
    missing = [NewTopic(t, num_partitions=partitions, replication_factor=1)
               for t in config.ALL_TOPICS if t not in existing]
    for topic, future in admin.create_topics(missing).items() if missing else []:
        future.result()
        log.info("Topic créé : %s", topic)


class EventPublisher:
    def __init__(self, bootstrap: str | None = None):
        self.producer = Producer({
            "bootstrap.servers": bootstrap or config.KAFKA_BOOTSTRAP,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 50,
        })
        self.delivery_errors = 0

    def _on_delivery(self, err, msg):
        if err is not None:
            self.delivery_errors += 1
            log.error("Échec de livraison vers %s : %s", msg.topic(), err)

    def _send(self, topic: str, key: str, value: dict) -> None:
        while True:
            try:
                self.producer.produce(topic, key=key, value=json.dumps(value).encode(),
                                      on_delivery=self._on_delivery)
                break
            except BufferError:  # file d'attente locale pleine : on vide un peu
                self.producer.poll(0.5)
        self.producer.poll(0)

    def publish(self, events: list[dict]) -> tuple[int, int]:
        """Valide puis publie. Renvoie (nb valides, nb rejetés)."""
        valid = rejected = 0
        for event in events:
            errors = validate(event)
            if errors:
                rejected += 1
                self._send(config.TOPIC_DLQ, str(event.get("ba", "unknown")),
                           {"reasons": errors, "origin_topic": TOPIC_BY_DATASET.get(event.get("dataset")),
                            "event": event})
            else:
                valid += 1
                self._send(TOPIC_BY_DATASET[event["dataset"]], event["ba"], event)
        return valid, rejected

    def send_raw(self, topic: str, key: str, raw: bytes) -> None:
        """Envoie un message brut SANS validation (utilisé pour simuler une
        source qui envoie des données corrompues)."""
        self.producer.produce(topic, key=key, value=raw, on_delivery=self._on_delivery)
        self.producer.poll(0)

    def flush(self, timeout: float = 60) -> None:
        remaining = self.producer.flush(timeout)
        if remaining or self.delivery_errors:
            raise RuntimeError(f"{remaining} message(s) non livrés, {self.delivery_errors} erreur(s) de livraison")
