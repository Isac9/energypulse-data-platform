# Architecture d'EnergyPulse

## 1. Flux de données

| Étape | Composant | Entrée | Sortie | Garantie |
|---|---|---|---|---|
| Collecte live | `pipeline.py live-eia` / `live-weather` (DAG horaire) | API EIA, Open-Meteo | fichiers bruts + topics Kafka | relances, disjoncteur, validation |
| Collecte historique | `pipeline.py backfill-file` / `backfill-weather` (DAG manuel) | fichiers EIA-930, archive Open-Meteo | `staging.*` directement | idempotent (UPSERT) |
| Rejeu | `pipeline.py replay` | fichiers EIA-930 + archive météo | topics Kafka, à vitesse ×N | vraies dates conservées |
| Transport | Kafka 3.9 (KRaft) | événements JSON, clé = zone | 4 topics × 3 partitions, 7 jours de rétention | `acks=all`, producteur idempotent |
| Chargement | `consumer.py` | topics data + DLQ | `staging.*`, `ops.dead_letters`, `ops.consumer_heartbeat` | au moins une fois + UPSERT = pas de perte, pas de doublon |
| Streaming | Spark Structured Streaming | `energy.generation` | `marts.generation_mix_stream_log` → vue `generation_mix_live` | checkpoint, watermark 6 h, dédoublonnage |
| Indicateurs | DAG `energypulse_quality_sla` | `staging.*` | `marts.energy_hourly` (vue matérialisée), `energy_daily`, `weather_correlation` | REFRESH CONCURRENTLY |
| Supervision | DAG `energypulse_quality_sla` | `staging.*`, `ops.*` | `ops.sla_checks`, `ops.incidents`, `ops.anomalies` | un seul incident ouvert par (contrôle, cible) |
| Restitution | FastAPI | `marts.*`, `ops.*` | JSON + tableau de bord | lecture seule |

## 2. Format des événements

Tous les événements partagent : `event_id` (SHA-1 de la clé naturelle), `dataset`, `source`, `ba`, `period_utc` (début d'heure, UTC, ISO 8601) et `ingested_at`.

```json
{"event_id": "c280e5…", "dataset": "generation", "source": "eia_api", "ba": "CISO",
 "period_utc": "2026-11-20T18:00:00+00:00", "fuel": "SUN", "mw": 8553, "ingested_at": "2026-11-26T09:04:03+00:00"}
```

**Convention horaire** : l'EIA publie en « heure de fin » (la valeur de 01:00 couvre 00:00 → 01:00). Le projet stocke le **début** d'heure. Les deux sources EIA (API et fichiers) sont converties de la même façon, ce qui permet de les mélanger sans décalage.

## 3. Modèle de données PostgreSQL

- `staging.generation (ba, period_utc, fuel)` : production par filière
- `staging.demand (ba, period_utc)` : demande, prévision, production nette, échanges
- `staging.weather (ba, period_utc)` : température, vent à 100 m, rayonnement, nébulosité
- `staging.balancing_authority`, `staging.fuel_type` : référentiels
- `marts.energy_hourly` : une ligne par zone et par heure, avec le mix, la part renouvelable, la demande, l'erreur de prévision et la météo
- `marts.energy_daily`, `marts.weather_correlation`, `marts.generation_mix_live`
- `ops.ingestion_runs`, `ops.source_health`, `ops.dead_letters`, `ops.consumer_heartbeat`, `ops.sla_checks`, `ops.incidents`, `ops.anomalies`

Les données brutes ne sont pas en base : elles sont conservées en `data/raw/<source>/<jour>/*.json.gz`, ce qui permet de tout recalculer sans rappeler les API (`reprocess-raw`).

## 4. Choix techniques

| Choix | Pourquoi | Alternative écartée |
|---|---|---|
| Kafka KRaft, 1 broker | gratuit, local, pas de ZooKeeper | cluster managé (payant) |
| Clé de message = zone | ordre garanti par zone | clé aléatoire (pas d'ordre) |
| Au moins une fois + UPSERT | simple et robuste | transactions Kafka exactly-once (complexe, inutile avec une base idempotente) |
| Spark en `local[2]` | démontre l'API Structured Streaming sans cluster | cluster Spark standalone (gourmand en RAM) |
| Sortie Spark en ajout + vue « dernière version » | pas d'UPSERT JDBC à écrire | `foreachBatch` avec MERGE (plus de code) |
| Backfill sans Kafka | l'historique en masse n'a pas besoin d'un bus temps réel | tout faire passer par Kafka (lent, inutile) |
| Vue matérialisée | indicateurs rapides pour l'API, recalcul horaire | dbt (déjà présenté dans d'autres projets) |
| Airflow LocalExecutor | suffisant pour 3 DAGs | CeleryExecutor + Redis |

## 5. Ports

| Service | Port hôte |
|---|---|
| PostgreSQL | 5440 |
| Kafka (externe) | 9094 |
| Kafka UI | 8095 |
| API | 8030 |
| Airflow | 8090 |
| Spark UI | 4040 |
