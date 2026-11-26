# Runbook : que faire quand ça casse

Chaque incident est visible dans `ops.incidents`, `GET /incidents` et le tableau de bord. Il se **ferme tout seul** dès que le contrôle redevient vert : il n'y a rien à fermer à la main.

## Incident `freshness` (données en retard)

**Signal** : `freshness generation:CISO breach, retard 9.2 h`.

1. La source répond-elle ? → `GET /pipeline` : regarde `sources` (état du disjoncteur) et `last_runs` (dernier statut, erreur).
2. Statut `skipped` avec « EIA_API_KEY absente » → ajoute la clé dans `.env`, puis `docker compose up -d airflow-scheduler`.
3. Statut `failed` → lis `error`. Avec un 5xx ou un timeout, la source est en panne : le disjoncteur s'ouvrira et le DAG réessaiera tout seul. Avec un 403, la clé est invalide.
4. Le consumer est-il vivant ? → `ops.consumer_heartbeat`, puis `docker logs ep-consumer`.
5. En attendant, si l'API reste en panne : `energypulse_backfill` avec les fichiers EIA-930.

## Incident `completeness` (heures manquantes)

**Signal** : `completeness demand:PJM breach, 42/48 heures`.

1. Le trou vient-il de la source ? Compare avec le fichier EIA-930 : `backfill-file` sur le jour concerné.
2. Relance le DAG `energypulse_backfill` avec `{"start": "AAAA-MM-JJ", "end": "AAAA-MM-JJ"}`. C'est idempotent : aucun doublon possible.
3. Si le trou existe aussi dans le fichier officiel, c'est un trou de la source (exemple réel : PJM le 1er novembre 2026, jour du changement d'heure). Documente-le : l'incident se fermera quand la fenêtre de 48 h l'aura dépassé.

## Incident `pipeline` (source en panne, disjoncteur ouvert)

**Signal** : `pipeline open_meteo breach, disjoncteur ouvert`.

1. Rien à faire dans l'immédiat : la source n'est plus appelée pendant `CIRCUIT_COOLDOWN_MINUTES` (30 min). Cela évite de la surcharger et de remplir les logs.
2. Une fois ce délai passé, la prochaine exécution fait un appel test (`half_open`) : s'il réussit, le disjoncteur se referme, sinon il se rouvre.
3. Pour forcer un nouvel essai : `UPDATE ops.source_health SET opened_until = now() WHERE source = '...'`.

## Incident `dlq` (messages rejetés)

**Signal** : `dlq energy.dlq breach, 304 messages`.

1. Regroupe les raisons de rejet :
   ```sql
   SELECT reasons[1], COUNT(*) FROM ops.dead_letters
   WHERE received_at > now() - interval '1 hour' GROUP BY 1 ORDER BY 2 DESC;
   ```
2. Une seule raison très fréquente : c'est souvent la source qui a changé de format, ou une règle trop stricte (exemple réel : les valeurs « Autres » négatives de CISO).
3. Corrige le parseur ou la règle dans `validation.py`, **redémarre le consumer** (`docker compose restart consumer`, car il garde l'ancien code en mémoire), puis recharge la période avec le backfill.

## Incident `anomaly`

**Signal** : `anomaly production:ERCO warning, 3 heure(s) anormale(s)`.

1. `GET /anomalies?ba=ERCO` : regarde `fuel_group`, `deviation_pct` et `zscore`.
2. `solar_vs_weather` : le soleil brillait, mais la production solaire était inférieure à la moitié de l'attendu. C'est un problème réel (panne, effacement) ou une donnée fausse.
3. `total` ou `renewable` : baisse d'au moins 30 % et z-score ≤ −3 par rapport à la même heure des 14 jours précédents.

## Le consumer a planté

Rien n'est perdu : les offsets Kafka ne sont validés qu'après l'écriture en base.

```powershell
docker compose ps consumer
docker logs ep-consumer --tail 50
docker compose up -d consumer
docker exec ep-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group energypulse-loader
```

La colonne `LAG` doit redescendre à 0.

## Spark a planté ou a été arrêté

`docker compose up -d spark` suffit : il repart de son checkpoint et ne traite que les nouveaux messages. Pour **tout recalculer** depuis Kafka (après un changement de logique) :

```powershell
docker compose rm -sf spark
docker volume rm energypulse_spark-checkpoints
docker exec ep-postgres psql -U energypulse -c "TRUNCATE marts.generation_mix_stream_log"
docker compose up -d spark
```

## Recalculer depuis les fichiers bruts (sans rappeler l'API)

```powershell
docker compose run --rm app python -m energypulse.pipeline reprocess-raw --source open_meteo --day 2026-11-26
```
