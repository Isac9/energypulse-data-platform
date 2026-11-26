-- =====================================================================
-- EnergyPulse : schéma PostgreSQL
-- Exécuté automatiquement au premier démarrage du conteneur postgres.
--   staging : données nettoyées, une ligne par mesure (clé naturelle)
--   marts   : indicateurs prêts à l'emploi (API, tableaux de bord)
--   ops     : supervision (exécutions, santé des sources, SLA, incidents)
-- Les données brutes, elles, sont conservées en fichiers JSON (data/raw).
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS ops;

-- ---------- Référentiels ----------
CREATE TABLE IF NOT EXISTS staging.balancing_authority (
    ba          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    city        TEXT NOT NULL,
    latitude    NUMERIC(7,4) NOT NULL,
    longitude   NUMERIC(7,4) NOT NULL
);
INSERT INTO staging.balancing_authority VALUES
    ('CISO', 'California ISO',      'Fresno, CA',   36.74, -119.79),
    ('ERCO', 'ERCOT (Texas)',       'Abilene, TX',  32.45,  -99.73),
    ('PJM',  'PJM Interconnection', 'Columbus, OH', 39.96,  -83.00),
    ('NYIS', 'New York ISO',        'Albany, NY',   42.65,  -73.75)
ON CONFLICT (ba) DO NOTHING;

CREATE TABLE IF NOT EXISTS staging.fuel_type (
    fuel         TEXT PRIMARY KEY,
    label        TEXT NOT NULL,
    is_renewable BOOLEAN NOT NULL
);
INSERT INTO staging.fuel_type VALUES
    ('COL','Charbon',FALSE), ('NG','Gaz naturel',FALSE), ('NUC','Nucléaire',FALSE),
    ('OIL','Pétrole',FALSE), ('WAT','Hydraulique',TRUE), ('PS','Pompage-turbinage',FALSE),
    ('SUN','Solaire',TRUE), ('SNB','Solaire + batterie',TRUE), ('WND','Éolien',TRUE),
    ('WNB','Éolien + batterie',TRUE), ('BAT','Batteries',FALSE), ('GEO','Géothermie',TRUE),
    ('OES','Autre stockage',FALSE), ('UES','Stockage inconnu',FALSE),
    ('OTH','Autres',FALSE), ('UNK','Inconnu',FALSE)
ON CONFLICT (fuel) DO NOTHING;

-- ---------- Données (staging) ----------
-- La clé primaire est la clé NATURELLE de la mesure : recevoir deux fois la
-- même mesure ne crée jamais de doublon (INSERT ... ON CONFLICT DO UPDATE).
CREATE TABLE IF NOT EXISTS staging.generation (
    ba           TEXT        NOT NULL REFERENCES staging.balancing_authority,
    period_utc   TIMESTAMPTZ NOT NULL,
    fuel         TEXT        NOT NULL REFERENCES staging.fuel_type,
    mw           NUMERIC     NOT NULL,
    source       TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ba, period_utc, fuel)
);

CREATE TABLE IF NOT EXISTS staging.demand (
    ba                 TEXT        NOT NULL REFERENCES staging.balancing_authority,
    period_utc         TIMESTAMPTZ NOT NULL,
    demand_mw          NUMERIC,
    forecast_mw        NUMERIC,
    net_generation_mw  NUMERIC,
    interchange_mw     NUMERIC,
    source             TEXT        NOT NULL,
    event_id           TEXT        NOT NULL,
    ingested_at        TIMESTAMPTZ NOT NULL,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ba, period_utc)
);

CREATE TABLE IF NOT EXISTS staging.weather (
    ba                       TEXT        NOT NULL REFERENCES staging.balancing_authority,
    period_utc               TIMESTAMPTZ NOT NULL,
    temperature_c            NUMERIC,
    wind_speed_100m_kmh      NUMERIC,
    shortwave_radiation_wm2  NUMERIC,
    cloud_cover_pct          NUMERIC,
    source                   TEXT        NOT NULL,
    event_id                 TEXT        NOT NULL,
    ingested_at              TIMESTAMPTZ NOT NULL,
    loaded_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ba, period_utc)
);

-- ---------- Sortie du job Spark Structured Streaming ----------
-- Spark écrit en ajout (append) : chaque micro-batch ajoute la version la
-- plus récente d'une fenêtre. La vue marts.generation_mix_live garde la
-- dernière version de chaque (zone, heure).
CREATE TABLE IF NOT EXISTS marts.generation_mix_stream_log (
    ba               TEXT        NOT NULL,
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    total_mw         DOUBLE PRECISION,
    gross_mw         DOUBLE PRECISION,
    renewable_mw     DOUBLE PRECISION,
    solar_mw         DOUBLE PRECISION,
    wind_mw          DOUBLE PRECISION,
    hydro_mw         DOUBLE PRECISION,
    fossil_mw        DOUBLE PRECISION,
    nuclear_mw       DOUBLE PRECISION,
    renewable_share  DOUBLE PRECISION,
    event_count      BIGINT,
    batch_id         BIGINT,
    computed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stream_log ON marts.generation_mix_stream_log (ba, window_start, computed_at DESC);

CREATE OR REPLACE VIEW marts.generation_mix_live AS
SELECT DISTINCT ON (ba, window_start) *
FROM marts.generation_mix_stream_log
ORDER BY ba, window_start, computed_at DESC, batch_id DESC;

-- ---------- Supervision (ops) ----------
-- Une ligne par exécution d'ingestion (API, fichier, rejeu, backfill).
CREATE TABLE IF NOT EXISTS ops.ingestion_runs (
    run_id         BIGSERIAL PRIMARY KEY,
    source         TEXT        NOT NULL,
    dataset        TEXT        NOT NULL,
    mode           TEXT        NOT NULL,      -- live / backfill / replay
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    status         TEXT        NOT NULL DEFAULT 'running',  -- running / success / failed / skipped
    rows_fetched   INTEGER     DEFAULT 0,
    rows_valid     INTEGER     DEFAULT 0,
    rows_rejected  INTEGER     DEFAULT 0,
    raw_file       TEXT,
    error          TEXT
);

-- État de chaque source pour le disjoncteur (circuit breaker).
CREATE TABLE IF NOT EXISTS ops.source_health (
    source                TEXT PRIMARY KEY,
    state                 TEXT        NOT NULL DEFAULT 'closed', -- closed=OK / open=coupé / half_open=test
    consecutive_failures  INTEGER     NOT NULL DEFAULT 0,
    last_success_at       TIMESTAMPTZ,
    last_failure_at       TIMESTAMPTZ,
    last_error            TEXT,
    opened_until          TIMESTAMPTZ
);

-- Messages rejetés (copie du topic energy.dlq).
CREATE TABLE IF NOT EXISTS ops.dead_letters (
    id            BIGSERIAL PRIMARY KEY,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    origin_topic  TEXT,
    reasons       TEXT[]      NOT NULL,
    payload       TEXT        NOT NULL
);

-- Battement de cœur du consumer : dernier message traité par topic.
CREATE TABLE IF NOT EXISTS ops.consumer_heartbeat (
    topic             TEXT PRIMARY KEY,
    last_message_at   TIMESTAMPTZ NOT NULL,
    last_offset       BIGINT,
    messages_total    BIGINT NOT NULL DEFAULT 0
);

-- Résultat de chaque contrôle qualité / SLA.
CREATE TABLE IF NOT EXISTS ops.sla_checks (
    id          BIGSERIAL PRIMARY KEY,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    check_name  TEXT        NOT NULL,   -- freshness / completeness / anomaly / dlq / pipeline
    target      TEXT        NOT NULL,   -- ex. 'generation:CISO'
    status      TEXT        NOT NULL,   -- ok / warning / breach
    measured    NUMERIC,
    threshold   NUMERIC,
    details     TEXT
);
CREATE INDEX IF NOT EXISTS ix_sla_checks ON ops.sla_checks (check_name, target, checked_at DESC);

-- Incidents : ouverts automatiquement, fermés automatiquement quand le
-- contrôle redevient vert. Un seul incident ouvert par (contrôle, cible).
CREATE TABLE IF NOT EXISTS ops.incidents (
    incident_id   BIGSERIAL PRIMARY KEY,
    check_name    TEXT        NOT NULL,
    target        TEXT        NOT NULL,
    severity      TEXT        NOT NULL,   -- warning / critical
    opened_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at   TIMESTAMPTZ,
    occurrences   INTEGER     NOT NULL DEFAULT 1,
    details       TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_open_incident
    ON ops.incidents (check_name, target) WHERE resolved_at IS NULL;

-- Anomalies de production détectées.
CREATE TABLE IF NOT EXISTS ops.anomalies (
    ba            TEXT        NOT NULL,
    fuel_group    TEXT        NOT NULL,   -- renewable / total / solar / wind
    period_utc    TIMESTAMPTZ NOT NULL,
    observed_mw   NUMERIC,
    expected_mw   NUMERIC,
    deviation_pct NUMERIC,
    zscore        NUMERIC,
    detected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ba, fuel_group, period_utc)
);
