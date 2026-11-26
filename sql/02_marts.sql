-- =====================================================================
-- Couche « marts » : indicateurs prêts à l'emploi.
-- Vue matérialisée = résultat d'une requête stocké sur disque, recalculé
-- à la demande (REFRESH) par le DAG Airflow energypulse_quality_sla.
-- =====================================================================
DROP MATERIALIZED VIEW IF EXISTS marts.energy_hourly CASCADE;
CREATE MATERIALIZED VIEW marts.energy_hourly AS
WITH gen AS (
    SELECT g.ba, g.period_utc,
           SUM(g.mw)                                                   AS total_mw,
           -- Production brute = somme des valeurs POSITIVES. Le stockage qui se
           -- recharge (valeurs négatives) ne doit pas gonfler la part renouvelable.
           SUM(GREATEST(g.mw, 0))                                      AS gross_mw,
           SUM(g.mw) FILTER (WHERE f.is_renewable)                     AS renewable_mw,
           SUM(g.mw) FILTER (WHERE g.fuel IN ('SUN','SNB'))            AS solar_mw,
           SUM(g.mw) FILTER (WHERE g.fuel IN ('WND','WNB'))            AS wind_mw,
           SUM(g.mw) FILTER (WHERE g.fuel = 'WAT')                     AS hydro_mw,
           SUM(g.mw) FILTER (WHERE g.fuel = 'NG')                      AS gas_mw,
           SUM(g.mw) FILTER (WHERE g.fuel = 'COL')                     AS coal_mw,
           SUM(g.mw) FILTER (WHERE g.fuel = 'NUC')                     AS nuclear_mw,
           COUNT(*)                                                    AS fuel_count
    FROM staging.generation g
    JOIN staging.fuel_type f USING (fuel)
    GROUP BY g.ba, g.period_utc
)
SELECT gen.ba,
       gen.period_utc,
       gen.total_mw, gen.gross_mw, gen.renewable_mw, gen.solar_mw, gen.wind_mw, gen.hydro_mw,
       gen.gas_mw, gen.coal_mw, gen.nuclear_mw, gen.fuel_count,
       ROUND(100 * gen.renewable_mw / NULLIF(gen.gross_mw, 0), 2)           AS renewable_share_pct,
       d.demand_mw, d.forecast_mw, d.interchange_mw,
       ROUND(100 * (d.demand_mw - d.forecast_mw) / NULLIF(d.forecast_mw, 0), 2) AS forecast_error_pct,
       w.temperature_c, w.wind_speed_100m_kmh, w.shortwave_radiation_wm2, w.cloud_cover_pct
FROM gen
LEFT JOIN staging.demand  d ON d.ba = gen.ba AND d.period_utc = gen.period_utc
LEFT JOIN staging.weather w ON w.ba = gen.ba AND w.period_utc = gen.period_utc;

-- Index unique obligatoire pour REFRESH ... CONCURRENTLY (lecture possible pendant le recalcul)
CREATE UNIQUE INDEX ux_energy_hourly ON marts.energy_hourly (ba, period_utc);

-- Synthèse journalière (jour UTC)
CREATE OR REPLACE VIEW marts.energy_daily AS
SELECT ba,
       (period_utc AT TIME ZONE 'UTC')::date                   AS day_utc,
       COUNT(*)                                                AS hours,
       ROUND(SUM(total_mw))                                    AS generation_mwh,
       ROUND(SUM(renewable_mw))                                AS renewable_mwh,
       ROUND(100 * SUM(renewable_mw) / NULLIF(SUM(gross_mw), 0), 2) AS renewable_share_pct,
       ROUND(SUM(solar_mw))                                    AS solar_mwh,
       ROUND(SUM(wind_mw))                                     AS wind_mwh,
       ROUND(MAX(demand_mw))                                   AS peak_demand_mw,
       ROUND(AVG(temperature_c), 1)                            AS avg_temperature_c,
       ROUND(AVG(wind_speed_100m_kmh), 1)                      AS avg_wind_kmh,
       ROUND(AVG(shortwave_radiation_wm2), 1)                  AS avg_radiation_wm2
FROM marts.energy_hourly
GROUP BY ba, (period_utc AT TIME ZONE 'UTC')::date;

-- Corrélation production renouvelable / météo, par zone (sur tout l'historique)
CREATE OR REPLACE VIEW marts.weather_correlation AS
SELECT ba,
       COUNT(*)                                                       AS hours,
       ROUND(CORR(solar_mw, shortwave_radiation_wm2)::numeric, 3)     AS corr_solar_radiation,
       ROUND(CORR(wind_mw, wind_speed_100m_kmh)::numeric, 3)          AS corr_wind_speed,
       ROUND(CORR(demand_mw, temperature_c)::numeric, 3)              AS corr_demand_temperature
FROM marts.energy_hourly
WHERE shortwave_radiation_wm2 IS NOT NULL
GROUP BY ba;
