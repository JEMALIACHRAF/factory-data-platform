-- models/staging/stg_iot_events.sql
-- Modèle dbt : nettoie et standardise les events IoT bruts
-- Matérialisé en VIEW (pas de stockage, recalculé à chaque query)

{{ config(materialized='view') }}

SELECT
    event_id,
    device_sk,
    plant_sk,
    date_sk,
    event_ts,
    event_name,
    value_numeric,
    unit,
    quality,
    alert_threshold,
    threshold_breach,
    anomaly_score,
    -- Nettoyage : qualité null → 0
    COALESCE(quality, 0)            AS quality_clean,
    -- Flag anomalie haute
    anomaly_score > 0.8             AS is_high_anomaly,
    -- Heure de l'event pour partitionnement
    DATE_TRUNC('hour', event_ts)    AS event_hour,
    processed_at

FROM {{ source('factory', 'fact_iot_events') }}

-- Filtre les events sans timestamp
WHERE event_ts IS NOT NULL