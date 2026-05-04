-- models/marts/weekly_plant_report.sql
-- Rapport hebdomadaire par usine
-- Matérialisé en TABLE (stocké, performances optimales pour BI)

{{ config(materialized='table') }}

WITH weekly_events AS (
    SELECT
        DATE_TRUNC('week', event_ts)::DATE  AS week_start,
        plant_sk,
        COUNT(*)                            AS total_events,
        COUNT(*) FILTER (
            WHERE threshold_breach = TRUE
        )                                   AS total_alarms,
        COUNT(*) FILTER (
            WHERE anomaly_score > 0.8
        )                                   AS high_anomalies,
        AVG(value_numeric)                  AS avg_value,
        AVG(quality)                        AS avg_quality
    FROM {{ ref('stg_iot_events') }}
    WHERE event_ts >= '2024-01-01'
    GROUP BY 1, 2
),
with_plant AS (
    SELECT
        w.week_start,
        p.plant_id,
        p.plant_name,
        p.country_code,
        w.total_events,
        w.total_alarms,
        w.high_anomalies,
        ROUND(w.avg_value::NUMERIC, 2)      AS avg_sensor_value,
        ROUND(w.avg_quality::NUMERIC, 1)    AS avg_quality,
        ROUND(
            w.total_alarms::NUMERIC
            / NULLIF(w.total_events, 0) * 100, 2
        )                                   AS alarm_rate_pct,
        -- WoW trend avec window function
        LAG(w.total_alarms) OVER (
            PARTITION BY w.plant_sk
            ORDER BY w.week_start
        )                                   AS prev_week_alarms,
        CASE
            WHEN ROUND(
                w.total_alarms::NUMERIC
                / NULLIF(w.total_events, 0) * 100, 2
            ) < 1.0 THEN 'COMPLIANT'
            ELSE 'NON-COMPLIANT'
        END                                 AS sla_status
    FROM weekly_events w
    JOIN {{ source('factory', 'dim_plant') }} p
        ON p.plant_sk = w.plant_sk
)
SELECT
    week_start,
    plant_id,
    plant_name,
    country_code,
    total_events,
    total_alarms,
    high_anomalies,
    avg_sensor_value,
    avg_quality,
    alarm_rate_pct,
    ROUND(
        (total_alarms - COALESCE(prev_week_alarms, total_alarms))::NUMERIC
        / NULLIF(prev_week_alarms, 0) * 100, 1
    )                                       AS alarm_wow_delta_pct,
    sla_status
FROM with_plant
ORDER BY week_start DESC, total_alarms DESC