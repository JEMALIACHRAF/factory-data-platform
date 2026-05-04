-- =============================================================================
-- Factory DW – DDL PostgreSQL (dev/test)
-- Schéma identique à Redshift, syntaxe adaptée :
--   • DISTKEY / SORTKEY / ENCODE supprimés (non supportés PostgreSQL)
--   • BIGINT IDENTITY → BIGSERIAL
--   • VARCHAR max 65535 → TEXT
--   • Commentaires indiquent l'équivalent Redshift
-- En production → utiliser 01_tables_redshift.sql sur Redshift
-- =============================================================================

-- ── Schemas ───────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS factory;
CREATE SCHEMA IF NOT EXISTS reporting;
CREATE SCHEMA IF NOT EXISTS staging;

SET search_path = factory, public;

-- =============================================================================
-- DIMENSIONS (équivalent DISTSTYLE ALL sur Redshift)
-- =============================================================================

CREATE TABLE IF NOT EXISTS factory.dim_device (
    device_sk       BIGSERIAL PRIMARY KEY,      -- Redshift: BIGINT IDENTITY(1,1)
    device_id       VARCHAR(100) NOT NULL,
    device_type     VARCHAR(50),                -- SENSOR | PLC | HMI
    asset_name      VARCHAR(200),
    plant_id        VARCHAR(50)  NOT NULL,
    line_id         VARCHAR(50),
    location        VARCHAR(200),
    criticality     VARCHAR(20),                -- HIGH | MEDIUM | LOW
    manufacturer    VARCHAR(100),
    model           VARCHAR(100),
    install_date    DATE,
    sla_minutes     INTEGER,
    is_active       BOOLEAN DEFAULT TRUE,
    valid_from      TIMESTAMP,
    valid_to        TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Redshift: DISTSTYLE ALL | SORTKEY (plant_id, device_id)
CREATE INDEX idx_dim_device_id    ON factory.dim_device(device_id);
CREATE INDEX idx_dim_device_plant ON factory.dim_device(plant_id);


CREATE TABLE IF NOT EXISTS factory.dim_plant (
    plant_sk              BIGSERIAL PRIMARY KEY,
    plant_id              VARCHAR(50) NOT NULL UNIQUE,
    plant_name            VARCHAR(200),
    country_code          VARCHAR(3),
    region                VARCHAR(100),
    timezone              VARCHAR(50),
    shift_morning_start   TIME,
    shift_afternoon_start TIME,
    shift_night_start     TIME,
    capacity_units        INTEGER,
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Redshift: DISTSTYLE ALL | SORTKEY (plant_id)


CREATE TABLE IF NOT EXISTS factory.dim_date (
    date_sk         INTEGER PRIMARY KEY,        -- format YYYYMMDD
    full_date       DATE NOT NULL,
    year            SMALLINT,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(20),
    week_of_year    SMALLINT,
    day_of_month    SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN,
    fiscal_period   VARCHAR(10)
);
-- Redshift: DISTSTYLE ALL | SORTKEY (date_sk)


-- =============================================================================
-- FACTS (équivalent DISTKEY sur Redshift)
-- =============================================================================

CREATE TABLE IF NOT EXISTS factory.fact_iot_events (
    event_sk            BIGSERIAL PRIMARY KEY,  -- Redshift: BIGINT IDENTITY(1,1)
    event_id            VARCHAR(100),
    device_sk           BIGINT REFERENCES factory.dim_device(device_sk),
    plant_sk            BIGINT REFERENCES factory.dim_plant(plant_sk),
    date_sk             INTEGER REFERENCES factory.dim_date(date_sk),
    event_ts            TIMESTAMP NOT NULL,
    event_name          VARCHAR(100),
    value_numeric       DOUBLE PRECISION,
    value_string        VARCHAR(500),
    unit                VARCHAR(50),
    quality             SMALLINT,
    alert_threshold     DOUBLE PRECISION,
    threshold_breach    BOOLEAN,
    anomaly_score       DOUBLE PRECISION,
    avg_value_1min      DOUBLE PRECISION,
    stddev_value_1min   DOUBLE PRECISION,
    processed_at        TIMESTAMP,
    year                SMALLINT,
    month               SMALLINT
);
-- Redshift: DISTKEY(device_sk) | COMPOUND SORTKEY(event_ts, plant_sk, event_name)
CREATE INDEX idx_fact_iot_ts        ON factory.fact_iot_events(event_ts);
CREATE INDEX idx_fact_iot_device    ON factory.fact_iot_events(device_sk);
CREATE INDEX idx_fact_iot_plant     ON factory.fact_iot_events(plant_sk);
CREATE INDEX idx_fact_iot_breach    ON factory.fact_iot_events(threshold_breach);


CREATE TABLE IF NOT EXISTS factory.fact_machine_logs (
    log_sk              BIGSERIAL PRIMARY KEY,
    event_id            VARCHAR(100) NOT NULL,
    machine_id          VARCHAR(100) NOT NULL,
    device_sk           BIGINT REFERENCES factory.dim_device(device_sk),
    plant_sk            BIGINT REFERENCES factory.dim_plant(plant_sk),
    date_sk             INTEGER REFERENCES factory.dim_date(date_sk),
    event_ts            TIMESTAMP NOT NULL,
    event_type          VARCHAR(50),
    severity            VARCHAR(20),
    error_code          VARCHAR(50),
    temperature_c       REAL,
    vibration_hz        REAL,
    pressure_bar        REAL,
    cycle_count         BIGINT,
    operator_id         VARCHAR(100),
    shift               VARCHAR(20),
    is_alarm            BOOLEAN,
    is_critical         BOOLEAN,
    resolution_minutes  INTEGER,
    processed_at        TIMESTAMP,
    year                SMALLINT,
    month               SMALLINT
);
-- Redshift: DISTKEY(machine_id) | COMPOUND SORTKEY(event_ts, plant_sk, event_type, severity)
CREATE INDEX idx_logs_ts       ON factory.fact_machine_logs(event_ts);
CREATE INDEX idx_logs_machine  ON factory.fact_machine_logs(machine_id);
CREATE INDEX idx_logs_alarm    ON factory.fact_machine_logs(is_alarm);
CREATE INDEX idx_logs_severity ON factory.fact_machine_logs(severity);


-- =============================================================================
-- STAGING (pour le pattern insert → upsert)
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_iot_events
    (LIKE factory.fact_iot_events INCLUDING DEFAULTS);

CREATE TABLE IF NOT EXISTS staging.stg_machine_logs
    (LIKE factory.fact_machine_logs INCLUDING DEFAULTS);


-- =============================================================================
-- VUE REPORTING (remplace la Materialized View Redshift)
-- PostgreSQL supporte les MATERIALIZED VIEW nativement
-- =============================================================================

CREATE MATERIALIZED VIEW reporting.mv_hourly_plant_kpis AS
SELECT
    DATE_TRUNC('hour', e.event_ts)              AS event_hour,
    p.plant_id,
    p.plant_name,
    p.country_code,
    p.region,
    COUNT(*)                                    AS total_events,
    COUNT(*) FILTER (WHERE e.threshold_breach)  AS alarm_count,
    COUNT(*) FILTER (WHERE e.anomaly_score > 0.8) AS high_anomaly_count,
    AVG(e.avg_value_1min)                       AS avg_sensor_value,
    AVG(e.quality)                              AS avg_quality,
    COUNT(*) FILTER (WHERE e.quality < 60)      AS low_quality_events,
    ROUND(
        COUNT(*) FILTER (WHERE e.threshold_breach)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                           AS alarm_rate_pct
FROM factory.fact_iot_events e
JOIN factory.dim_plant p ON p.plant_sk = e.plant_sk
GROUP BY 1, 2, 3, 4, 5;

CREATE INDEX idx_mv_hour  ON reporting.mv_hourly_plant_kpis(event_hour);
CREATE INDEX idx_mv_plant ON reporting.mv_hourly_plant_kpis(plant_id);


-- =============================================================================
-- DONNEES DE TEST – pour valider le schéma immédiatement
-- =============================================================================

INSERT INTO factory.dim_plant VALUES
(1, 'LYON-01',  'Usine Lyon Nord',   'FRA', 'EMEA', 'Europe/Paris', '06:00', '14:00', '22:00', 500, NOW()),
(2, 'PARIS-01', 'Usine Paris Sud',   'FRA', 'EMEA', 'Europe/Paris', '06:00', '14:00', '22:00', 300, NOW()),
(3, 'BERLIN-01','Usine Berlin Est',  'DEU', 'EMEA', 'Europe/Berlin','06:00', '14:00', '22:00', 800, NOW());

INSERT INTO factory.dim_device VALUES
(1, 'DEVICE-001', 'SENSOR', 'Capteur Température L1', 'LYON-01',   'LINE-A', 'Hall A', 'HIGH',   'Siemens', 'S7-1200', '2022-01-15', 5,  TRUE, NOW(), NULL, NOW()),
(2, 'DEVICE-002', 'PLC',    'Automate Ligne 2',       'LYON-01',   'LINE-B', 'Hall B', 'HIGH',   'ABB',     'AC500',   '2021-06-01', 10, TRUE, NOW(), NULL, NOW()),
(3, 'DEVICE-003', 'SENSOR', 'Capteur Pression P1',    'PARIS-01',  'LINE-A', 'Hall C', 'MEDIUM', 'Bosch',   'BMP280',  '2023-03-20', 15, TRUE, NOW(), NULL, NOW());

INSERT INTO factory.dim_date VALUES
(20240101, '2024-01-01', 2024, 1, 1,  'January', 1,  1, 1, 'Monday',    FALSE, TRUE,  'FY24-Q1'),
(20240515, '2024-05-15', 2024, 2, 5,  'May',     20, 15, 3, 'Wednesday', FALSE, FALSE, 'FY24-Q2'),
(20241201, '2024-12-01', 2024, 4, 12, 'December',48, 1,  7, 'Sunday',    TRUE,  FALSE, 'FY24-Q4');

INSERT INTO factory.fact_iot_events
    (event_id, device_sk, plant_sk, date_sk, event_ts, event_name,
     value_numeric, unit, quality, alert_threshold, threshold_breach,
     anomaly_score, year, month)
VALUES
('EVT-001', 1, 1, 20240515, '2024-05-15 08:00:00', 'TEMPERATURE', 85.3,  '°C',  95, 90.0,  FALSE, 0.1, 2024, 5),
('EVT-002', 1, 1, 20240515, '2024-05-15 08:05:00', 'TEMPERATURE', 92.7,  '°C',  90, 90.0,  TRUE,  0.8, 2024, 5),
('EVT-003', 2, 1, 20240515, '2024-05-15 08:10:00', 'VIBRATION',   1.2,   'Hz',  88, 2.0,   FALSE, 0.2, 2024, 5),
('EVT-004', 3, 2, 20240515, '2024-05-15 09:00:00', 'PRESSURE',    4.8,   'bar', 72, 5.0,   FALSE, 0.3, 2024, 5),
('EVT-005', 1, 1, 20240515, '2024-05-15 09:15:00', 'TEMPERATURE', 110.0, '°C',  85, 90.0,  TRUE,  0.9, 2024, 5);