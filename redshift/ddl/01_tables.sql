-- =============================================================================
-- Factory DW – DDL
-- Redshift best practices:
--   • DISTKEY on join column (device_id / plant_id)
--   • SORTKEY COMPOUND on most-used filter/order columns
--   • ENCODE for compression (AZ64 default for numbers, LZO for strings)
--   • Fact tables: KEY distribution | Dimension tables: ALL distribution
-- =============================================================================

-- ── Schema isolation ──────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS factory   AUTHORIZATION admin;
CREATE SCHEMA IF NOT EXISTS reporting AUTHORIZATION admin;
CREATE SCHEMA IF NOT EXISTS staging   AUTHORIZATION admin;

-- ── Search path ───────────────────────────────────────────────────────────────
SET search_path = factory, public;


-- =============================================================================
-- DIMENSION TABLES (DISTSTYLE ALL → replicated to every slice)
-- =============================================================================

CREATE TABLE IF NOT EXISTS factory.dim_device (
    device_sk       BIGINT IDENTITY(1,1)    ENCODE az64,
    device_id       VARCHAR(100) NOT NULL   ENCODE lzo,
    device_type     VARCHAR(50)             ENCODE bytedict,
    asset_name      VARCHAR(200)            ENCODE lzo,
    plant_id        VARCHAR(50) NOT NULL    ENCODE lzo,
    line_id         VARCHAR(50)             ENCODE lzo,
    location        VARCHAR(200)            ENCODE lzo,
    criticality     VARCHAR(20)             ENCODE bytedict, -- HIGH/MEDIUM/LOW
    manufacturer    VARCHAR(100)            ENCODE lzo,
    model           VARCHAR(100)            ENCODE lzo,
    install_date    DATE                    ENCODE az64,
    sla_minutes     INTEGER                 ENCODE az64,
    is_active       BOOLEAN DEFAULT TRUE    ENCODE raw,
    valid_from      TIMESTAMP               ENCODE az64,
    valid_to        TIMESTAMP               ENCODE az64,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ENCODE az64
)
DISTSTYLE ALL
SORTKEY (plant_id, device_id);


CREATE TABLE IF NOT EXISTS factory.dim_plant (
    plant_sk        BIGINT IDENTITY(1,1)    ENCODE az64,
    plant_id        VARCHAR(50) NOT NULL    ENCODE lzo,
    plant_name      VARCHAR(200)            ENCODE lzo,
    country_code    VARCHAR(3)              ENCODE bytedict,
    region          VARCHAR(100)            ENCODE lzo,
    timezone        VARCHAR(50)             ENCODE lzo,
    shift_morning_start   TIME             ENCODE raw,
    shift_afternoon_start TIME             ENCODE raw,
    shift_night_start     TIME             ENCODE raw,
    capacity_units  INTEGER                 ENCODE az64,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ENCODE az64
)
DISTSTYLE ALL
SORTKEY (plant_id);


CREATE TABLE IF NOT EXISTS factory.dim_date (
    date_sk         INTEGER NOT NULL        ENCODE raw,    -- YYYYMMDD int
    full_date       DATE NOT NULL           ENCODE az64,
    year            SMALLINT                ENCODE az64,
    quarter         SMALLINT                ENCODE az64,
    month           SMALLINT                ENCODE az64,
    month_name      VARCHAR(20)             ENCODE bytedict,
    week_of_year    SMALLINT                ENCODE az64,
    day_of_month    SMALLINT                ENCODE az64,
    day_of_week     SMALLINT                ENCODE az64,
    day_name        VARCHAR(20)             ENCODE bytedict,
    is_weekend      BOOLEAN                 ENCODE raw,
    is_holiday      BOOLEAN                 ENCODE raw,
    fiscal_period   VARCHAR(10)             ENCODE bytedict
)
DISTSTYLE ALL
SORTKEY (date_sk);


-- =============================================================================
-- FACT TABLES (DISTKEY on most-joined column)
-- =============================================================================

-- Core IoT event fact (high volume – 100M+ rows/day)
CREATE TABLE IF NOT EXISTS factory.fact_iot_events (
    event_sk            BIGINT IDENTITY(1,1) ENCODE az64,
    event_id            VARCHAR(100)         ENCODE lzo,
    device_sk           BIGINT               ENCODE az64,
    plant_sk            BIGINT               ENCODE az64,
    date_sk             INTEGER              ENCODE az64,
    event_ts            TIMESTAMP NOT NULL   ENCODE az64,
    event_name          VARCHAR(100)         ENCODE bytedict,
    value_numeric       DOUBLE PRECISION     ENCODE raw,
    value_string        VARCHAR(500)         ENCODE lzo,
    unit                VARCHAR(50)          ENCODE bytedict,
    quality             SMALLINT             ENCODE az64,
    alert_threshold     DOUBLE PRECISION     ENCODE raw,
    threshold_breach    BOOLEAN              ENCODE raw,
    anomaly_score       DOUBLE PRECISION     ENCODE raw,
    avg_value_1min      DOUBLE PRECISION     ENCODE raw,
    stddev_value_1min   DOUBLE PRECISION     ENCODE raw,
    processed_at        TIMESTAMP            ENCODE az64,
    -- partition cols kept for reference
    year                SMALLINT             ENCODE az64,
    month               SMALLINT             ENCODE az64
)
DISTKEY (device_sk)
COMPOUND SORTKEY (event_ts, plant_sk, event_name);


-- Machine alarm / maintenance log fact
CREATE TABLE IF NOT EXISTS factory.fact_machine_logs (
    log_sk              BIGINT IDENTITY(1,1) ENCODE az64,
    event_id            VARCHAR(100) NOT NULL ENCODE lzo,
    machine_id          VARCHAR(100) NOT NULL ENCODE lzo,
    device_sk           BIGINT               ENCODE az64,
    plant_sk            BIGINT               ENCODE az64,
    date_sk             INTEGER              ENCODE az64,
    event_ts            TIMESTAMP NOT NULL   ENCODE az64,
    event_type          VARCHAR(50)          ENCODE bytedict,
    severity            VARCHAR(20)          ENCODE bytedict,
    error_code          VARCHAR(50)          ENCODE lzo,
    temperature_c       REAL                 ENCODE raw,
    vibration_hz        REAL                 ENCODE raw,
    pressure_bar        REAL                 ENCODE raw,
    cycle_count         BIGINT               ENCODE az64,
    operator_id         VARCHAR(100)         ENCODE lzo,
    shift               VARCHAR(20)          ENCODE bytedict,
    is_alarm            BOOLEAN              ENCODE raw,
    is_critical         BOOLEAN              ENCODE raw,
    resolution_minutes  INTEGER              ENCODE az64,  -- NULL until resolved
    processed_at        TIMESTAMP            ENCODE az64,
    year                SMALLINT             ENCODE az64,
    month               SMALLINT             ENCODE az64
)
DISTKEY (machine_id)
COMPOUND SORTKEY (event_ts, plant_sk, event_type, severity);


-- =============================================================================
-- STAGING TABLES for COPY → upsert pattern
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_iot_events (LIKE factory.fact_iot_events);
CREATE TABLE IF NOT EXISTS staging.stg_machine_logs (LIKE factory.fact_machine_logs);


-- =============================================================================
-- COPY commands (run by Glue / Step Functions loader)
-- =============================================================================

-- Copy IoT events from processed S3 Parquet
COPY staging.stg_iot_events
FROM 's3://factory-processed-prod-ACCOUNT_ID/processed/iot_events/'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/factory-redshift-role-prod'
FORMAT AS PARQUET
SERIALIZETOJSON
REGION 'eu-west-1';

-- Upsert (delete existing + insert from staging)
BEGIN;
    DELETE FROM factory.fact_iot_events
    USING staging.stg_iot_events s
    WHERE factory.fact_iot_events.event_id = s.event_id;

    INSERT INTO factory.fact_iot_events
    SELECT * FROM staging.stg_iot_events;

    TRUNCATE staging.stg_iot_events;
COMMIT;
