-- ─────────────────────────────────────────
-- SkyPulse Snowflake Setup
-- Run in order — each section depends on the previous
-- Account: bgzutol/wg59288 (AWS us-east-2)
-- ─────────────────────────────────────────

-- ─────────────────────────────────────────
-- SECTION 1: DATABASE + SCHEMAS + WAREHOUSE
-- ─────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS SKYPULSE;

CREATE SCHEMA IF NOT EXISTS SKYPULSE.SILVER;
CREATE SCHEMA IF NOT EXISTS SKYPULSE.GOLD;
CREATE SCHEMA IF NOT EXISTS SKYPULSE.ANALYTICS;

CREATE WAREHOUSE IF NOT EXISTS SKYPULSE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'SkyPulse analytics warehouse — auto-suspends after 60s idle';

-- Verify
SHOW DATABASES LIKE 'SKYPULSE';
SHOW SCHEMAS IN DATABASE SKYPULSE;
SHOW WAREHOUSES LIKE 'SKYPULSE_WH';


-- ─────────────────────────────────────────
-- SECTION 2: GCS STORAGE INTEGRATION
-- Allows Snowflake to authenticate with GCS
-- After running DESC INTEGRATION, grant the
-- STORAGE_GCP_SERVICE_ACCOUNT objectViewer on GCS bucket:
-- gsutil iam ch serviceAccount:<email>:objectViewer gs://skypulse-triptide
-- ─────────────────────────────────────────

USE DATABASE SKYPULSE;
USE SCHEMA SILVER;
USE WAREHOUSE SKYPULSE_WH;

CREATE STORAGE INTEGRATION IF NOT EXISTS gcs_skypulse_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://skypulse-triptide/');

-- Get Snowflake service account email to grant GCS access
DESC INTEGRATION gcs_skypulse_integration;
-- Copy STORAGE_GCP_SERVICE_ACCOUNT value
-- Then run: gsutil iam ch serviceAccount:<value>:objectViewer gs://skypulse-triptide


-- ─────────────────────────────────────────
-- SECTION 3: EXTERNAL STAGE
-- Points to GCS Bronze folder
-- ─────────────────────────────────────────

CREATE STAGE IF NOT EXISTS gcs_bronze_stage
    URL = 'gcs://skypulse-triptide/bronze/routes/'
    STORAGE_INTEGRATION = gcs_skypulse_integration
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

-- Verify GCS files are visible from Snowflake
LIST @gcs_bronze_stage;


-- ─────────────────────────────────────────
-- SECTION 4: SILVER TABLE
-- Stores raw AviationStack flight schedule data
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS SKYPULSE.SILVER.raw_flights (
    flight_date         VARCHAR,
    flight_status       VARCHAR,
    departure_airport   VARCHAR,
    departure_iata      VARCHAR,
    departure_scheduled TIMESTAMP_TZ,
    arrival_airport     VARCHAR,
    arrival_iata        VARCHAR,
    arrival_scheduled   TIMESTAMP_TZ,
    airline_name        VARCHAR,
    airline_iata        VARCHAR,
    flight_number       VARCHAR,
    flight_iata         VARCHAR,
    codeshared_airline  VARCHAR,
    codeshared_flight   VARCHAR,
    raw_json            VARIANT,
    loaded_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);


-- ─────────────────────────────────────────
-- SECTION 5: LOAD EXISTING BRONZE FILES
-- One-time bulk load of all existing Bronze files
-- Snowpipe handles new files going forward
-- ─────────────────────────────────────────

COPY INTO SKYPULSE.SILVER.raw_flights (
    flight_date,
    flight_status,
    departure_airport,
    departure_iata,
    departure_scheduled,
    arrival_airport,
    arrival_iata,
    arrival_scheduled,
    airline_name,
    airline_iata,
    flight_number,
    flight_iata,
    codeshared_airline,
    codeshared_flight,
    raw_json
)
FROM (
    SELECT
        $1:flight_date::VARCHAR,
        $1:flight_status::VARCHAR,
        $1:departure:airport::VARCHAR,
        $1:departure:iata::VARCHAR,
        $1:departure:scheduled::TIMESTAMP_TZ,
        $1:arrival:airport::VARCHAR,
        $1:arrival:iata::VARCHAR,
        $1:arrival:scheduled::TIMESTAMP_TZ,
        $1:airline:name::VARCHAR,
        $1:airline:iata::VARCHAR,
        $1:flight:number::VARCHAR,
        $1:flight:iata::VARCHAR,
        $1:flight:codeshared:airline_name::VARCHAR,
        $1:flight:codeshared:flight_iata::VARCHAR,
        $1
    FROM @gcs_bronze_stage
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
ON_ERROR = CONTINUE;

-- Verify load
SELECT COUNT(*) as total_rows,
       COUNT(DISTINCT flight_date) as unique_dates,
       MIN(flight_date) as earliest_date,
       MAX(flight_date) as latest_date
FROM SKYPULSE.SILVER.raw_flights;


-- ─────────────────────────────────────────
-- SECTION 6: SNOWPIPE
-- Auto-ingests new Bronze files as Function A creates them daily
-- AUTO_INGEST=FALSE: trigger manually or via Snowflake Task
-- ─────────────────────────────────────────

CREATE PIPE IF NOT EXISTS SKYPULSE.SILVER.gcs_routes_pipe
    AUTO_INGEST = FALSE
    COMMENT = 'Loads daily AviationStack route files from GCS Bronze'
AS
COPY INTO SKYPULSE.SILVER.raw_flights (
    flight_date,
    flight_status,
    departure_airport,
    departure_iata,
    departure_scheduled,
    arrival_airport,
    arrival_iata,
    arrival_scheduled,
    airline_name,
    airline_iata,
    flight_number,
    flight_iata,
    codeshared_airline,
    codeshared_flight,
    raw_json
)
FROM (
    SELECT
        $1:flight_date::VARCHAR,
        $1:flight_status::VARCHAR,
        $1:departure:airport::VARCHAR,
        $1:departure:iata::VARCHAR,
        $1:departure:scheduled::TIMESTAMP_TZ,
        $1:arrival:airport::VARCHAR,
        $1:arrival:iata::VARCHAR,
        $1:arrival:scheduled::TIMESTAMP_TZ,
        $1:airline:name::VARCHAR,
        $1:airline:iata::VARCHAR,
        $1:flight:number::VARCHAR,
        $1:flight:iata::VARCHAR,
        $1:flight:codeshared:airline_name::VARCHAR,
        $1:flight:codeshared:flight_iata::VARCHAR,
        $1
    FROM @gcs_bronze_stage
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

-- Manually trigger Snowpipe for new files
ALTER PIPE SKYPULSE.SILVER.gcs_routes_pipe REFRESH;

-- Check pipe status
SHOW PIPES LIKE 'gcs_routes_pipe';


-- ─────────────────────────────────────────
-- SECTION 7: GOLD TABLES
-- Denormalized analytics tables with simulated prices
-- In production: export BigQuery Gold → GCS → Snowpipe → here
-- ─────────────────────────────────────────

USE DATABASE SKYPULSE;
USE SCHEMA GOLD;
USE WAREHOUSE SKYPULSE_WH;

CREATE TABLE IF NOT EXISTS flight_price_summary (
    flight_number       VARCHAR,
    route_id            VARCHAR,
    origin              VARCHAR,
    destination         VARCHAR,
    airline_name        VARCHAR,
    alliance            VARCHAR,
    date                VARCHAR,
    is_weekend          BOOLEAN,
    season              VARCHAR,
    avg_price_usd       FLOAT,
    min_price_usd       FLOAT,
    max_price_usd       FLOAT,
    anomaly_count       INTEGER,
    window_count        INTEGER,
    loaded_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS route_stats (
    route_id            VARCHAR,
    origin              VARCHAR,
    destination         VARCHAR,
    date                VARCHAR,
    airline_count       INTEGER,
    cheapest_airline    VARCHAR,
    min_price_usd       FLOAT,
    avg_price_usd       FLOAT,
    max_price_usd       FLOAT,
    anomaly_count       INTEGER,
    loaded_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);


-- ─────────────────────────────────────────
-- SECTION 8: POPULATE GOLD TABLES
-- ─────────────────────────────────────────

-- Populate route_stats
INSERT INTO SKYPULSE.GOLD.route_stats (
    route_id, origin, destination, date,
    airline_count, cheapest_airline,
    min_price_usd, avg_price_usd, max_price_usd, anomaly_count
)
SELECT
    departure_iata || '-' || arrival_iata as route_id,
    departure_iata as origin,
    arrival_iata as destination,
    flight_date as date,
    COUNT(DISTINCT airline_iata) as airline_count,
    MIN_BY(airline_name, departure_scheduled) as cheapest_airline,
    CASE
        WHEN arrival_iata IN ('LHR','CDG','FRA','NRT','PVG','AMS','GRU','TPE','DOH')
            THEN ROUND(UNIFORM(600, 1000, RANDOM()), 2)
        WHEN arrival_iata IN ('LAX','SFO','SEA','DEN','MIA','JFK','BOS')
            THEN ROUND(UNIFORM(150, 250, RANDOM()), 2)
        ELSE ROUND(UNIFORM(80, 150, RANDOM()), 2)
    END as min_price_usd,
    CASE
        WHEN arrival_iata IN ('LHR','CDG','FRA','NRT','PVG','AMS','GRU','TPE','DOH')
            THEN ROUND(UNIFORM(700, 1100, RANDOM()), 2)
        WHEN arrival_iata IN ('LAX','SFO','SEA','DEN','MIA','JFK','BOS')
            THEN ROUND(UNIFORM(170, 280, RANDOM()), 2)
        ELSE ROUND(UNIFORM(90, 180, RANDOM()), 2)
    END as avg_price_usd,
    CASE
        WHEN arrival_iata IN ('LHR','CDG','FRA','NRT','PVG','AMS','GRU','TPE','DOH')
            THEN ROUND(UNIFORM(800, 1200, RANDOM()), 2)
        WHEN arrival_iata IN ('LAX','SFO','SEA','DEN','MIA','JFK','BOS')
            THEN ROUND(UNIFORM(200, 320, RANDOM()), 2)
        ELSE ROUND(UNIFORM(100, 210, RANDOM()), 2)
    END as max_price_usd,
    0 as anomaly_count
FROM SKYPULSE.SILVER.raw_flights
WHERE departure_iata = 'ORD'
AND arrival_iata IS NOT NULL
AND airline_iata IS NOT NULL
GROUP BY
    departure_iata || '-' || arrival_iata,
    departure_iata, arrival_iata, flight_date;

-- Populate flight_price_summary
INSERT INTO SKYPULSE.GOLD.flight_price_summary (
    flight_number, route_id, origin, destination,
    airline_name, alliance, date, is_weekend, season,
    avg_price_usd, min_price_usd, max_price_usd,
    anomaly_count, window_count
)
SELECT
    COALESCE(flight_iata, 'UNKNOWN') as flight_number,
    departure_iata || '-' || arrival_iata as route_id,
    departure_iata as origin,
    arrival_iata as destination,
    airline_name,
    CASE
        WHEN airline_iata IN ('UA','LH','AC') THEN 'Star Alliance'
        WHEN airline_iata IN ('AA','BA','QR') THEN 'Oneworld'
        WHEN airline_iata IN ('DL','AF','KL') THEN 'SkyTeam'
        ELSE 'None'
    END as alliance,
    flight_date as date,
    DAYOFWEEK(TO_DATE(flight_date)) IN (1, 7) as is_weekend,
    CASE
        WHEN MONTH(TO_DATE(flight_date)) IN (12,1,2) THEN 'Winter'
        WHEN MONTH(TO_DATE(flight_date)) IN (3,4,5)  THEN 'Spring'
        WHEN MONTH(TO_DATE(flight_date)) IN (6,7,8)  THEN 'Summer'
        ELSE 'Fall'
    END as season,
    CASE
        WHEN arrival_iata IN ('LHR','CDG','FRA','NRT','PVG','AMS','GRU','TPE','DOH')
            THEN ROUND(UNIFORM(700, 1100, RANDOM()), 2)
        WHEN arrival_iata IN ('LAX','SFO','SEA','DEN','MIA','JFK','BOS')
            THEN ROUND(UNIFORM(170, 280, RANDOM()), 2)
        ELSE ROUND(UNIFORM(90, 180, RANDOM()), 2)
    END as avg_price_usd,
    CASE
        WHEN arrival_iata IN ('LHR','CDG','FRA','NRT','PVG','AMS','GRU','TPE','DOH')
            THEN ROUND(UNIFORM(600, 1000, RANDOM()), 2)
        WHEN arrival_iata IN ('LAX','SFO','SEA','DEN','MIA','JFK','BOS')
            THEN ROUND(UNIFORM(150, 250, RANDOM()), 2)
        ELSE ROUND(UNIFORM(80, 150, RANDOM()), 2)
    END as min_price_usd,
    CASE
        WHEN arrival_iata IN ('LHR','CDG','FRA','NRT','PVG','AMS','GRU','TPE','DOH')
            THEN ROUND(UNIFORM(800, 1200, RANDOM()), 2)
        WHEN arrival_iata IN ('LAX','SFO','SEA','DEN','MIA','JFK','BOS')
            THEN ROUND(UNIFORM(200, 320, RANDOM()), 2)
        ELSE ROUND(UNIFORM(100, 210, RANDOM()), 2)
    END as max_price_usd,
    0 as anomaly_count,
    12 as window_count
FROM SKYPULSE.SILVER.raw_flights
WHERE departure_iata = 'ORD'
AND arrival_iata IS NOT NULL
AND flight_iata IS NOT NULL;

-- Verify
SELECT COUNT(*) FROM SKYPULSE.GOLD.flight_price_summary;
SELECT COUNT(*) FROM SKYPULSE.GOLD.route_stats;


-- ─────────────────────────────────────────
-- SECTION 9: DYNAMIC TABLE
-- Auto-refreshes every hour when source data changes
-- No manual refresh needed
-- ─────────────────────────────────────────

USE DATABASE SKYPULSE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE SKYPULSE_WH;

CREATE OR REPLACE DYNAMIC TABLE hourly_route_summary
    TARGET_LAG = '1 hour'
    WAREHOUSE = SKYPULSE_WH
    COMMENT = 'Hourly refreshed route price summary — auto-updates when Gold changes'
AS
SELECT
    route_id,
    origin,
    destination,
    date,
    season,
    is_weekend,
    COUNT(DISTINCT airline_name)        as airline_count,
    ROUND(AVG(avg_price_usd), 2)        as avg_price,
    ROUND(MIN(min_price_usd), 2)        as min_price,
    ROUND(MAX(max_price_usd), 2)        as max_price,
    SUM(anomaly_count)                  as total_anomalies,
    MAX(loaded_at)                      as last_updated
FROM SKYPULSE.GOLD.flight_price_summary
GROUP BY
    route_id, origin, destination,
    date, season, is_weekend;

-- Verify
SELECT COUNT(*) FROM hourly_route_summary;

-- Sample query
SELECT
    route_id,
    date,
    season,
    is_weekend,
    airline_count,
    avg_price,
    min_price,
    max_price
FROM hourly_route_summary
WHERE date = CURRENT_DATE()::VARCHAR
ORDER BY avg_price DESC
LIMIT 10;


-- ─────────────────────────────────────────
-- SECTION 10: SEMANTIC VIEW
-- Defines business vocabulary for Cortex Analyst
-- Enables plain English queries on flight price data
-- ─────────────────────────────────────────

USE ROLE ACCOUNTADMIN;
USE DATABASE SKYPULSE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE SKYPULSE_WH;

CREATE OR REPLACE SEMANTIC VIEW flight_price_semantic_view
TABLES (
    hourly_route_summary
        PRIMARY KEY (route_id, date)
)
DIMENSIONS (
    hourly_route_summary.route_id       AS hourly_route_summary.route_id,
    hourly_route_summary.origin         AS hourly_route_summary.origin,
    hourly_route_summary.destination    AS hourly_route_summary.destination,
    hourly_route_summary.date           AS hourly_route_summary.date,
    hourly_route_summary.season         AS hourly_route_summary.season,
    hourly_route_summary.is_weekend     AS hourly_route_summary.is_weekend
)
METRICS (
    hourly_route_summary.avg_price      AS AVG(hourly_route_summary.avg_price),
    hourly_route_summary.min_price      AS MIN(hourly_route_summary.min_price),
    hourly_route_summary.max_price      AS MAX(hourly_route_summary.max_price),
    hourly_route_summary.airline_count  AS MAX(hourly_route_summary.airline_count),
    hourly_route_summary.total_anomalies AS SUM(hourly_route_summary.total_anomalies)
);

-- Verify semantic view
SHOW SEMANTIC VIEWS LIKE 'flight_price_semantic_view';

-- Test semantic view query
SELECT * FROM SEMANTIC_VIEW(
    flight_price_semantic_view
    METRICS (
        hourly_route_summary.avg_price
    )
    DIMENSIONS (
        hourly_route_summary.route_id,
        hourly_route_summary.season
    )
)
ORDER BY avg_price ASC
LIMIT 10;


-- ─────────────────────────────────────────
-- SECTION 11: USEFUL ANALYTICS QUERIES
-- ─────────────────────────────────────────

-- Cheapest routes today
SELECT route_id, avg_price, min_price, airline_count
FROM SKYPULSE.ANALYTICS.hourly_route_summary
WHERE date = CURRENT_DATE()::VARCHAR
ORDER BY avg_price ASC
LIMIT 10;

-- International vs domestic price comparison
SELECT
    CASE
        WHEN destination IN ('LHR','CDG','FRA','NRT','PVG','AMS','GRU','TPE','DOH')
            THEN 'International'
        ELSE 'Domestic'
    END as route_type,
    ROUND(AVG(avg_price), 2) as avg_price,
    COUNT(DISTINCT route_id) as route_count
FROM SKYPULSE.ANALYTICS.hourly_route_summary
GROUP BY route_type;

-- Weekend vs weekday prices
SELECT
    is_weekend,
    ROUND(AVG(avg_price), 2) as avg_price,
    COUNT(*) as data_points
FROM SKYPULSE.ANALYTICS.hourly_route_summary
GROUP BY is_weekend;

-- Routes with most airlines (most competitive)
SELECT route_id, MAX(airline_count) as airlines
FROM SKYPULSE.ANALYTICS.hourly_route_summary
GROUP BY route_id
ORDER BY airlines DESC
LIMIT 10;

-- Price range by destination
SELECT
    destination,
    ROUND(MIN(min_price), 2) as cheapest,
    ROUND(AVG(avg_price), 2) as typical,
    ROUND(MAX(max_price), 2) as most_expensive
FROM SKYPULSE.ANALYTICS.hourly_route_summary
GROUP BY destination
ORDER BY typical ASC;