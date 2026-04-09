-- =============================================================================
-- FraudShield Redshift Schema
-- =============================================================================
-- Run once to initialize the warehouse. Subsequent data loading is handled
-- by src/warehouse/load.py via the daily Airflow pipeline.
--
-- Schema: analytics
-- Naming: fct_ prefix for fact tables, dim_ prefix for dimensions,
--         mv_ prefix for materialized views, _staging suffix for temp tables

CREATE SCHEMA IF NOT EXISTS analytics;

-- =============================================================================
-- Staging table (truncated after each load cycle)
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics.fct_transactions_staging (
    transaction_id        VARCHAR(36),
    transaction_ts        TIMESTAMP,
    transaction_date      DATE,
    merchant_id           VARCHAR(20),
    card_token            VARCHAR(64),
    amount_usd            DECIMAL(12,2),
    currency_code         VARCHAR(3),
    transaction_type      VARCHAR(20),
    channel               VARCHAR(20),
    country_code          VARCHAR(2),
    fraud_score           DECIMAL(5,4),
    fraud_decision        VARCHAR(20),
    is_fraudulent         BOOLEAN,
    processing_latency_ms INTEGER,
    ingestion_ts          TIMESTAMP
);

-- =============================================================================
-- Fact tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics.fct_transactions (
    transaction_id        VARCHAR(36) NOT NULL,
    transaction_ts        TIMESTAMP NOT NULL,
    transaction_date      DATE NOT NULL,
    merchant_id           VARCHAR(20) NOT NULL,
    card_token            VARCHAR(64) NOT NULL,
    amount_usd            DECIMAL(12,2) NOT NULL,
    currency_code         VARCHAR(3) NOT NULL,
    transaction_type      VARCHAR(20),
    channel               VARCHAR(20),
    country_code          VARCHAR(2),
    fraud_score           DECIMAL(5,4),
    fraud_decision        VARCHAR(20),
    is_fraudulent         BOOLEAN,
    chargeback_flag       BOOLEAN DEFAULT FALSE,
    processing_latency_ms INTEGER,
    ingestion_ts          TIMESTAMP DEFAULT GETDATE(),

    SORTKEY (transaction_date, merchant_id),
    DISTKEY (merchant_id)
);


CREATE TABLE IF NOT EXISTS analytics.fct_chargebacks (
    chargeback_id       VARCHAR(36) NOT NULL,
    transaction_id      VARCHAR(36) NOT NULL,
    chargeback_date     DATE NOT NULL,
    reason_code         VARCHAR(10),
    amount_usd          DECIMAL(12,2),
    status              VARCHAR(20),
    resolution_date     DATE,
    days_to_file        INTEGER,
    ingestion_ts        TIMESTAMP DEFAULT GETDATE(),

    SORTKEY (chargeback_date)
);


CREATE TABLE IF NOT EXISTS analytics.fct_fraud_alerts (
    alert_id            VARCHAR(36) NOT NULL,
    transaction_id      VARCHAR(36) NOT NULL,
    alert_ts            TIMESTAMP NOT NULL,
    fraud_score         DECIMAL(5,4),
    alert_threshold     DECIMAL(5,4),
    analyst_action      VARCHAR(20),
    review_ts           TIMESTAMP,
    resolution          VARCHAR(30),
    ingestion_ts        TIMESTAMP DEFAULT GETDATE(),

    SORTKEY (alert_ts)
);

-- =============================================================================
-- Dimension tables
-- =============================================================================

-- SCD Type 2: tracks changes over time via effective_from/effective_to
CREATE TABLE IF NOT EXISTS analytics.dim_merchants (
    merchant_sk         INTEGER IDENTITY(1,1),
    merchant_id         VARCHAR(20) NOT NULL,
    merchant_name       VARCHAR(200),
    category_code       VARCHAR(10),
    category_name       VARCHAR(100),
    risk_tier           VARCHAR(10),
    onboarding_date     DATE,
    country_code        VARCHAR(2),
    city                VARCHAR(100),
    is_current          BOOLEAN DEFAULT TRUE,
    effective_from      DATE NOT NULL,
    effective_to        DATE DEFAULT '9999-12-31',

    DISTSTYLE ALL
);


CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key            DATE NOT NULL,
    year                SMALLINT,
    quarter             SMALLINT,
    month               SMALLINT,
    month_name          VARCHAR(10),
    week_of_year        SMALLINT,
    day_of_week         SMALLINT,
    day_name            VARCHAR(10),
    is_weekend          BOOLEAN,
    is_holiday          BOOLEAN,

    DISTSTYLE ALL
);


CREATE TABLE IF NOT EXISTS analytics.dim_card_bins (
    bin_prefix          VARCHAR(8) NOT NULL,
    issuer_name         VARCHAR(200),
    card_brand          VARCHAR(20),
    card_type           VARCHAR(20),
    issuing_country     VARCHAR(2),

    DISTSTYLE ALL
);

-- =============================================================================
-- Materialized views for Power BI dashboards
-- =============================================================================

-- Fraud operations overview: daily metrics by category and channel
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_fraud_overview AS
SELECT
    t.transaction_date,
    m.category_name,
    m.risk_tier,
    t.channel,
    COUNT(*)                                                    AS total_transactions,
    SUM(t.amount_usd)                                           AS total_volume,
    SUM(CASE WHEN t.fraud_score > 0.7 THEN 1 ELSE 0 END)       AS high_risk_count,
    AVG(t.fraud_score)                                          AS avg_fraud_score,
    SUM(CASE WHEN t.chargeback_flag THEN t.amount_usd ELSE 0 END) AS chargeback_volume,
    SUM(CASE WHEN t.chargeback_flag THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0)                                   AS chargeback_rate
FROM analytics.fct_transactions t
JOIN analytics.dim_merchants m
    ON t.merchant_id = m.merchant_id
    AND m.is_current = TRUE
WHERE t.transaction_date >= DATEADD(day, -90, CURRENT_DATE)
GROUP BY 1, 2, 3, 4;


-- Chargeback trends: for the finance forecasting dashboard
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_chargeback_trends AS
SELECT
    t.transaction_date,
    t.country_code,
    cb.reason_code,
    COUNT(DISTINCT cb.chargeback_id)        AS chargeback_count,
    SUM(cb.amount_usd)                      AS chargeback_amount,
    AVG(cb.days_to_file)                    AS avg_days_to_file,
    PERCENTILE_CONT(0.5)
        WITHIN GROUP (ORDER BY cb.days_to_file) AS median_days_to_file
FROM analytics.fct_chargebacks cb
JOIN analytics.fct_transactions t
    ON cb.transaction_id = t.transaction_id
WHERE t.transaction_date >= DATEADD(day, -180, CURRENT_DATE)
GROUP BY 1, 2, 3;


-- Daily KPIs: the headline numbers from the README
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_daily_kpis AS
SELECT
    transaction_date,
    COUNT(*)                                                AS total_transactions,
    SUM(amount_usd)                                         AS total_volume,
    SUM(CASE WHEN is_fraudulent THEN 1 ELSE 0 END)          AS fraud_count,
    SUM(CASE WHEN is_fraudulent THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0)                               AS fraud_rate,
    SUM(CASE WHEN fraud_decision IN ('held','declined')
             AND is_fraudulent THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(SUM(CASE WHEN is_fraudulent THEN 1 ELSE 0 END), 0) AS detection_rate,
    AVG(processing_latency_ms)                              AS avg_latency_ms
FROM analytics.fct_transactions
WHERE transaction_date >= DATEADD(day, -90, CURRENT_DATE)
GROUP BY 1;


-- =============================================================================
-- Populate dim_date (one-time seed for 2020-2030)
-- =============================================================================
-- This would typically run once after schema creation.
-- Generates a row for every day in the range.

-- INSERT INTO analytics.dim_date
-- SELECT
--     d::DATE                                    AS date_key,
--     EXTRACT(year FROM d)                       AS year,
--     EXTRACT(quarter FROM d)                    AS quarter,
--     EXTRACT(month FROM d)                      AS month,
--     TO_CHAR(d, 'Month')                        AS month_name,
--     EXTRACT(week FROM d)                       AS week_of_year,
--     EXTRACT(dow FROM d)                        AS day_of_week,
--     TO_CHAR(d, 'Day')                          AS day_name,
--     CASE WHEN EXTRACT(dow FROM d) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
--     FALSE                                      AS is_holiday
-- FROM (
--     SELECT '2020-01-01'::DATE + (n * INTERVAL '1 day') AS d
--     FROM (SELECT ROW_NUMBER() OVER () - 1 AS n FROM stl_scan LIMIT 4018) seq
-- ) dates;
