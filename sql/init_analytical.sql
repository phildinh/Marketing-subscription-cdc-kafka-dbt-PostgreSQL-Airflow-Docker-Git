-- =============================================================
-- init_analytical.sql
-- Creates schemas in analytical_db
-- Kafka consumer writes to raw schema
-- dbt transforms raw → staging → marts
-- =============================================================

-- Run this script connected to analytical_db

-- -------------------------------------------------------------
-- Schemas
-- Three layers matching the dbt architecture
-- -------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- -------------------------------------------------------------
-- raw.cdc_events
-- Landing table for every CDC event coming from Kafka
-- One row per event — INSERT only, never updated or deleted
-- This is the single source of truth before dbt touches anything
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.cdc_events (
    id              SERIAL PRIMARY KEY,
    topic           VARCHAR(255)    NOT NULL,
    partition       INT             NOT NULL,
    offset_value    BIGINT          NOT NULL,
    op              VARCHAR(10)     NOT NULL,
    source_table    VARCHAR(100)    NOT NULL,
    before          JSONB,
    after           JSONB,
    ts_ms           BIGINT          NOT NULL,
    consumed_at     TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- Indexes on raw.cdc_events
-- Support dbt queries filtering by table and operation type
-- -------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_cdc_events_source_table
    ON raw.cdc_events(source_table);

CREATE INDEX IF NOT EXISTS idx_cdc_events_op
    ON raw.cdc_events(op);

CREATE INDEX IF NOT EXISTS idx_cdc_events_ts_ms
    ON raw.cdc_events(ts_ms);

CREATE INDEX IF NOT EXISTS idx_cdc_events_topic
    ON raw.cdc_events(topic);