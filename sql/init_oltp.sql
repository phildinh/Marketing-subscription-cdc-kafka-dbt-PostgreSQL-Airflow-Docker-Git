-- =============================================================
-- init_oltp.sql
-- Creates the OLTP source tables in oltp_db
-- Debezium watches these tables via WAL logical replication
-- =============================================================

-- Run this script connected to oltp_db
-- psql -U postgres -d oltp_db -f sql/init_oltp.sql

-- -------------------------------------------------------------
-- Enable logical replication support
-- Required for Debezium to read the WAL
-- -------------------------------------------------------------
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;

-- -------------------------------------------------------------
-- plans
-- Subscription tiers available to customers
-- INSERT fires when a new plan is added
-- UPDATE fires when price changes (triggers SCD Type 2 in dbt)
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plans (
    plan_id         SERIAL PRIMARY KEY,
    name            VARCHAR(50)     NOT NULL,
    price           DECIMAL(10, 2)  NOT NULL,
    billing_cycle   VARCHAR(20)     NOT NULL CHECK (billing_cycle IN ('monthly', 'annual')),
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- users
-- Customer accounts
-- UPDATE fires on email change, plan upgrade/downgrade
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    user_id         SERIAL PRIMARY KEY,
    email           VARCHAR(255)    NOT NULL UNIQUE,
    name            VARCHAR(100)    NOT NULL,
    plan_id         INT             REFERENCES plans(plan_id),
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- subscriptions
-- One row per subscription lifecycle event per user
-- UPDATE fires on upgrade, downgrade, cancellation
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id SERIAL PRIMARY KEY,
    user_id         INT             NOT NULL REFERENCES users(user_id),
    plan_id         INT             NOT NULL REFERENCES plans(plan_id),
    status          VARCHAR(20)     NOT NULL CHECK (status IN ('active', 'cancelled', 'paused', 'expired')),
    started_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMP
);

-- -------------------------------------------------------------
-- payments
-- One row per payment transaction including retries and refunds
-- INSERT fires on new payment or retry
-- UPDATE fires on refund
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments (
    payment_id      SERIAL PRIMARY KEY,
    subscription_id INT             NOT NULL REFERENCES subscriptions(subscription_id),
    amount          DECIMAL(10, 2)  NOT NULL,
    status          VARCHAR(20)     NOT NULL CHECK (status IN ('success', 'failed', 'refunded', 'pending')),
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- events
-- Clickstream events - page views, clicks, feature usage
-- properties column stores flexible JSON payload
-- INSERT only - events are never updated or deleted
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    event_id        SERIAL PRIMARY KEY,
    user_id         INT             NOT NULL REFERENCES users(user_id),
    event_type      VARCHAR(50)     NOT NULL,
    properties      JSONB,
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- Indexes
-- Support common query patterns and foreign key lookups
-- -------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_users_plan_id
    ON users(plan_id);

CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id
    ON subscriptions(user_id);

CREATE INDEX IF NOT EXISTS idx_subscriptions_plan_id
    ON subscriptions(plan_id);

CREATE INDEX IF NOT EXISTS idx_subscriptions_status
    ON subscriptions(status);

CREATE INDEX IF NOT EXISTS idx_payments_subscription_id
    ON payments(subscription_id);

CREATE INDEX IF NOT EXISTS idx_payments_status
    ON payments(status);

CREATE INDEX IF NOT EXISTS idx_events_user_id
    ON events(user_id);

CREATE INDEX IF NOT EXISTS idx_events_event_type
    ON events(event_type);

CREATE INDEX IF NOT EXISTS idx_events_created_at
    ON events(created_at);