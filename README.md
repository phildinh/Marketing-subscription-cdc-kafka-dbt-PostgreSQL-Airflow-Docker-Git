# CDC Subscription Pipeline

> A local Change Data Capture pipeline built to learn Debezium and Kafka by deliberately breaking it in 12 real-world failure scenarios — fixing each one and documenting exactly what broke and why.

---

## What Makes This Project Different

Anyone can spin up a Kafka container and follow a tutorial. This project is built around a different question: **what actually goes wrong when CDC runs continuously** — and how do you diagnose and fix it?

### The full pipeline runs end-to-end — generator, CDC, consumer, dbt, Airflow, Slack

Four Docker services (Zookeeper, Kafka, Debezium, Airflow) orchestrate a continuous stream of subscription data from PostgreSQL through Kafka into an analytical database — transformed by dbt and monitored via Slack.

![Docker Desktop showing all 4 containers running and healthy](docs/Screenshot%202026-04-27%20212350.png)

### The data generator simulates real subscription business activity

Users sign up, plans get upgraded and downgraded, payments fail and retry, subscriptions get cancelled. Every action fires a CDC event captured by Debezium.

![Data generator terminal — users created, payments retried, subscriptions cancelled](docs/Screenshot%202026-04-27%20212005.png)

### The Kafka consumer processes CDC events reliably with manual offset commits

Offsets are committed **only after** a successful write to PostgreSQL. If the database write fails, Kafka replays the batch on restart — no events lost, no silent failures.

![Kafka consumer receiving events and writing 22 events to raw.cdc_events](docs/Screenshot%202026-04-27%20212022.png)

### Slack alerts fire on every pipeline event — success, failure, schema change, silence

Five different alert types keep the pipeline observable without checking dashboards.

![Slack channel showing CDC pipeline succeeded, failed, schema change, and no-events alerts](docs/Screenshot%202026-04-27%20212916.png)

### Real bugs found and fixed during development

| Bug | How it was found | Fix |
|-----|-----------------|-----|
| `airflow users create` args treated as separate shell commands | Container crashed, port 8080 never opened | YAML `>` folded scalar + list-form command syntax |
| `${file:...}` secrets not expanded in connector config | 500 error on connector registration | Added `CONNECT_CONFIG_PROVIDERS` env var to Debezium |
| Secrets file mounted as directory, not file | `Is a directory` error inside container | Hardcoded credentials in gitignored connector config |
| `op=None` in all consumer messages | NOT NULL violation on `raw.cdc_events` insert | Added schema wrapper detection + `__op`/`op` field handling |
| `created_at` ISO strings fail `::bigint` cast in dbt | Date/time field out of range error in staging | Converted seed data to microsecond epoch values |
| `payment_id` uniqueness test failed with 690 results | Airflow dbt test task failed | Added `DISTINCT ON (payment_id)` in `fct_payments` CTE |
| dbt lands models in `raw_staging` not `staging` | Could not query `staging.stg_users` | Added `generate_schema_name` macro override |
| `phase/06` branched from stale develop | Files from phases 3-5 disappeared | Merged `origin/main` into all feature branches |
| Local `main` 4 commits behind remote | `git merge main` said "already up to date" | Always merge from `origin/main`, not local `main` |

---

## Table of Contents

- [Business Problem](#business-problem)
- [Architecture](#architecture)
- [Stack](#stack)
- [Data Pipeline Flow](#data-pipeline-flow)
- [Database Design](#database-design)
- [dbt Transformation Layers](#dbt-transformation-layers)
- [Airflow Orchestration](#airflow-orchestration)
- [Slack Alerting](#slack-alerting)
- [12 Break-Fix Scenarios](#12-break-fix-scenarios)
- [CI/CD](#cicd)
- [Project Structure](#project-structure)
- [Setup Guide](#setup-guide)
- [Key Learnings](#key-learnings)

---

## Business Problem

A marketing SaaS company runs PostgreSQL as its operational database. The analytics team needs access to user behaviour, subscription changes, and payment history — but they can't query the production database directly without impacting performance.

**The solution:** CDC streaming pipeline that:
- Captures every INSERT, UPDATE and DELETE from the OLTP database in real time
- Streams events through Kafka so nothing is lost even if the consumer goes down
- Lands events in an analytical database that the team can query freely
- Transforms raw events into clean star schema models via dbt
- Runs on a 30-minute Airflow schedule with full Slack observability

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Local Docker Network                         │
│                                                                     │
│  PostgreSQL (host)                                                  │
│  ┌──────────────┐                                                   │
│  │  oltp_db     │  WAL logical replication                          │
│  │  users       │──────────────────────────────┐                   │
│  │  plans       │                              ▼                   │
│  │  subscriptions│              ┌──────────────────────┐           │
│  │  payments    │               │   Debezium 2.4       │           │
│  │  events      │               │   PostgreSQL CDC      │           │
│  └──────────────┘               │   connector          │           │
│                                 └──────────┬───────────┘           │
│                                            │ CDC events            │
│                                            ▼                       │
│                                 ┌──────────────────────┐           │
│                                 │   Apache Kafka 7.5   │           │
│                                 │   5 topics           │           │
│                                 │   manual offsets     │           │
│                                 └──────────┬───────────┘           │
│                                            │                       │
│                                            ▼                       │
│                                 ┌──────────────────────┐           │
│  Python consumer ───────────────│  kafka_consumer.py   │           │
│  (runs locally)                 │  batch write         │           │
│                                 │  offset commit       │           │
│                                 └──────────┬───────────┘           │
│                                            │                       │
│  PostgreSQL (host)                         ▼                       │
│  ┌──────────────┐               ┌──────────────────────┐           │
│  │ analytical_db│◀──────────────│  raw.cdc_events      │           │
│  │  raw         │               └──────────────────────┘           │
│  │  staging     │                          │                       │
│  │  marts       │◀─────────────────────────┘                       │
│  │  snapshots   │   dbt run / snapshot / test                      │
│  └──────────────┘                                                  │
│         ▲                                                           │
│         │                  ┌──────────────────────┐                │
│         └──────────────────│   Airflow 2.8        │                │
│                            │   30-min schedule    │                │
│                            │   8-task DAG         │                │
│                            └──────────┬───────────┘                │
│                                       │ Slack webhooks             │
│                                       ▼                            │
│                            ┌──────────────────────┐                │
│                            │  #all-pipeline-notice│                │
│                            └──────────────────────┘                │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Source DB | PostgreSQL (local) | 15 |
| CDC | Debezium | 2.4 |
| Message broker | Apache Kafka (Confluent) | 7.5 |
| Coordination | Apache Zookeeper | — |
| Transformation | dbt-postgres | 1.8.2 |
| Orchestration | Apache Airflow | 2.8.1 |
| Containerisation | Docker Compose | — |
| Alerting | Slack Webhooks | — |
| CI | GitHub Actions | — |
| Language | Python | 3.10 |

---

## Data Pipeline Flow

### Continuous (generator + consumer running)

```
Step 1 — data_generator/generator.py runs continuously
         • Creates users, subscriptions, payments, events
         • Simulates upgrades, downgrades, cancellations
         • Inserts directly into oltp_db via psycopg2

Step 2 — Debezium reads PostgreSQL WAL
         • Captures every INSERT / UPDATE / DELETE
         • Publishes to 5 Kafka topics (one per table)
         • op=c (insert), op=u (update), op=d (delete), op=r (snapshot)

Step 3 — kafka_consumer/consumer.py reads from all 5 topics
         • Polls every 1 second
         • Parses CDC events — extracts op, table, row data
         • Batch inserts into raw.cdc_events
         • Commits Kafka offsets AFTER successful DB write

Step 4 — Airflow DAG fires every 30 minutes
         • Checks Kafka and Debezium are healthy
         • dbt run staging  → views over raw.cdc_events
         • dbt run marts    → fact and dimension tables
         • dbt snapshot     → SCD Type 2 for users and plans
         • dbt test         → 38 data quality tests
         • Slack success or failure alert
```

### On Failure

```
Any task fails → Airflow catches the error
               → slack_notify_failure fires via TriggerRule.ONE_FAILED
               → Slack posts failed task name + DAG + timestamp
               → Airflow marks task for retry (1 retry, 5-min delay)
```

---

## Database Design

### OLTP — oltp_db (source)

```
oltp_db (public schema)
├── plans           ← subscription tiers (starter / growth / pro)
├── users           ← customer accounts
├── subscriptions   ← one row per subscription lifecycle event
├── payments        ← one row per transaction including retries
└── events          ← clickstream (page_view, button_click, feature_used...)
```

### Analytical — analytical_db

```
analytical_db
├── raw
│   └── cdc_events          ← every CDC event from all 5 topics
│
├── staging (dbt views)
│   ├── stg_users
│   ├── stg_plans
│   ├── stg_subscriptions
│   ├── stg_payments
│   └── stg_events
│
├── marts (dbt tables)
│   ├── dim_users            ← latest state per user
│   ├── dim_plans            ← latest state per plan
│   ├── dim_dates            ← date spine 2024–2030
│   ├── fct_subscriptions    ← MRR and churn metrics
│   └── fct_payments         ← revenue and retry metrics
│
└── snapshots (SCD Type 2)
    ├── users_snapshot       ← full user history
    └── plans_snapshot       ← price change history
```

---

## dbt Transformation Layers

### Staging — clean and cast only, no joins

Each staging model reads from `raw.cdc_events` filtered by `source_table`. Timestamps are converted from Debezium microseconds to PostgreSQL `TIMESTAMP` using `to_timestamp(value / 1000000.0)`.

### Marts — business logic and joins

| Model | Key metric |
|-------|-----------|
| `fct_subscriptions` | MRR (annual plans divided by 12), `is_churned` flag, subscription duration in days |
| `fct_payments` | `recognised_revenue` (success only), `is_retry` flag, failure rate |
| `dim_users` | Latest state deduped with `DISTINCT ON (user_id) ORDER BY cdc_ts_ms DESC` |

### Snapshots — SCD Type 2

Both snapshots use the `timestamp` strategy on `cdc_consumed_at`. Every plan price change and every user email/plan change is preserved as a new row with `dbt_valid_from` / `dbt_valid_to` timestamps.

---

## Airflow Orchestration

8-task DAG running every 30 minutes:

```
check_kafka_health → check_debezium_health → dbt_run_staging
  → dbt_run_marts → dbt_snapshot → dbt_test
      → slack_notify_success  (ALL_SUCCESS)
      → slack_notify_failure  (ONE_FAILED)
```

dbt runs inside the Airflow container via volume mount (`./dbt:/opt/airflow/dbt`). The `ANALYTICAL_DB_HOST` is set to `host.docker.internal` so dbt inside Docker can reach PostgreSQL on the host.

---

## Slack Alerting

Five alert types in `slack/alerts.py`:

| Function | Trigger | Message |
|----------|---------|---------|
| `alert_pipeline_succeeded` | All DAG tasks green | DAG name, execution time, duration |
| `alert_pipeline_failed` | Any task fails | Failed task name, DAG, timestamp |
| `alert_pipeline_started` | DAG begins | DAG name, start time |
| `alert_schema_change` | New column detected | Table name, new columns |
| `alert_no_events` | Consumer silence | Minutes silent |

All alerts gracefully skip if `SLACK_WEBHOOK_URL` is not configured.

---

## 12 Break-Fix Scenarios

Implemented in `data_generator/experiments/` — each script deliberately breaks the pipeline to demonstrate a real CDC failure mode.

| # | Scenario | Concept tested |
|---|----------|---------------|
| 01 | Kafka crash mid-stream | Consumer offset recovery — replays from last commit |
| 02 | Postgres table drop | WAL replication slot — Debezium recovery |
| 03 | Debezium misconfig | Connector validation — connector fails gracefully |
| 04 | Duplicate event | Idempotency — double counting in marts |
| 05 | Null required field | Schema constraints — NOT NULL violation |
| 06 | Late arriving event | Out-of-order handling — staging timestamp ordering |
| 07 | New column added | Schema evolution — consumer and dbt impact |
| 08 | Column renamed | Breaking schema change — cast failure in staging |
| 09 | Type change | PostgreSQL type coercion — bigint vs varchar |
| 10 | Out-of-order events | Latest-state logic — wrong record in dim tables |
| 11 | Cancel before signup | Referential integrity — FK violation on insert |
| 12 | Race condition | Concurrent inserts — duplicate key handling |

---

## CI/CD

GitHub Actions runs on every push and pull request.

### dbt CI (`.github/workflows/dbt_ci.yml`)

| Step | What it does |
|------|-------------|
| Spin up PostgreSQL | Creates `analytical_db` with required schemas |
| Create `raw.cdc_events` | Table that staging models source from |
| Seed test data | 5 rows — one per OLTP table, microsecond timestamps |
| `dbt run` | Builds all 10 models against CI database |
| `dbt test` | Runs all 38 data quality tests |

### Local CI (`cicd/run_dbt_tests.sh`)

```bash
./cicd/run_dbt_tests.sh
```

Mirrors the GitHub Actions workflow locally — `dbt debug`, `dbt run`, `dbt test` — so failures are caught before pushing.

---

## Project Structure

```
├── sql/
│   ├── init_oltp.sql           OLTP schema + WAL settings
│   └── init_analytical.sql     Analytical schemas + raw.cdc_events
│
├── data_generator/
│   ├── generator.py            Continuous synthetic data generator
│   ├── schemas.py              Domain dataclasses and constants
│   ├── requirements.txt
│   └── experiments/            12 break-fix scenario scripts
│
├── kafka_consumer/
│   ├── consumer.py             Reads Kafka, writes raw.cdc_events
│   └── requirements.txt
│
├── debezium/
│   └── connector_config.json   PostgreSQL CDC connector
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── staging/            5 staging views
│   │   └── marts/              5 mart tables + schema.yml
│   ├── snapshots/              SCD Type 2 for users and plans
│   └── tests/                  2 singular business logic tests
│
├── airflow/
│   └── dags/
│       └── cdc_pipeline_dag.py 8-task DAG
│
├── slack/
│   └── alerts.py               Centralised Slack alert functions
│
├── utils/
│   ├── db_connections.py       OLTP and analytical connection helpers
│   ├── kafka_helpers.py        Kafka health check + bootstrap servers
│   ├── logging_config.py       Shared logger
│   └── retry_helpers.py        @db_retry and @kafka_retry decorators
│
├── tests/
│   ├── test_consumer.py        parse_message unit tests (no infra)
│   ├── test_generator.py       Schema and dataclass tests (no infra)
│   └── test_connections.py     Integration tests (needs Postgres + Kafka)
│
├── cicd/
│   └── run_dbt_tests.sh        Local CI runner
│
├── .github/workflows/
│   └── dbt_ci.yml              GitHub Actions CI
│
├── docker-compose.yml
├── .env                        Local credentials (gitignored)
└── requirements.txt
```

---

## Setup Guide

### Prerequisites

- Docker Desktop
- Python 3.10+
- PostgreSQL running locally on port 5432
- Git

### 1. Clone and configure

```bash
git clone https://github.com/phildinh/Marketing-subscription-cdc-kafka-dbt-PostgreSQL-Airflow-Docker-Git.git
cd Marketing-subscription-cdc-kafka-dbt-PostgreSQL-Airflow-Docker-Git
```

Create `.env` with your credentials (see `.env` structure in the repo).

### 2. Prepare local PostgreSQL

```bash
psql -U postgres -c "CREATE DATABASE oltp_db;"
psql -U postgres -c "CREATE DATABASE analytical_db;"
psql -U postgres -c "CREATE DATABASE airflow_db;"

psql -U postgres -d oltp_db -f sql/init_oltp.sql
psql -U postgres -d analytical_db -f sql/init_analytical.sql
```

Restart PostgreSQL so `wal_level = logical` takes effect.

### 3. Start Docker services

```bash
docker compose up -d
```

### 4. Register Debezium connector

```bash
# Wait ~30s for Debezium to be healthy, then:
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connector_config.json
```

### 5. Create Python environment

```bash
python -m venv .venv
.venv\Scripts\activate       # Windows
pip install -r requirements.txt
```

### 6. Run the pipeline

Terminal 1 — data generator:
```bash
python data_generator/generator.py
```

Terminal 2 — Kafka consumer:
```bash
python kafka_consumer/consumer.py
```

Terminal 3 — dbt:
```bash
cd dbt
dbt run && dbt snapshot && dbt test
```

### 7. Open Airflow

Go to `http://localhost:8080` — login `admin / admin` — trigger `cdc_pipeline` DAG.

---

## Key Learnings

### CDC and Kafka

**Why manual offset commits matter:** Auto-commit would mark messages as processed as soon as they're received. If the database write fails mid-batch, those events are gone. Manual commit after a successful write means Kafka replays the batch on restart — at-least-once delivery by design.

**Why Debezium sends `op=r` on startup:** When a connector registers for the first time, Debezium snapshots the current state of all monitored tables before streaming new changes. Every existing row appears as `op=r` (read). Staging models must include `r` alongside `c` and `u` — or they return zero rows after the first run.

**Why `${file:...}` secrets failed:** The `FileConfigProvider` for Kafka Connect must be explicitly registered via `CONNECT_CONFIG_PROVIDERS` env var — it is not loaded by default. Without it, the literal string `${file:...}` is passed as the password.

**Why `value.converter.schemas.enable: false` matters:** By default Kafka Connect wraps messages in a `{"schema": ..., "payload": ...}` envelope. The consumer's `payload.pop("op")` fails because `op` is nested inside `payload`, not at the top level.

### dbt

**Why `generate_schema_name` macro is required:** Without it, dbt prepends the profile's default schema to every custom schema — staging models land in `raw_staging` instead of `staging`. The macro override makes schema names absolute.

**Why timestamps need `to_timestamp(value / 1000000.0)`:** Debezium with `timestamp.mode: adaptive_time_microseconds` stores all timestamps as microseconds since Unix epoch — a BIGINT like `1767225600000000`. Direct `::timestamp` cast fails. Dividing by 1,000,000 converts to seconds before casting.

### Git

**Always merge from `origin/main`, not local `main`:** Local `main` only updates on `git pull`. If you create a feature branch from a stale local `main`, it's missing every commit since your last pull. `git merge origin/main` always reads from the remote.

---

## Author

**Phil Dinh**
Data Engineer | Sydney, Australia

- GitHub: [github.com/phildinh](https://github.com/phildinh)
- LinkedIn: [linkedin.com/in/phil-dinh](https://www.linkedin.com/in/phil-dinh/)

---

## License

MIT License — feel free to use this project as a reference for your own data engineering work.
