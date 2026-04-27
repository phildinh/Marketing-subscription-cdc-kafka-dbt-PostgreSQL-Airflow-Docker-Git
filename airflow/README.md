# Airflow

Orchestrates the dbt transformation layer on a 30-minute schedule.

## DAG: `cdc_pipeline`

```
check_kafka_health
    │
check_debezium_health
    │
dbt_run_staging        ← transforms raw.cdc_events into staging views
    │
dbt_run_marts          ← builds fact and dimension tables
    │
dbt_snapshot           ← SCD Type 2 for users and plans
    │
dbt_test               ← runs all data quality tests
    │
    ├── slack_notify_success   (all tasks green)
    └── slack_notify_failure   (any task fails)
```

## Setup

Airflow connects to PostgreSQL on the host machine via `host.docker.internal`.

The `airflow_db` database must exist before starting:
```sql
CREATE DATABASE airflow_db;
```

dbt and the Slack module are available inside the container via volume mounts:
- `./dbt` → `/opt/airflow/dbt`
- `./slack` → `/opt/airflow/slack`

## Credentials

All credentials are read from `.env` via `docker-compose.yml`. The Airflow UI login is `admin / admin` by default.

## Triggering manually

1. Go to `http://localhost:8080`
2. Find `cdc_pipeline`
3. Click the play button (▶) → Trigger DAG
