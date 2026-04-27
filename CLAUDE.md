# CDC Subscription Pipeline â€” Claude Reference

## Project Purpose
Local CDC streaming pipeline for learning Debezium and Kafka.
Deliberately break the pipeline in 12 scenarios, fix each one, document in README.

## Stack
PostgreSQL (local, port 5432) â†’ Debezium â†’ Kafka â†’ PostgreSQL analytical â†’ dbt â†’ Power BI
Orchestrated with Airflow + Docker. Slack alerts on pipeline events.

## Key Ports
| Service    | Port |
|------------|------|
| Kafka      | 9092 |
| Debezium   | 8083 |
| Airflow    | 8080 |
| Zookeeper  | 2181 |
| Postgres   | 5432 (local, not Docker) |

## Databases
| Database       | Purpose          |
|----------------|------------------|
| oltp_db        | OLTP source      |
| analytical_db  | Analytical dest  |
| airflow_db     | Airflow metadata |

## Folder Structure
- sql/            â†’ OLTP and analytical schema scripts
- data_generator/ â†’ Synthetic data generation
- kafka_consumer/ â†’ Reads Kafka, writes to analytical Postgres
- debezium/       â†’ Connector config
- dbt/            â†’ Staging and mart models
- airflow/dags/   â†’ Pipeline orchestration
- slack/          â†’ Webhook alerts
- utils/          â†’ Shared helpers
- tests/          â†’ Unit and integration tests

## Domain
Subscription and customer lifecycle.
Tables: users, plans, subscriptions, payments, events.

## dbt Layers
- staging   â†’ clean, rename, cast only. No joins.
- marts     â†’ facts and dimensions. Joins happen here only.
- snapshots â†’ SCD Type 2 for dim_users and dim_plans.

## Conventions
- Branch strategy: feature branches off main directly (develop branch abandoned)
- Current branch pattern: phase/0X-description
- Always explain business problem before technical solution
- No walls of unstructured text

## What Phil Knows â€” Never Re-explain
- Star schema, SCD Type 2, dbt, Airflow, Docker, GitHub Actions, pandas, PostgreSQL

## What Phil Is Learning â€” Always Explain Carefully
- Debezium connector config and WAL logical replication
- Kafka topics, consumers, offset management, before/after/op event structure
- Streaming vs batch differences
- Schema evolution and pipeline resilience
- Idempotency and out-of-order event handling

## Common Commands

### Docker
```bash
# Start all services
docker compose up -d

# Start a single service
docker compose up -d airflow

# View logs
docker compose logs -f airflow

# Stop everything
docker compose down

# Full reset (removes volumes)
docker compose down -v
```

### dbt
```bash
cd dbt
dbt run              # run all models
dbt run -s staging   # run staging layer only
dbt test             # run all tests
dbt snapshot         # run SCD snapshots
```

### Tests
```bash
pytest tests/
```

### Register Debezium connector
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connector_config.json
```

