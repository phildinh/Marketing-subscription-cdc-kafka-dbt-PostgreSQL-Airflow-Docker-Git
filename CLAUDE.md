# CDC Subscription Pipeline — Claude Reference

## Project Purpose
Local CDC streaming pipeline for learning Debezium and Kafka.
Deliberately break the pipeline in 12 scenarios, fix each one, document in README.

## Stack
PostgreSQL (local, port 5432) → Debezium → Kafka → PostgreSQL analytical → dbt → Power BI
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
- sql/            → OLTP and analytical schema scripts
- data_generator/ → Synthetic data generation
- kafka_consumer/ → Reads Kafka, writes to analytical Postgres
- debezium/       → Connector config
- dbt/            → Staging and mart models
- airflow/dags/   → Pipeline orchestration
- slack/          → Webhook alerts
- utils/          → Shared helpers
- tests/          → Unit and integration tests

## Domain
Subscription and customer lifecycle.
Tables: users, plans, subscriptions, payments, events.

## dbt Layers
- staging   → clean, rename, cast only. No joins.
- marts     → facts and dimensions. Joins happen here only.
- snapshots → SCD Type 2 for dim_users and dim_plans.

## Conventions
- Branch strategy: feature branches off develop, develop merges to main
- Current branch pattern: phase/0X-description
- Always explain business problem before technical solution
- No walls of unstructured text

## What Phil Knows — Never Re-explain
- Star schema, SCD Type 2, dbt, Airflow, Docker, GitHub Actions, pandas, PostgreSQL

## What Phil Is Learning — Always Explain Carefully
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
