# Data Generator

Generates synthetic marketing subscription data and inserts it into `oltp_db`.
Simulates realistic business activity: signups, upgrades, cancellations, payments, and clickstream events.

## How it works

The generator runs in a continuous loop:

1. Creates new users with randomised emails, names, and plans
2. Creates a subscription and initial payment for each user
3. Retries failed payments (simulates real payment retry logic)
4. Upgrades or downgrades 10% of active subscriptions each cycle
5. Cancels 5% of active subscriptions each cycle
6. Fires 1-5 clickstream events per user

## Running

```bash
# From project root with venv activated
python data_generator/generator.py
```

## Configuration

Credentials are loaded from `.env` via `python-dotenv`. No hardcoded passwords.

| Env Var | Description |
|---------|-------------|
| `OLTP_DB_HOST` | PostgreSQL host (default: localhost) |
| `OLTP_DB_PORT` | PostgreSQL port (default: 5432) |
| `OLTP_DB_NAME` | Database name |
| `OLTP_DB_USER` | Database user |
| `OLTP_DB_PASSWORD` | Database password |

## Experiments

The `experiments/` folder contains 12 scripts that deliberately break the pipeline:

| File | Scenario |
|------|----------|
| `exp_01_kafka_crash.py` | Kills Kafka mid-stream |
| `exp_02_postgres_drop.py` | Drops and recreates a table |
| `exp_03_debezium_misconfig.py` | Breaks the connector config |
| `exp_04_duplicate_event.py` | Forces duplicate inserts |
| `exp_05_null_field.py` | Inserts rows with null required fields |
| `exp_06_late_arriving.py` | Inserts backdated events |
| `exp_07_new_column.py` | Adds a column to the OLTP table |
| `exp_08_column_rename.py` | Renames a column |
| `exp_09_type_change.py` | Changes a column type |
| `exp_10_out_of_order.py` | Sends events out of timestamp order |
| `exp_11_cancel_before_signup.py` | Cancels a non-existent subscription |
| `exp_12_race_condition.py` | Concurrent inserts on the same key |
