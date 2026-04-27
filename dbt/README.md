# dbt

Transforms CDC events from `raw.cdc_events` into clean staging views, mart tables, and SCD Type 2 snapshots.

## Layers

```
raw.cdc_events          (written by kafka_consumer)
    │
    ▼
staging.*               (views — clean and cast only, no joins)
    │
    ▼
marts.*                 (tables — joins and business logic)
    │
    ▼
snapshots.*             (SCD Type 2 history tables)
```

## Models

### Staging (views)
| Model | Source | Description |
|-------|--------|-------------|
| `stg_users` | users CDC events | Cleaned user records, timestamp cast from microseconds |
| `stg_plans` | plans CDC events | Cleaned plan records with price as decimal |
| `stg_subscriptions` | subscriptions CDC events | Subscription lifecycle events |
| `stg_payments` | payments CDC events | Payment transactions with retry flag |
| `stg_events` | events CDC events | Clickstream events with flattened properties JSONB |

### Marts (tables)
| Model | Description |
|-------|-------------|
| `dim_users` | Latest state per user — deduplicated by `cdc_ts_ms` |
| `dim_plans` | Latest state per plan |
| `dim_dates` | Calendar dimension — date spine 2024–2030 |
| `fct_subscriptions` | One row per subscription event with MRR and churn flags |
| `fct_payments` | One row per payment with revenue and retry flags |

### Snapshots (SCD Type 2)
| Snapshot | Tracks |
|----------|--------|
| `users_snapshot` | Every version of a user over time |
| `plans_snapshot` | Every version of a plan — captures price history |

## Running

```bash
cd dbt

dbt run              # build all models
dbt run -s staging   # staging layer only
dbt run -s marts     # marts layer only
dbt snapshot         # run SCD Type 2 snapshots
dbt test             # run all data quality tests
dbt debug            # verify connection
```

## Key Design Decisions

**Timestamps:** Debezium sends timestamps as microseconds since Unix epoch (`timestamp.mode: adaptive_time_microseconds`). Staging models convert using `to_timestamp(value / 1000000.0)`.

**Schema naming:** A custom `generate_schema_name` macro ensures models land in `staging`, `marts`, and `snapshots` — not `raw_staging`, `raw_marts` etc.

**Deduplication:** Mart dimensions use `DISTINCT ON (id) ORDER BY id, cdc_ts_ms DESC` to keep the latest CDC event per entity.
