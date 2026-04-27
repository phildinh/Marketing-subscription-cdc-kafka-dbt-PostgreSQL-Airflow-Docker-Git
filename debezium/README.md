# Debezium

Captures every INSERT, UPDATE, and DELETE from `oltp_db` via PostgreSQL WAL logical replication and publishes them as events to Kafka.

## How CDC works

PostgreSQL writes every change to the Write-Ahead Log (WAL). Debezium reads the WAL via a replication slot and translates each change into a structured JSON event published to a Kafka topic.

```
PostgreSQL WAL → Debezium replication slot → Kafka topic
```

Each event contains:
- `op`: operation type — `c` (create), `u` (update), `d` (delete), `r` (snapshot read)
- `before`: row state before the change (null on inserts)
- `after`: row state after the change (null on deletes)
- `ts_ms`: timestamp in microseconds

## Prerequisites

WAL logical replication must be enabled on PostgreSQL. Run once and restart PostgreSQL:

```sql
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
```

## Kafka Topics

One topic per OLTP table:

| Topic | Source Table |
|-------|-------------|
| `cdc_subscription.public.users` | users |
| `cdc_subscription.public.plans` | plans |
| `cdc_subscription.public.subscriptions` | subscriptions |
| `cdc_subscription.public.payments` | payments |
| `cdc_subscription.public.events` | events |

## Registering the connector

After `docker compose up -d`, register the connector once:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connector_config.json
```

Check status:
```bash
curl http://localhost:8083/connectors/cdc-subscription-connector/status
```

Expected: `"state": "RUNNING"` for both connector and task.

## Configuration highlights

| Setting | Value | Why |
|---------|-------|-----|
| `plugin.name` | `pgoutput` | Native PostgreSQL logical replication plugin |
| `timestamp.mode` | `adaptive_time_microseconds` | Timestamps as microseconds since epoch |
| `transforms` | `ExtractNewRecordState` | Flattens Debezium envelope — removes `before`/`after` nesting |
| `value.converter.schemas.enable` | `false` | Removes schema wrapper from messages — simpler for consumer |
| `decimal.handling.mode` | `string` | Avoids floating point precision issues on price fields |
