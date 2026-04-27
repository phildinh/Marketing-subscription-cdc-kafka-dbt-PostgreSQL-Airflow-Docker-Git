# Kafka Consumer

Reads CDC events from Kafka topics and writes them to `raw.cdc_events` in `analytical_db`.

## How it works

1. Subscribes to all 5 CDC topics (`users`, `plans`, `subscriptions`, `payments`, `events`)
2. Polls Kafka every second for new messages
3. Parses each message — extracts `op`, `source_table`, `ts_ms` and the row data
4. Writes a batch to `raw.cdc_events` in a single `INSERT`
5. Commits Kafka offsets only **after** a successful write to Postgres

Step 5 is the key reliability guarantee — if the Postgres write fails, offsets are not committed and Kafka replays the batch on restart.

## Running

```bash
# From project root with venv activated
python kafka_consumer/consumer.py
```

## Configuration

| Env Var | Description |
|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker (default: localhost:9092) |
| `ANALYTICAL_DB_HOST` | PostgreSQL host (default: localhost) |
| `ANALYTICAL_DB_PORT` | PostgreSQL port (default: 5432) |
| `ANALYTICAL_DB_NAME` | Database name |
| `ANALYTICAL_DB_USER` | Database user |
| `ANALYTICAL_DB_PASSWORD` | Database password |

## Message format

Debezium with `ExtractNewRecordState` unwrap transform produces flat JSON:

```json
{
  "user_id": 1,
  "email": "phil@example.com",
  "op": "c",
  "table": "users",
  "ts_ms": 1767225600000000
}
```

The consumer detects and handles the Kafka Connect schema wrapper (`{"schema": ..., "payload": ...}`) for backwards compatibility with older messages.

## Consumer group

Group ID: `cdc-analytical-consumer`

Kafka tracks the committed offset per group. On restart the consumer resumes from the last committed offset — no events are lost or double-processed.

## Output table

All events land in `raw.cdc_events` in `analytical_db`:

| Column | Description |
|--------|-------------|
| `topic` | Kafka topic |
| `partition` | Kafka partition |
| `offset_value` | Kafka offset — used for idempotency checks |
| `op` | CDC operation: c / u / d / r |
| `source_table` | OLTP table that changed |
| `before` | Row state before change (JSONB) |
| `after` | Row state after change (JSONB) |
| `ts_ms` | Debezium timestamp in microseconds |
| `consumed_at` | When this row was written by the consumer |
