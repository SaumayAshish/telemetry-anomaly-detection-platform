# ADR 0004: Idempotency Key for Alerts

## Status

Accepted

## Context

Kafka delivers at least once. A record is seen again after a crash before the offset commit, after a rebalance, or after a commit the group rejected (ADR 0003). Writing an alert must therefore be safe to repeat: a redelivered record must not create a second row.

## Decision

### 1. The key

The identity of an alert is the Kafka position of its record on `telemetry.sensor.anomalies.v1`: the pair `(partition, offset)`. It is stored in `kafka_partition` and `kafka_offset` and protected by the unique constraint `uq_anomaly_alerts_kafka_position`.

### 2. How the insert uses it

`INSERT INTO anomaly_alerts (...) VALUES (...) ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING`. The writer reads the number of rows the database reports: 1 means the alert was inserted, 0 means this position was already stored and is treated as done (logged as `ALERT_DUPLICATE`). More than one row is impossible and is treated as an error.

### 3. Dead letters

`alert_dlq` uses `(kafka_topic, kafka_partition, kafka_offset)` with the same `ON CONFLICT DO NOTHING`, so a redelivered bad record does not create a second dead-letter row (logged as `DEAD_LETTER_DUPLICATE`).

## Alternatives considered

- A key built from the event content (sensor id and reading timestamp): it ties the table to the payload format, and two distinct events that share a reading timestamp would be collapsed.
- A generated id with a separate "already processed" lookup: two statements per record and a race between them, where one statement with ON CONFLICT is atomic.

## Consequences

- The key identifies a Kafka record, not an anomaly. If the stream application is replayed and emits the same anomaly again, it gets a new offset and is stored again.
- `anomaly_alerts` has no topic column, so exactly one input topic is supported. If the topic is recreated or its offsets are reset, new records collide with old rows and are silently skipped as duplicates. Reading from a second topic would need a migration that adds `kafka_topic` to the unique key, as `alert_dlq` already has.
- Because redelivery is harmless, the consumer can commit after a whole batch (ADR 0003).
