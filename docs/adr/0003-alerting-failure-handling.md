# ADR 0003: Failure Handling in the Alerting Consumer

## Status

Accepted

## Context

alerting-service reads anomaly events from Kafka and writes them to PostgreSQL. Kafka delivery is at least once and the consumer commits offsets itself, so a record can be seen more than once. Some failures are temporary (the database restarts), some are permanent for one record (a constraint violation), and some messages can never become alerts at all (malformed JSON). A single bad record must not block a partition, and no record may be dropped silently.

## Decision

### 1. Commit after the batch

`BatchLoop` processes every record of a polled batch and then commits once. An empty batch is not committed and a failed batch is not committed, so a crash redelivers it. Redelivery is harmless because inserts are idempotent (ADR 0004).

### 2. Routing of one record

`RecordProcessor` decides what happens to each record:

- Malformed JSON, a JSON null or a missing payload: dead letter, `sql_state` DESERIALIZATION.
- A well-formed event that fails Bean Validation: dead letter, `sql_state` VALIDATION, with the sorted violation summary as the message.
- A valid event: persisted. A position that was already stored is treated as done.
- A valid event the database permanently refuses (for example a foreign-key violation, SQLState 23503): dead letter with the real SQLState, reason INSERT_REJECTED.

A dead letter always stores the payload exactly as it was delivered, never a re-serialized copy: re-serializing can fail, and it would drop fields a newer producer added. The `alert_dlq` row is unique per (topic, partition, offset), so a redelivered record does not create a second row.

### 3. Transient database failures

`SqlFailureClassifier` decides whether an error is retryable. For a retryable error `AlertWriter` closes the connection, reconnects, prepares the statements again and repeats the same write: at most 5 attempts, pausing 1, 2, 4 and 8 seconds between them (about 15 seconds in all). When the attempts are used up the last SQLException is thrown, the process stops, the offset is not committed and Kafka redelivers.

### 4. A long database outage

A persistent outage stays a crash-restart loop. The consumer does not hold a record longer than the retry budget, because waiting longer inside the poll loop would approach `max.poll.interval.ms` (Kafka's default is 5 minutes) and get the consumer evicted from its group. Restarting is cheap, Kafka retains the data, and the supervisor (the `restart` policy in the compose file) owns the waiting.

### 5. A commit the group rejected

If the commit fails with CommitFailedException, the group has already moved the partitions elsewhere. Every record of the batch was persisted or dead-lettered, so `BatchLoop` logs `COMMIT_LOST` at WARN and carries on; the next poll redelivers and the idempotent inserts absorb it. Any other commit failure still propagates.

### 6. Last resort for the dead letters themselves

If the dead-letter table permanently refuses a write, the record is appended as one JSON line to a local file and forced to disk. The path is `DLQ_FALLBACK_PATH` (default `dlq-fallback.jsonl`; `/data/dlq-fallback.jsonl` on a volume in the compose pipeline). If that fails as well, the process stops with an error rather than lose the record.

### 7. Connection checks

The connection is validated, with a bounded `isValid(2)`, only after it has sat idle for 30 seconds. While records are flowing, a connection that died is found by the failing write and handled by section 3.

## Consequences

- A poison message cannot stall a partition. It ends up in `alert_dlq`, where an operator can see it.
- Dead letters and the fallback file need human attention. This ADR does not define how they are replayed.
- A database outage costs at most one retry budget per restart.
- What may be logged on these paths is governed by `docs/logging-policy.md`.
